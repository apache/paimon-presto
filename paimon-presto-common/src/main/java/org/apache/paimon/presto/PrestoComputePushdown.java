/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.presto;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.hive.SubfieldExtractor;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.PlanVisitor;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/** PrestoComputePushdown. */
public class PrestoComputePushdown implements ConnectorPlanOptimizer {

    private final StandardFunctionResolution functionResolution;
    private final RowExpressionService rowExpressionService;

    public PrestoComputePushdown(
            StandardFunctionResolution functionResolution,
            RowExpressionService rowExpressionService) {

        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.rowExpressionService =
                requireNonNull(rowExpressionService, "rowExpressionService is null");
    }

    @Override
    public PlanNode optimize(
            PlanNode maxSubplan,
            ConnectorSession session,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator) {

        return maxSubplan.accept(new Visitor(session, idAllocator), null);
    }

    private class Visitor extends PlanVisitor<PlanNode, Void> {

        private final ConnectorSession session;
        private final PlanNodeIdAllocator idAllocator;

        public Visitor(ConnectorSession session, PlanNodeIdAllocator idAllocator) {
            this.session = requireNonNull(session, "session is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        }

        private Optional<List<ColumnHandle>> extractColumns(
                RowExpression expression,
                Map<VariableReferenceExpression, ColumnHandle> assignments) {

            if (expression instanceof VariableReferenceExpression) {
                VariableReferenceExpression variable = (VariableReferenceExpression) expression;
                ColumnHandle columnHandle = assignments.get(variable);
                if (columnHandle != null) {
                    return Optional.of(Collections.singletonList(columnHandle));
                }
                return Optional.empty();
            } else if (expression instanceof CallExpression) {
                CallExpression callExpression = (CallExpression) expression;
                List<ColumnHandle> columns = new ArrayList<>();
                for (RowExpression argument : callExpression.getArguments()) {
                    extractColumns(argument, assignments).ifPresent(columns::addAll);
                }
                return Optional.of(columns);
            }
            return Optional.empty();
        }

        @Override
        public PlanNode visitPlan(PlanNode node, Void context) {
            ImmutableList.Builder<PlanNode> children = ImmutableList.builder();
            boolean changed = false;
            for (PlanNode child : node.getSources()) {
                PlanNode newChild = child.accept(this, null);
                if (newChild != child) {
                    changed = true;
                }
                children.add(newChild);
            }
            if (!changed) {
                return node;
            }
            return node.replaceChildren(children.build());
        }

        @Override
        public PlanNode visitFilter(FilterNode filter, Void context) {
            if (!(filter.getSource() instanceof TableScanNode)) {
                return filter;
            }

            TableScanNode tableScan = (TableScanNode) filter.getSource();

            Map<String, PrestoColumnHandle> nameToColumnHandlesMapping =
                    tableScan.getAssignments().entrySet().stream()
                            .collect(
                                    Collectors.toMap(
                                            e -> e.getKey().getName(),
                                            e -> (PrestoColumnHandle) e.getValue()));

            RowExpression filterPredicate = filter.getPredicate();

            DomainTranslator.ExtractionResult<Subfield> decomposedFilter =
                    rowExpressionService
                            .getDomainTranslator()
                            .fromPredicate(
                                    session,
                                    filterPredicate,
                                    new SubfieldExtractor(
                                                    functionResolution,
                                                    rowExpressionService.getExpressionOptimizer(),
                                                    session)
                                            .toColumnExtractor());

            // Build paimon predicate presto column.
            TupleDomain<PrestoColumnHandle> entireColumnDomain =
                    decomposedFilter
                            .getTupleDomain()
                            .transform(
                                    subfield ->
                                            subfield.getPath().isEmpty()
                                                    ? subfield.getRootName()
                                                    : null)
                            .transform(nameToColumnHandlesMapping::get);

            // Build paimon predicate column list.
            Map<VariableReferenceExpression, ColumnHandle> assignments = tableScan.getAssignments();
            Optional<List<ColumnHandle>> projectedColumns =
                    extractColumns(filterPredicate, assignments);

            // Build paimon new presto table handle use pushdown.
            PrestoTableHandle oldPrestoTableHandle =
                    (PrestoTableHandle) tableScan.getTable().getConnectorHandle();
            PrestoTableHandle newPrestoTableHandle =
                    new PrestoTableHandle(
                            oldPrestoTableHandle.getSchemaName(),
                            oldPrestoTableHandle.getTableName(),
                            oldPrestoTableHandle.getSerializedTable(),
                            entireColumnDomain,
                            projectedColumns);

            PrestoTableLayoutHandle newLayoutHandle =
                    new PrestoTableLayoutHandle(
                            newPrestoTableHandle, tableScan.getCurrentConstraint());
            TableScanNode newTableScan =
                    new TableScanNode(
                            tableScan.getSourceLocation(),
                            tableScan.getId(),
                            new TableHandle(
                                    tableScan.getTable().getConnectorId(),
                                    newPrestoTableHandle,
                                    tableScan.getTable().getTransaction(),
                                    Optional.of(newLayoutHandle)),
                            tableScan.getOutputVariables(),
                            tableScan.getAssignments(),
                            tableScan.getCurrentConstraint(),
                            tableScan.getEnforcedConstraint());

            return new FilterNode(
                    filter.getSourceLocation(), filter.getId(), newTableScan, filterPredicate);
        }
    }
}
