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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.utils.Preconditions;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.predicate.Primitives;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeUtils;
import com.facebook.presto.expressions.DefaultRowExpressionTraversalVisitor;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.hive.SubfieldExtractor;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.PlanVisitor;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.intersection;
import static java.util.Objects.requireNonNull;
import static org.apache.paimon.presto.PrestoSessionProperties.isPaimonPushdownEnabled;

/** PrestoComputePushdown. */
public class PrestoComputePushdown implements ConnectorPlanOptimizer {

    private final StandardFunctionResolution functionResolution;
    private final RowExpressionService rowExpressionService;
    private final FunctionMetadataManager functionMetadataManager;
    private final PrestoTransactionManager transactionManager;

    public PrestoComputePushdown(
            StandardFunctionResolution functionResolution,
            RowExpressionService rowExpressionService,
            FunctionMetadataManager functionMetadataManager,
            PrestoTransactionManager transactionManager) {
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.rowExpressionService =
                requireNonNull(rowExpressionService, "rowExpressionService is null");
        this.functionMetadataManager =
                requireNonNull(functionMetadataManager, "functionMetadataManager is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
    }

    @Override
    public PlanNode optimize(
            PlanNode maxSubplan,
            ConnectorSession session,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator) {

        if (isPaimonPushdownEnabled(session)) {
            return maxSubplan.accept(new Visitor(session, idAllocator), null);
        }
        return maxSubplan;
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

            Table table = ((PrestoTableHandle) tableScan.getTable().getConnectorHandle()).table();

            ImmutableSet.Builder<VariableReferenceExpression> builder = ImmutableSet.builder();
            decomposedFilter
                    .getRemainingExpression()
                    .accept(new VariableCollector(tableScan.getOutputVariables()), builder);
            Set<ColumnHandle> remainingFilterProjects =
                    builder.build().stream()
                            .map(
                                    v ->
                                            Preconditions.checkNotNull(
                                                    assignments.get(v),
                                                    "The variable is"
                                                            + " not found in the assignments"))
                            .collect(Collectors.toSet());

            // Prune the partition
            Set<String> partitionColumns = new HashSet<>(table.partitionKeys());
            Optional<List<Map<String, String>>> remainingPartitions = Optional.empty();
            // we have predicate on the partition field, then we have to list the partition and do
            // the prune.
            if (PrestoSessionProperties.isPartitionPruneEnabled(session)
                    && !remainingFilterProjects.isEmpty()
                    && remainingFilterProjects.stream()
                            .map(PrestoColumnHandle.class::cast)
                            .anyMatch(c -> partitionColumns.contains(c.getColumnName()))) {
                Map<String, ColumnHandle> columns =
                        transactionManager
                                .get(tableScan.getTable().getTransaction())
                                .getColumnHandles(
                                        session, tableScan.getTable().getConnectorHandle());
                remainingPartitions =
                        getRemainingPartition(
                                table, decomposedFilter, session, entireColumnDomain, columns);
            }

            // Build paimon new presto table handle use pushdown.
            PrestoTableHandle oldPrestoTableHandle =
                    (PrestoTableHandle) tableScan.getTable().getConnectorHandle();
            PrestoTableHandle newPrestoTableHandle =
                    new PrestoTableHandle(
                            oldPrestoTableHandle.getSchemaName(),
                            oldPrestoTableHandle.getTableName(),
                            oldPrestoTableHandle.getSerializedTable(),
                            entireColumnDomain,
                            projectedColumns,
                            remainingPartitions);

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

    private Optional<List<Map<String, String>>> getRemainingPartition(
            Table table,
            DomainTranslator.ExtractionResult<Subfield> decomposedFilter,
            ConnectorSession session,
            TupleDomain<PrestoColumnHandle> entireColumnDomain,
            Map<String, ColumnHandle> columns) {
        // Extract deterministic conjuncts that apply to partition columns and specify these as
        // Constraint#predicate
        Predicate<Map<ColumnHandle, NullableValue>> predicate = v -> true;

        RowExpression remainingExpression = decomposedFilter.getRemainingExpression();
        if (!TRUE_CONSTANT.equals(remainingExpression)) {
            LogicalRowExpressions logicalRowExpressions =
                    new LogicalRowExpressions(
                            rowExpressionService.getDeterminismEvaluator(),
                            functionResolution,
                            functionMetadataManager);
            RowExpression deterministicPredicate =
                    logicalRowExpressions.filterDeterministicConjuncts(remainingExpression);
            if (!TRUE_CONSTANT.equals(deterministicPredicate)) {
                ConstraintEvaluator evaluator =
                        new ConstraintEvaluator(
                                rowExpressionService, session, columns, deterministicPredicate);
                predicate = evaluator::isCandidate;
            }
        }

        ReadBuilder readBuilder = table.newReadBuilder();

        // list partition with filter.
        new PrestoFilterConverter(table.rowType())
                .convert(entireColumnDomain)
                .ifPresent(readBuilder::withFilter);

        List<BinaryRow> partitions = readBuilder.newScan().listPartitions();
        String partitionDefaultName = new CoreOptions(table.options()).partitionDefaultName();

        InternalRow.FieldGetter[] getters =
                table.rowType().project(table.partitionKeys()).fieldGetters();
        List<String> partitionKeys = table.partitionKeys();
        List<Map<String, String>> remainingPartitions = new ArrayList<>();
        for (BinaryRow partition : partitions) {
            Map<ColumnHandle, NullableValue> partitionPrestoValue = new HashMap<>();
            LinkedHashMap<String, String> partitionMap = new LinkedHashMap<>();
            for (int i = 0; i < getters.length; i++) {
                Object value = getters[i].getFieldOrNull(partition);
                PrestoColumnHandle handle = (PrestoColumnHandle) columns.get(partitionKeys.get(i));
                Preconditions.checkNotNull(
                        handle, "The %s column handle is not found.", partitionKeys.get(i));
                Type type = handle.getPrestoType();

                partitionPrestoValue.put(
                        handle,
                        NullableValue.of(
                                type,
                                value == null
                                        ? null
                                        : Utils.blockToNativeValue(
                                                type,
                                                PrestoTypeUtils.singleValueToBlock(type, value))));
                partitionMap.put(
                        partitionKeys.get(i),
                        value == null ? partitionDefaultName : value.toString());
            }
            if (predicate.test(partitionPrestoValue)) {
                remainingPartitions.add(partitionMap);
            }
        }
        return Optional.of(remainingPartitions);
    }

    private static class VariableCollector
            implements RowExpressionVisitor<
                    Void, ImmutableSet.Builder<VariableReferenceExpression>> {

        List<VariableReferenceExpression> inputVariables;

        public VariableCollector(List<VariableReferenceExpression> inputVariables) {
            this.inputVariables = inputVariables;
        }

        @Override
        public Void visitCall(
                CallExpression call, ImmutableSet.Builder<VariableReferenceExpression> context) {
            for (RowExpression argument : call.getArguments()) {
                argument.accept(this, context);
            }
            return null;
        }

        @Override
        public Void visitInputReference(
                InputReferenceExpression reference,
                ImmutableSet.Builder<VariableReferenceExpression> context) {
            int pos = reference.getField();
            context.add(inputVariables.get(pos));
            return null;
        }

        @Override
        public Void visitConstant(
                ConstantExpression literal,
                ImmutableSet.Builder<VariableReferenceExpression> context) {
            return null;
        }

        @Override
        public Void visitLambda(
                LambdaDefinitionExpression lambda,
                ImmutableSet.Builder<VariableReferenceExpression> context) {
            return null;
        }

        @Override
        public Void visitVariableReference(
                VariableReferenceExpression reference,
                ImmutableSet.Builder<VariableReferenceExpression> context) {
            context.add(reference);
            return null;
        }

        @Override
        public Void visitSpecialForm(
                SpecialFormExpression specialForm,
                ImmutableSet.Builder<VariableReferenceExpression> context) {
            specialForm.getArguments().forEach(argument -> argument.accept(this, context));
            return null;
        }
    }

    // copied from presto
    private static class VariableReferenceBuilderVisitor
            extends DefaultRowExpressionTraversalVisitor<
                    ImmutableSet.Builder<VariableReferenceExpression>> {
        @Override
        public Void visitVariableReference(
                VariableReferenceExpression variable,
                ImmutableSet.Builder<VariableReferenceExpression> builder) {
            builder.add(variable);
            return null;
        }
    }

    private static Set<VariableReferenceExpression> extractAll(RowExpression expression) {
        ImmutableSet.Builder<VariableReferenceExpression> builder = ImmutableSet.builder();
        expression.accept(new VariableReferenceBuilderVisitor(), builder);
        return builder.build();
    }

    private static class ConstraintEvaluator {
        private final Map<String, ColumnHandle> assignments;
        private final RowExpressionService evaluator;
        private final ConnectorSession session;
        private final RowExpression expression;
        private final Set<ColumnHandle> arguments;

        public ConstraintEvaluator(
                RowExpressionService evaluator,
                ConnectorSession session,
                Map<String, ColumnHandle> assignments,
                RowExpression expression) {
            this.assignments = assignments;
            this.evaluator = evaluator;
            this.session = session;
            this.expression = expression;

            arguments =
                    ImmutableSet.copyOf(extractAll(expression)).stream()
                            .map(VariableReferenceExpression::getName)
                            .map(assignments::get)
                            .collect(toImmutableSet());
        }

        private boolean isCandidate(Map<ColumnHandle, NullableValue> bindings) {
            if (intersection(bindings.keySet(), arguments).isEmpty()) {
                return true;
            }

            Function<VariableReferenceExpression, Object> variableResolver =
                    variable -> {
                        ColumnHandle column = assignments.get(variable.getName());
                        checkArgument(column != null, "Missing column assignment for %s", variable);

                        if (!bindings.containsKey(column)) {
                            return variable;
                        }

                        return bindings.get(column).getValue();
                    };

            // Skip pruning if evaluation fails in a recoverable way. Failing here can cause
            // spurious query failures for partitions that would otherwise be filtered out.
            Object optimized = null;
            try {
                optimized =
                        evaluator
                                .getExpressionOptimizer()
                                .optimize(expression, OPTIMIZED, session, variableResolver);
            } catch (PrestoException e) {
                propagateIfUnhandled(e);
                return true;
            }

            // If any conjuncts evaluate to FALSE or null, then the whole predicate will never be
            // true and so the partition should be pruned
            return !Boolean.FALSE.equals(optimized)
                    && optimized != null
                    && (!(optimized instanceof ConstantExpression)
                            || !((ConstantExpression) optimized).isNull());
        }

        private static void propagateIfUnhandled(PrestoException e) throws PrestoException {
            int errorCode = e.getErrorCode().getCode();
            if (errorCode == DIVISION_BY_ZERO.toErrorCode().getCode()
                    || errorCode == INVALID_CAST_ARGUMENT.toErrorCode().getCode()
                    || errorCode == INVALID_FUNCTION_ARGUMENT.toErrorCode().getCode()
                    || errorCode == NUMERIC_VALUE_OUT_OF_RANGE.toErrorCode().getCode()) {
                return;
            }

            throw e;
        }
    }

    private static final class Utils {
        private Utils() {}

        public static Block nativeValueToBlock(Type type, Object object) {
            if (object != null && !Primitives.wrap(type.getJavaType()).isInstance(object)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Object '%s' does not match type %s", object, type.getJavaType()));
            } else {
                BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
                TypeUtils.writeNativeValue(type, blockBuilder, object);
                return blockBuilder.build();
            }
        }

        public static Object blockToNativeValue(Type type, Block block) {
            return TypeUtils.readNativeValue(type, block, 0);
        }
    }
}
