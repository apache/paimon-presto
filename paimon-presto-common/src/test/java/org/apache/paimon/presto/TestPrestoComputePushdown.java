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

import org.apache.paimon.types.BigIntType;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.PredicateCompiler;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.gen.RowExpressionPredicateCompiler;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.planPrinter.RowExpressionFormatter;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.facebook.presto.sql.relational.RowExpressionOptimizer;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TestPrestoComputePushdown}. */
public class TestPrestoComputePushdown {

    public static final MetadataManager METADATA = MetadataManager.createTestMetadataManager();

    public static final StandardFunctionResolution FUNCTION_RESOLUTION =
            new FunctionResolution(FunctionAndTypeManager.createTestFunctionAndTypeManager());

    public static final RowExpressionService ROW_EXPRESSION_SERVICE =
            new RowExpressionService() {
                @Override
                public DomainTranslator getDomainTranslator() {
                    return new RowExpressionDomainTranslator(METADATA);
                }

                @Override
                public ExpressionOptimizer getExpressionOptimizer() {
                    return new RowExpressionOptimizer(METADATA);
                }

                @Override
                public PredicateCompiler getPredicateCompiler() {
                    return new RowExpressionPredicateCompiler(METADATA);
                }

                @Override
                public DeterminismEvaluator getDeterminismEvaluator() {
                    return new RowExpressionDeterminismEvaluator(METADATA);
                }

                @Override
                public String formatRowExpression(
                        ConnectorSession session, RowExpression expression) {
                    return new RowExpressionFormatter(METADATA.getFunctionAndTypeManager())
                            .formatRowExpression(session, expression);
                }
            };

    private TableScanNode createTableScan() {
        PlanVariableAllocator variableAllocator = new PlanVariableAllocator();
        VariableReferenceExpression variableA = variableAllocator.newVariable("a", BIGINT);

        Map<VariableReferenceExpression, ColumnHandle> assignments =
                ImmutableMap.<VariableReferenceExpression, ColumnHandle>builder()
                        .put(
                                variableA,
                                PrestoColumnHandle.create(
                                        "id", new BigIntType(), createTestFunctionAndTypeManager()))
                        .build();

        PrestoTableHandle tableHandle = new PrestoTableHandle("test", "test", "table".getBytes());

        return new TableScanNode(
                Optional.empty(),
                new PlanNodeId(UUID.randomUUID().toString()),
                new TableHandle(
                        new ConnectorId("paimon"),
                        tableHandle,
                        new PrestoTransactionHandle(),
                        Optional.of(
                                new PrestoTableLayoutHandle(
                                        new PrestoTableHandle(
                                                "test",
                                                "test",
                                                "table".getBytes(),
                                                TupleDomain.all(),
                                                Optional.empty()),
                                        TupleDomain.all()))),
                ImmutableList.copyOf(assignments.keySet()),
                assignments,
                TupleDomain.all(),
                TupleDomain.all());
    }

    private PlanNode createFilterNode() {
        String expression = "a = 1";
        TypeProvider typeProvider = TypeProvider.copyOf(ImmutableMap.of("a", BIGINT));
        RowExpression rowExpression =
                new TestingRowExpressionTranslator(METADATA)
                        .translateAndOptimize(expression(expression), typeProvider);

        return new FilterNode(
                Optional.empty(),
                new PlanNodeId(UUID.randomUUID().toString()),
                createTableScan(),
                rowExpression);
    }

    @Test
    public void testOptimizeFilter() {
        // Mock data.
        PrestoColumnHandle testData = new PrestoColumnHandle("id", "BIGINT", BIGINT);

        PrestoComputePushdown prestoComputePushdown =
                new PrestoComputePushdown(FUNCTION_RESOLUTION, ROW_EXPRESSION_SERVICE);

        PlanNode mockInputPlan = createFilterNode();
        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        PlanVariableAllocator variableAllocator = new PlanVariableAllocator();
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

        // Call optimize
        PlanNode result =
                prestoComputePushdown.optimize(
                        mockInputPlan, session, variableAllocator, idAllocator);

        // Optimize result convert.
        TableScanNode source = (TableScanNode) ((FilterNode) result).getSource();
        TableHandle table = source.getTable();
        PrestoTableLayoutHandle prestoTableLayoutHandle =
                (PrestoTableLayoutHandle)
                        table.getLayout()
                                .orElseThrow(
                                        () -> new IllegalStateException("Layout is not present"));

        // Assert whether there are any modifications after optimization optionalFilter.
        Optional<TupleDomain<PrestoColumnHandle>> optionalFilter =
                Optional.ofNullable(prestoTableLayoutHandle.getTableHandle().getFilter());
        optionalFilter.ifPresent(
                filter ->
                        assertThat(
                                        filter.getDomains().get().keySet().stream()
                                                .allMatch(testcase -> testcase.equals(testData)))
                                .isEqualTo(true));

        // Assert whether there are any modifications after optimization projectedColumns.
        Optional<List<ColumnHandle>> projectedColumns =
                prestoTableLayoutHandle.getTableHandle().getProjectedColumns();
        projectedColumns.ifPresent(
                columns ->
                        assertThat(columns.stream().allMatch(testcase -> testcase.equals(testData)))
                                .isEqualTo(true));
    }
}
