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

import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.connector.ConnectorPlanOptimizerProvider;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;

import java.util.Set;

import static java.util.Objects.requireNonNull;

/** PrestoPlanOptimizerProvider. */
public class PrestoPlanOptimizerProvider implements ConnectorPlanOptimizerProvider {

    private final StandardFunctionResolution functionResolution;
    private final RowExpressionService rowExpressionService;
    private final FunctionMetadataManager functionMetadataManager;
    private final PrestoTransactionManager transactionManager;

    @Inject
    public PrestoPlanOptimizerProvider(
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
    public Set<ConnectorPlanOptimizer> getLogicalPlanOptimizers() {
        return ImmutableSet.of(
                new PrestoComputePushdown(
                        functionResolution,
                        rowExpressionService,
                        functionMetadataManager,
                        transactionManager));
    }

    @Override
    public Set<ConnectorPlanOptimizer> getPhysicalPlanOptimizers() {
        return ImmutableSet.of();
    }
}
