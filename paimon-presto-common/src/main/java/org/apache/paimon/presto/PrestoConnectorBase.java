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

import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorPlanOptimizerProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorMetadata;
import com.facebook.presto.spi.transaction.IsolationLevel;

import java.util.Optional;

import static com.facebook.presto.spi.transaction.IsolationLevel.READ_COMMITTED;
import static com.facebook.presto.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

/** Presto {@link Connector}. */
public abstract class PrestoConnectorBase implements Connector {

    private final PrestoTransactionManager transactionManager;
    private final PrestoSplitManager prestoSplitManager;
    private final PrestoPageSourceProvider prestoPageSourceProvider;
    private final PrestoMetadata prestoMetadata;
    private final Optional<PrestoPlanOptimizerProvider> prestoPlanOptimizerProvider;

    public PrestoConnectorBase(
            PrestoTransactionManager transactionManager,
            PrestoSplitManager prestoSplitManager,
            PrestoPageSourceProvider prestoPageSourceProvider,
            PrestoMetadata prestoMetadata) {
        this(
                transactionManager,
                prestoSplitManager,
                prestoPageSourceProvider,
                prestoMetadata,
                Optional.empty());
    }

    public PrestoConnectorBase(
            PrestoTransactionManager transactionManager,
            PrestoSplitManager prestoSplitManager,
            PrestoPageSourceProvider prestoPageSourceProvider,
            PrestoMetadata prestoMetadata,
            Optional<PrestoPlanOptimizerProvider> prestoPlanOptimizerProvider) {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.prestoSplitManager = requireNonNull(prestoSplitManager, "prestoSplitManager is null");
        this.prestoPageSourceProvider =
                requireNonNull(prestoPageSourceProvider, "prestoPageSourceProvider is null");
        this.prestoMetadata = requireNonNull(prestoMetadata, "prestoMetadata is null");
        this.prestoPlanOptimizerProvider =
                requireNonNull(prestoPlanOptimizerProvider, "prestoPlanOptimizerProvider is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(
            IsolationLevel isolationLevel, boolean readOnly) {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        ConnectorTransactionHandle transaction = new PrestoTransactionHandle();
        try (ThreadContextClassLoader ignored =
                new ThreadContextClassLoader(getClass().getClassLoader())) {
            transactionManager.put(transaction, prestoMetadata);
        }
        return transaction;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle) {
        ConnectorMetadata metadata = transactionManager.get(transactionHandle);
        return new ClassLoaderSafeConnectorMetadata(metadata, getClass().getClassLoader());
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return prestoSplitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider() {
        return prestoPageSourceProvider;
    }

    @Override
    public void rollback(ConnectorTransactionHandle transaction) {
        transactionManager.remove(transaction);
    }

    @Override
    public ConnectorPlanOptimizerProvider getConnectorPlanOptimizerProvider() {
        return prestoPlanOptimizerProvider.orElseThrow(UnsupportedOperationException::new);
    }
}
