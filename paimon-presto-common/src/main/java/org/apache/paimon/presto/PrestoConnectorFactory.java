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

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.inject.Injector;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

/** Presto {@link ConnectorFactory}. */
public class PrestoConnectorFactory implements ConnectorFactory {

    @Override
    public String getName() {
        return "paimon";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver() {
        return new PrestoHandleResolver();
    }

    @Override
    public Connector create(
            String catalogName, Map<String, String> config, ConnectorContext context) {
        requireNonNull(config, "config is null");

        try {
            Bootstrap app =
                    new Bootstrap(
                            new JsonModule(),
                            new PaimonModule(
                                    catalogName,
                                    context.getTypeManager(),
                                    context.getFunctionMetadataManager(),
                                    context.getStandardFunctionResolution(),
                                    context.getRowExpressionService(),
                                    config));

            Bootstrap bootstrap =
                    app.doNotInitializeLogging().setRequiredConfigurationProperties(config).quiet();
            try {
                // Using reflection to achieve compatibility with different versions of
                // dependencies.
                Method noStrictConfigMethod = Bootstrap.class.getMethod("noStrictConfig");
                noStrictConfigMethod.invoke(bootstrap);
            } catch (java.lang.NoSuchMethodException e) {
                // ignore
            }
            Injector injector = bootstrap.initialize();

            PrestoSessionProperties prestoSessionProperties =
                    injector.getInstance(PrestoSessionProperties.class);
            PrestoTransactionManager prestoTransactionManager =
                    injector.getInstance(PrestoTransactionManager.class);
            PrestoSplitManager prestoSplitManager = injector.getInstance(PrestoSplitManager.class);
            PrestoPageSourceProvider prestoPageSourceProvider =
                    injector.getInstance(PrestoPageSourceProvider.class);
            PrestoMetadata prestoMetadata = injector.getInstance(PrestoMetadata.class);
            PrestoPlanOptimizerProvider prestoPlanOptimizerProvider =
                    injector.getInstance(PrestoPlanOptimizerProvider.class);

            return new PrestoConnector(
                    prestoSessionProperties.getSessionProperties(),
                    prestoTransactionManager,
                    prestoSplitManager,
                    prestoPageSourceProvider,
                    prestoMetadata,
                    Optional.of(prestoPlanOptimizerProvider));
        } catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
