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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;

/** Presto {@link PrestoSessionProperties}. */
public class PrestoSessionProperties {

    public static final String QUERY_PUSHDOWN_ENABLED = "query_pushdown_enabled";
    public static final String PARTITION_PRUNE_ENABLED = "partition_prune_enabled";
    public static final String RANGE_FILTERS_ON_SUBSCRIPTS_ENABLED =
            "range_filters_on_subscripts_enabled";
    public static final String SCAN_VERSION = "scan_version";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public PrestoSessionProperties(PaimonConfig config) {
        sessionProperties =
                ImmutableList.of(
                        booleanProperty(
                                QUERY_PUSHDOWN_ENABLED,
                                "Enable paimon query pushdown",
                                config.isPaimonPushdownEnabled(),
                                false),
                        booleanProperty(
                                PARTITION_PRUNE_ENABLED,
                                "Enable paimon query partition prune",
                                config.isPaimonPartitionPruningEnabled(),
                                false),
                        booleanProperty(
                                RANGE_FILTERS_ON_SUBSCRIPTS_ENABLED,
                                "Whether to enable pushdown of range filters on subscripts like (a[2] = 5)",
                                false,
                                false),
                        stringProperty(SCAN_VERSION, "Paimon table scan version", null, false));
    }

    public List<PropertyMetadata<?>> getSessionProperties() {
        return sessionProperties;
    }

    public static boolean isPaimonPushdownEnabled(ConnectorSession session) {
        return session.getProperty(QUERY_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static boolean isPartitionPruneEnabled(ConnectorSession session) {
        return session.getProperty(PARTITION_PRUNE_ENABLED, Boolean.class);
    }

    public static String getScanVersion(ConnectorSession session) {
        return session.getProperty(SCAN_VERSION, String.class);
    }
}
