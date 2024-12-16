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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;

/** Used for configuration item inspection and management. */
public class PaimonConfig {

    private String warehouse;
    private String metastore;
    private String uri;
    private boolean paimonPushdownEnabled = true;
    private boolean paimonPartitionPruningEnabled = true;

    public String getWarehouse() {
        return warehouse;
    }

    @Config("warehouse")
    public PaimonConfig setWarehouse(String warehouse) {
        this.warehouse = warehouse;
        return this;
    }

    public String getMetastore() {
        return metastore;
    }

    @Config("metastore")
    public PaimonConfig setMetastore(String metastore) {
        this.metastore = metastore;
        return this;
    }

    public String getUri() {
        return uri;
    }

    @Config("uri")
    public PaimonConfig setUri(String uri) {
        this.uri = uri;
        return this;
    }

    public boolean isPaimonPushdownEnabled() {
        return paimonPushdownEnabled;
    }

    @Config("paimon.query-pushdown-enabled")
    @ConfigDescription("Enable paimon query pushdown")
    public PaimonConfig setPaimonPushdownEnabled(boolean paimonPushdownEnabled) {
        this.paimonPushdownEnabled = paimonPushdownEnabled;
        return this;
    }

    public boolean isPaimonPartitionPruningEnabled() {
        return paimonPartitionPruningEnabled;
    }

    @Config("paimon.partition-prune-enabled")
    @ConfigDescription("Enable paimon query partition prune")
    public PaimonConfig setPaimonPartitionPruningEnabled(boolean paimonPartitionPruningEnabled) {
        this.paimonPartitionPruningEnabled = paimonPartitionPruningEnabled;
        return this;
    }
}
