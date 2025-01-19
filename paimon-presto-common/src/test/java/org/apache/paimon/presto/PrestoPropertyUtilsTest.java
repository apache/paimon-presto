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

import org.apache.paimon.catalog.Identifier;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PrestoPlugin}. */
public class PrestoPropertyUtilsTest {

    @Test
    public void testScanVersion() {
        Identifier t1 = new Identifier("db", "t1");
        String scanVersion1 = "version1";
        String scanVersion2 = "*.*.version2";
        String scanVersion3 = "*.t1.version3";
        String scanVersion4 = "db.t1.version4";
        String scanVersion5 = "db.t2.version5";
        String scanVersion6 = "db.t2.version5|db.t1.version6";

        assertThat(PrestoPropertyUtils.getScanVersion(scanVersion1, t1)).isEqualTo("version1");
        assertThat(PrestoPropertyUtils.getScanVersion(scanVersion2, t1)).isEqualTo("version2");
        assertThat(PrestoPropertyUtils.getScanVersion(scanVersion3, t1)).isEqualTo("version3");
        assertThat(PrestoPropertyUtils.getScanVersion(scanVersion4, t1)).isEqualTo("version4");
        assertThat(PrestoPropertyUtils.getScanVersion(scanVersion5, t1)).isEmpty();
        assertThat(PrestoPropertyUtils.getScanVersion(scanVersion6, t1)).isEqualTo("version6");
    }

    @Test
    public void testGlobalScanOption() {
        String scanVersion1 = "version1";
        String scanVersion2 = "*.*.version2";
        String scanVersion3 = "*.*.version2|db.tb.version3";

        assertThat(PrestoPropertyUtils.isGlobalScanOption(scanVersion1)).isTrue();
        assertThat(PrestoPropertyUtils.isGlobalScanOption(scanVersion2)).isFalse();
        assertThat(PrestoPropertyUtils.isGlobalScanOption(scanVersion3)).isFalse();
    }
}
