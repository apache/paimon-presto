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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Utils for PrestoProperties. */
public class PrestoPropertyUtils {

    private static final String TABLE_SCAN_OPTIONS_TEMPLATE = "(%s|\\*)\\.(%s|\\*)\\.(.+)";
    private static final String TABLE_SCAN_OPTIONS_SPLITTER = "\\|";

    /**
     * The scan version can be passed using two kind of formats: 1, global option format
     * ${scanVersion}; 2, table option format
     * "${dbName}.${tableName}.${scanVersion1}|${dbName}.${tableName}.${scanVersion2}", The
     * dbName/tableName can be *, which means matching all the specific parts.
     */
    public static String getScanVersion(String scanVersion, Identifier identifier) {
        if (isGlobalScanOption(scanVersion)) {
            // return scan version directly for global option format
            return scanVersion.trim();
        }

        String[] tableScanOptions = scanVersion.trim().split(TABLE_SCAN_OPTIONS_SPLITTER);
        String tableScanVersion = "";

        for (String tableScanOption : tableScanOptions) {
            String tableOptionsTemplate =
                    String.format(
                            TABLE_SCAN_OPTIONS_TEMPLATE,
                            identifier.getDatabaseName(),
                            identifier.getObjectName());
            Pattern tableOptionsPattern = Pattern.compile(tableOptionsTemplate);
            Matcher matcher = tableOptionsPattern.matcher(tableScanOption.trim());
            if (matcher.find()) {
                return matcher.group(3).trim();
            }
        }

        return tableScanVersion;
    }

    public static boolean isGlobalScanOption(String scanVersion) {
        return !scanVersion.contains(".");
    }
}
