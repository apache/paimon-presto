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

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.stream.Collectors;

/** util for date. */
public class DateUtils {
    private static String[] parsePatterns = {
        "yyyy-MM-dd",
        "yyyy-MM-dd HH",
        "yyyy-MM-dd HH:mm",
        "yyyy-MM-dd HH:mm:ss",
        "yyyy-MM-dd HH:mm:ss.SSS"
    };

    /**
     * auto format date.
     *
     * @param dateStr
     * @return
     */
    public static LocalDateTime autoFormat(String dateStr) {
        // date set default time
        if (dateStr.length() == "yyyy-MM-dd".length()) {
            dateStr += " 00:00:00";
        }
        int length = dateStr.length();

        String exceptionMessage =
                "Time travel param format not supported："
                        + dateStr
                        + ",Supported formats include："
                        + Arrays.stream(parsePatterns).collect(Collectors.joining(","));

        try {
            for (String pattern : parsePatterns) {
                if (pattern.length() == length) {
                    return LocalDateTime.parse(dateStr, DateTimeFormatter.ofPattern(pattern));
                }
            }
        } catch (DateTimeParseException exception) {
            throw new RuntimeException(exceptionMessage);
        }

        throw new RuntimeException(exceptionMessage);
    }

    /**
     * auto format date to timestamp.
     *
     * @param dateStr
     * @return
     */
    public static Long autoFormatToTimestamp(String dateStr) {
        LocalDateTime localDateTime = autoFormat(dateStr);
        return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

}