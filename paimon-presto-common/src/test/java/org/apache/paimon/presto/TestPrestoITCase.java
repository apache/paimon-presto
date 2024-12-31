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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.InnerTableCommit;
import org.apache.paimon.table.sink.InnerTableWrite;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VarCharType;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.facebook.airlift.testing.Closeables.closeAllSuppress;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.apache.paimon.data.BinaryString.fromString;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for presto connector. */
public class TestPrestoITCase {

    private QueryRunner queryRunner;

    private static final String CATALOG = "paimon";
    private static final String DB = "default";

    protected QueryRunner createQueryRunner() throws Exception {
        String warehouse =
                Files.createTempDirectory(UUID.randomUUID().toString()).toUri().toString();

        Path tablePath1 = new Path(warehouse, DB + ".db/t1");
        SimpleTableTestHelper testHelper1 = createTestHelper(tablePath1);
        testHelper1.write(GenericRow.of(1, 2L, fromString("1"), fromString("1")));
        testHelper1.write(GenericRow.of(3, 4L, fromString("2"), fromString("2")));
        testHelper1.write(GenericRow.of(5, 6L, fromString("3"), fromString("3")));
        testHelper1.write(
                GenericRow.ofKind(RowKind.DELETE, 3, 4L, fromString("2"), fromString("2")));
        testHelper1.commit();

        Path tablePath2 = new Path(warehouse, "default.db/t2");
        SimpleTableTestHelper testHelper2 = createTestHelper(tablePath2);
        testHelper2.write(GenericRow.of(1, 2L, fromString("1"), fromString("1")));
        testHelper2.write(GenericRow.of(3, 4L, fromString("2"), fromString("2")));
        testHelper2.commit();
        testHelper2.write(GenericRow.of(5, 6L, fromString("3"), fromString("3")));
        testHelper2.write(GenericRow.of(7, 8L, fromString("4"), fromString("4")));
        testHelper2.commit();

        {
            Path tablePath3 = new Path(warehouse, "default.db/t3");
            RowType rowType =
                    new RowType(
                            Arrays.asList(
                                    new DataField(0, "pt", new VarCharType()),
                                    new DataField(1, "a", new IntType()),
                                    new DataField(2, "b", new BigIntType()),
                                    new DataField(3, "c", new BigIntType()),
                                    new DataField(4, "d", new IntType())));
            new SchemaManager(LocalFileIO.create(), tablePath3)
                    .createTable(
                            new Schema(
                                    rowType.getFields(),
                                    Collections.singletonList("pt"),
                                    Collections.emptyList(),
                                    new HashMap<>(),
                                    ""));
            FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath3);
            InnerTableWrite writer = table.newWrite("user");
            InnerTableCommit commit = table.newCommit("user");
            writer.write(GenericRow.of(fromString("1"), 1, 1L, 1L, 1));
            writer.write(GenericRow.of(fromString("1"), 1, 2L, 2L, 2));
            writer.write(GenericRow.of(fromString("2"), 3, 3L, 3L, 3));
            commit.commit(0, writer.prepareCommit(true, 0));
        }

        {
            Path tablePath4 = new Path(warehouse, "default.db/t4");
            RowType rowType =
                    new RowType(
                            Arrays.asList(
                                    new DataField(0, "i", new IntType()),
                                    new DataField(
                                            1,
                                            "map",
                                            new MapType(
                                                    new VarCharType(VarCharType.MAX_LENGTH),
                                                    new VarCharType(VarCharType.MAX_LENGTH)))));
            new SchemaManager(LocalFileIO.create(), tablePath4)
                    .createTable(
                            new Schema(
                                    rowType.getFields(),
                                    Collections.emptyList(),
                                    Collections.singletonList("i"),
                                    ImmutableMap.of("bucket", "1"),
                                    ""));
            FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath4);
            InnerTableWrite writer = table.newWrite("user");
            InnerTableCommit commit = table.newCommit("user");
            writer.write(
                    GenericRow.of(
                            1,
                            new GenericMap(
                                    new HashMap<Object, Object>() {
                                        {
                                            put(fromString("1"), fromString("2"));
                                        }
                                    })));
            commit.commit(0, writer.prepareCommit(true, 0));
        }

        {
            // test for timestamp
            Path tablePath5 = new Path(warehouse, "default.db/test_timestamp");
            RowType rowType =
                    new RowType(
                            Arrays.asList(
                                    new DataField(0, "ts", new TimestampType()),
                                    new DataField(1, "ts_long_0", new TimestampType())));
            new SchemaManager(LocalFileIO.create(), tablePath5)
                    .createTable(
                            new Schema(
                                    rowType.getFields(),
                                    Collections.emptyList(),
                                    Collections.singletonList("ts"),
                                    ImmutableMap.of("bucket", "1"),
                                    ""));
            FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath5);
            InnerTableWrite writer = table.newWrite("user");
            InnerTableCommit commit = table.newCommit("user");
            writer.write(
                    GenericRow.of(
                            Timestamp.fromLocalDateTime(
                                    LocalDateTime.parse("2023-01-01T01:01:01.123")),
                            Timestamp.fromLocalDateTime(
                                    Instant.ofEpochMilli(1672534861123L)
                                            .atZone(ZoneId.systemDefault())
                                            .toLocalDateTime()))); // 2023-01-01T01:01:01.123 UTC
            commit.commit(0, writer.prepareCommit(true, 0));
        }

        {
            // test for decimal
            Path tablePath5 = new Path(warehouse, "default.db/test_decimal");
            RowType rowType =
                    new RowType(
                            Arrays.asList(
                                    new DataField(0, "c1", new DecimalType(20, 0)),
                                    new DataField(1, "c2", new DecimalType(6, 3))));
            new SchemaManager(LocalFileIO.create(), tablePath5)
                    .createTable(
                            new Schema(
                                    rowType.getFields(),
                                    Collections.emptyList(),
                                    Arrays.asList("c1", "c2"),
                                    ImmutableMap.of("bucket", "1"),
                                    ""));
            FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath5);
            InnerTableWrite writer = table.newWrite("user");
            InnerTableCommit commit = table.newCommit("user");
            writer.write(
                    GenericRow.of(
                            Decimal.fromBigDecimal(new BigDecimal("10000000000"), 20, 0),
                            Decimal.fromBigDecimal(new BigDecimal("123.456"), 6, 3)));
            commit.commit(0, writer.prepareCommit(true, 0));
        }

        // partitioned table
        {
            Path tablePath = new Path(warehouse, "default.db/t5");
            RowType rowType =
                    new RowType(
                            Arrays.asList(
                                    new DataField(0, "i1", VarCharType.STRING_TYPE),
                                    new DataField(1, "i2", new IntType()),
                                    new DataField(2, "i3", new IntType())));
            new SchemaManager(LocalFileIO.create(), tablePath)
                    .createTable(
                            new Schema(
                                    rowType.getFields(),
                                    ImmutableList.of("i1", "i2"),
                                    Collections.emptyList(),
                                    ImmutableMap.of("bucket", "1"),
                                    ""));
            FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath);
            InnerTableWrite writer = table.newWrite("user");
            InnerTableCommit commit = table.newCommit("user");
            writer.write(GenericRow.of(BinaryString.fromString("20241103"), 1, 1));
            writer.write(GenericRow.of(BinaryString.fromString("20241103"), 2, 2));
            writer.write(GenericRow.of(BinaryString.fromString("20241104"), 3, 2));
            commit.commit(0, writer.prepareCommit(true, 0));
        }

        // partitioned table
        {
            Path tablePath = new Path(warehouse, "default.db/t6");
            RowType rowType =
                    new RowType(
                            Arrays.asList(
                                    new DataField(0, "i1", new IntType()),
                                    new DataField(1, "i2", VarCharType.STRING_TYPE),
                                    new DataField(2, "i3", new IntType())));
            new SchemaManager(LocalFileIO.create(), tablePath)
                    .createTable(
                            new Schema(
                                    rowType.getFields(),
                                    ImmutableList.of("i2"),
                                    ImmutableList.of("i2", "i1"),
                                    ImmutableMap.of("bucket", "1"),
                                    ""));
            FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath);
            InnerTableWrite writer = table.newWrite("user");
            InnerTableCommit commit = table.newCommit("user");
            writer.write(GenericRow.of(1, BinaryString.fromString("20241103"), 1));
            writer.write(GenericRow.of(2, BinaryString.fromString("20241103"), 2));
            writer.write(GenericRow.of(3, BinaryString.fromString("20241104"), 2));
            commit.commit(0, writer.prepareCommit(true, 0));
        }

        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner =
                    DistributedQueryRunner.builder(
                                    testSessionBuilder()
                                            .setTimeZoneKey(
                                                    TimeZoneKey.getTimeZoneKey(
                                                            ZoneId.systemDefault().getId()))
                                            .setCatalog(CATALOG)
                                            .setSchema(DB)
                                            .build())
                            .build();
            queryRunner.installPlugin(new PrestoPlugin());
            Map<String, String> options = new HashMap<>();
            options.put("warehouse", warehouse);
            queryRunner.createCatalog(CATALOG, CATALOG, options);
            return queryRunner;
        } catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    private static SimpleTableTestHelper createTestHelper(Path tablePath) throws Exception {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "a", new IntType()),
                                new DataField(1, "b", new BigIntType()),
                                // test field name has upper case
                                new DataField(2, "aCa", new VarCharType()),
                                new DataField(3, "d", new CharType(1))));
        return new SimpleTableTestHelper(tablePath, rowType);
    }

    @BeforeSuite
    public void setup() throws Exception {
        // Change the default time zone for presto-tests, like Trino.
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }

    @BeforeTest
    public void init() throws Exception {
        queryRunner = createQueryRunner();
    }

    @AfterTest
    public void clear() throws IOException {
        // TODO Delete default.db
        queryRunner.close();
    }

    @Test
    public void testComplexTypes() throws Exception {
        assertThat(sql("SELECT * FROM paimon.default.t4")).isEqualTo("[[1, {1=2}]]");
    }

    @Test
    public void testSystemTable() throws Exception {
        assertThat(
                        sql(
                                "SELECT snapshot_id,schema_id,commit_user,commit_identifier,commit_kind FROM \"t1$snapshots\""))
                .isEqualTo("[[1, 0, user, 0, APPEND]]");
    }

    @Test
    public void testLimitCommon() throws Exception {
        assertThat(sql("SELECT * FROM paimon.default.t1 LIMIT 1")).isEqualTo("[[1, 2, 1, 1]]");
        assertThat(sql("SELECT * FROM paimon.default.t1 WHERE a = 5 LIMIT 1"))
                .isEqualTo("[[5, 6, 3, 3]]");
    }

    @Test
    public void testProjection() throws Exception {
        assertThat(sql("SELECT * FROM paimon.default.t1"))
                .isEqualTo("[[1, 2, 1, 1], [5, 6, 3, 3]]");
        assertThat(sql("SELECT a, aCa FROM paimon.default.t1")).isEqualTo("[[1, 1], [5, 3]]");
        assertThat(sql("SELECT SUM(b) FROM paimon.default.t1")).isEqualTo("[[8]]");
    }

    @Test
    public void testFilter() throws Exception {
        assertThat(sql("SELECT a, aCa FROM paimon.default.t2 WHERE a < 7"))
                .isEqualTo("[[1, 1], [3, 2], [5, 3]]");
    }

    @Test
    public void testFilterWithTimeTravel() throws Exception {
        // Time travel table t2 to first commit.
        assertThat(
                        sql(
                                "SELECT a, aCa FROM paimon.default.t2 WHERE a < 7",
                                PrestoSessionProperties.SCAN_VERSION,
                                "1"))
                .isEqualTo("[[1, 1], [3, 2]]");
    }

    @Test
    public void testGroupByWithCast() throws Exception {
        assertThat(
                        sql(
                                "SELECT pt, a, SUM(b), SUM(d) FROM paimon.default.t3 GROUP BY pt, a ORDER BY pt, a"))
                .isEqualTo("[[1, 1, 3, 3], [2, 3, 3, 3]]");
    }

    @Test
    public void testTimestampFormat() throws Exception {
        assertThat(
                        sql(
                                "SELECT ts, format_datetime(ts, 'yyyy-MM-dd HH:mm:ss') FROM paimon.default.test_timestamp"))
                .isEqualTo("[[2023-01-01T01:01:01.123, 2023-01-01 01:01:01]]");
    }

    @Test
    public void testDecimal() throws Exception {
        assertThat(sql("SELECT c1, c2 FROM paimon.default.test_decimal"))
                .isEqualTo("[[10000000000, 123.456]]");
    }

    @Test
    public void testTimestampPredicateWithTimezone() throws Exception {
        // Pacific/Apia
        assertThat(
                        sql(
                                "SELECT ts, ts_long_0 FROM paimon.default.test_timestamp "
                                        + "where ts = TIMESTAMP '2023-01-01 01:01:01.123 Pacific/Apia'"))
                .isEqualTo("[]");

        // UTC
        assertThat(
                        sql(
                                "SELECT ts, ts_long_0 FROM paimon.default.test_timestamp "
                                        + "where ts = TIMESTAMP '2023-01-01 01:01:01.123 UTC'"))
                .isEqualTo("[[2023-01-01T01:01:01.123, 2023-01-01T01:01:01.123]]");
    }

    @Test
    public void testTimestampPredicateEq() throws Exception {
        // In UT 1672534861123 is 2023-01-01T01:01:01.123 UTC.

        assertThat(
                        sql(
                                "SELECT ts, ts_long_0 FROM paimon.default.test_timestamp "
                                        + "where ts = TIMESTAMP '2023-01-01 01:01:01.123'"))
                .isEqualTo("[[2023-01-01T01:01:01.123, 2023-01-01T01:01:01.123]]");

        assertThat(
                        sql(
                                "SELECT ts, ts_long_0 FROM paimon.default.test_timestamp "
                                        + "where ts = TIMESTAMP '2023-01-01 01:01:01.123'"))
                .isEqualTo("[[2023-01-01T01:01:01.123, 2023-01-01T01:01:01.123]]");

        assertThat(
                        sql(
                                "SELECT ts, ts_long_0 FROM paimon.default.test_timestamp "
                                        + "WHERE ts_long_0 = date_add("
                                        + "'millisecond', "
                                        + "CAST(1672534861123 % 1000 AS INTEGER), "
                                        + "from_unixtime(CAST(1672534861123 / 1000 AS BIGINT))"
                                        + ")"))
                .isEqualTo("[[2023-01-01T01:01:01.123, 2023-01-01T01:01:01.123]]");

        assertThat(
                        sql(
                                "SELECT ts, ts_long_0 FROM paimon.default.test_timestamp "
                                        + "WHERE ts = TIMESTAMP '2023-01-01 01:01:01.123' "
                                        + "AND ts_long_0 = date_add("
                                        + "'millisecond', "
                                        + "CAST(1672534861123 % 1000 AS INTEGER), "
                                        + "from_unixtime(CAST(1672534861123 / 1000 AS BIGINT)))"))
                .isEqualTo("[[2023-01-01T01:01:01.123, 2023-01-01T01:01:01.123]]");
    }

    @Test
    public void testTimestampPredicate() throws Exception {
        // Test gt and gte.
        assertThat(
                        sql(
                                "SELECT ts FROM paimon.default.test_timestamp "
                                        + "where ts > TIMESTAMP '2023-01-01 01:01:01'"))
                .isEqualTo("[[2023-01-01T01:01:01.123]]");

        assertThat(
                        sql(
                                "SELECT ts FROM paimon.default.test_timestamp "
                                        + "where ts >= TIMESTAMP '2023-01-01 01:01:01.123'"))
                .isEqualTo("[[2023-01-01T01:01:01.123]]");

        // Test lt and lte.
        assertThat(
                        sql(
                                "SELECT ts FROM paimon.default.test_timestamp "
                                        + "where ts < TIMESTAMP '2023-01-01 01:01:02'"))
                .isEqualTo("[[2023-01-01T01:01:01.123]]");

        assertThat(
                        sql(
                                "SELECT ts FROM paimon.default.test_timestamp "
                                        + "where ts <= TIMESTAMP '2023-01-01 01:01:01.123'"))
                .isEqualTo("[[2023-01-01T01:01:01.123]]");

        // Test gt and lt.
        assertThat(
                        sql(
                                "SELECT ts FROM paimon.default.test_timestamp "
                                        + "where ts > TIMESTAMP '2023-01-01 01:01:00' "
                                        + "and ts < TIMESTAMP '2023-01-01 01:01:02'"))
                .isEqualTo("[[2023-01-01T01:01:01.123]]");

        // Test gt and lte.
        assertThat(
                        sql(
                                "SELECT ts FROM paimon.default.test_timestamp "
                                        + "where ts > TIMESTAMP '2023-01-01 01:01:00' "
                                        + "and ts <= TIMESTAMP '2023-01-01 01:01:01.123'"))
                .isEqualTo("[[2023-01-01T01:01:01.123]]");

        // Test gte and lte.
        assertThat(
                        sql(
                                "SELECT ts FROM paimon.default.test_timestamp "
                                        + "where ts >= TIMESTAMP '2023-01-01 01:01:01.123' "
                                        + "and ts <= TIMESTAMP '2023-01-01 01:01:01.123'"))
                .isEqualTo("[[2023-01-01T01:01:01.123]]");

        // Test gte and lt.
        assertThat(
                        sql(
                                "SELECT ts FROM paimon.default.test_timestamp "
                                        + "where ts >= TIMESTAMP '2023-01-01 01:01:01' "
                                        + "and ts < TIMESTAMP '2023-01-01 01:01:02'"))
                .isEqualTo("[[2023-01-01T01:01:01.123]]");
    }

    @Test
    public void testDecimalPredicate() throws Exception {
        // Test eq.
        assertThat(sql("SELECT c2 FROM paimon.default.test_decimal where c2 = 123.456"))
                .isEqualTo("[[123.456]]");

        assertThat(sql("SELECT c1 FROM paimon.default.test_decimal where c1 = 10000000000"))
                .isEqualTo("[[10000000000]]");

        // Test gt and gte.
        assertThat(sql("SELECT c2 FROM paimon.default.test_decimal where c2 > 123"))
                .isEqualTo("[[123.456]]");

        assertThat(sql("SELECT c2 FROM paimon.default.test_decimal where c2 > 123.455"))
                .isEqualTo("[[123.456]]");

        assertThat(sql("SELECT c2 FROM paimon.default.test_decimal where c2 >= 123"))
                .isEqualTo("[[123.456]]");

        assertThat(sql("SELECT c2 FROM paimon.default.test_decimal where c2 >= 123.456"))
                .isEqualTo("[[123.456]]");

        assertThat(sql("SELECT c1 FROM paimon.default.test_decimal where c1 >= 10000000000"))
                .isEqualTo("[[10000000000]]");

        // Test lt and lte.
        assertThat(sql("SELECT c2 FROM paimon.default.test_decimal where c2 < 124"))
                .isEqualTo("[[123.456]]");

        assertThat(sql("SELECT c2 FROM paimon.default.test_decimal where c2 < 123.457"))
                .isEqualTo("[[123.456]]");

        assertThat(sql("SELECT c2 FROM paimon.default.test_decimal where c2 <= 124"))
                .isEqualTo("[[123.456]]");

        assertThat(sql("SELECT c2 FROM paimon.default.test_decimal where c2 <= 123.457"))
                .isEqualTo("[[123.456]]");

        assertThat(sql("SELECT c1 FROM paimon.default.test_decimal where c1 <= 10000000000"))
                .isEqualTo("[[10000000000]]");

        // Test gt and lt.
        assertThat(sql("SELECT c2 FROM paimon.default.test_decimal where c2 > 123 and c2 < 666"))
                .isEqualTo("[[123.456]]");

        // Test gt and lte.
        assertThat(sql("SELECT c2 FROM paimon.default.test_decimal where c2 > 123 and c2 <= 666"))
                .isEqualTo("[[123.456]]");

        // Test gte and lte.
        assertThat(sql("SELECT c2 FROM paimon.default.test_decimal where c2 >= 123 and c2 <= 666"))
                .isEqualTo("[[123.456]]");

        // Test gte and lt.
        assertThat(sql("SELECT c2 FROM paimon.default.test_decimal where c2 >= 123 and c2 < 666"))
                .isEqualTo("[[123.456]]");

        assertThat(
                        sql(
                                "SELECT c1 FROM paimon.default.test_decimal where c1 >= 10000000000 and c1 < 10000000001"))
                .isEqualTo("[[10000000000]]");
    }

    @Test
    public void testPartitionPushDown1() throws Exception {
        assertThat(sql("EXPLAIN SELECT * FROM paimon.default.t5 where upper(i1) = '20241103'"))
                .contains(
                        "LAYOUT: PrestoTableLayoutHandle{tableHandle=PrestoTableHandle{schemaName='default', tableName='t5', filter=TupleDomain{ALL}, partitions=Optional[[{i1=20241103, i2=2}, {i1=20241103, i2=1}]]}, constraintSummary=TupleDomain{ALL}");
        assertThat(sql("SELECT * FROM paimon.default.t5 where upper(i1) = '20241103'"))
                .isEqualTo("[[20241103, 1, 1], [20241103, 2, 2]]");
    }

    @Test
    public void testPartitionPushDown2() throws Exception {
        assertThat(
                        sql(
                                ("EXPLAIN SELECT * FROM paimon.default.t5 where upper(i1) = '20241103' and i2 =1")))
                .contains(
                        "LAYOUT: PrestoTableLayoutHandle{tableHandle=PrestoTableHandle{schemaName='default', tableName='t5', filter=TupleDomain{...}, partitions=Optional[[{i1=20241103, i2=1}]]}, constraintSummary=TupleDomain{...}");
        assertThat(sql("SELECT * FROM paimon.default.t5 where upper(i1) = '20241103' and i2 =1"))
                .isEqualTo("[[20241103, 1, 1]]");
    }

    @Test
    public void testPartitionPushDown3() throws Exception {
        assertThat(
                        sql(
                                ("EXPLAIN SELECT * FROM paimon.default.t5 where upper(i1) = '20241105' and i2 =1")))
                .contains(
                        "LAYOUT: PrestoTableLayoutHandle{tableHandle=PrestoTableHandle{schemaName='default', "
                                + "tableName='t5', filter=TupleDomain{...}, partitions=Optional[[]]}, constraintSummary=TupleDomain{...}}");
        assertThat(sql("SELECT * FROM paimon.default.t5 where upper(i1) = '20241105' and i2 =1"))
                .isEqualTo("[]");
    }

    @Test
    public void testPartitionPushDown4() throws Exception {
        assertThat(
                        sql(
                                "EXPLAIN SELECT * FROM paimon.default.t5 where upper(i1) = "
                                        + "'20241103'and i2 =1",
                                "partition_prune_enabled",
                                "false"))
                .contains(
                        "LAYOUT: PrestoTableLayoutHandle{tableHandle=PrestoTableHandle{schemaName='default', "
                                + "tableName='t5', filter=TupleDomain{...}, partitions=Optional.empty}, constraintSummary=TupleDomain{...}");
        assertThat(
                        sql(
                                "SELECT * FROM paimon.default.t5 where upper(i1) = "
                                        + "'20241103'and i2 =1",
                                "partition_prune_enabled",
                                "false"))
                .isEqualTo("[[20241103, 1, 1]]");
    }

    @Test
    public void testPartitionPushDown5() throws Exception {
        assertThat(sql("SELECT * FROM paimon.default.t6 where upper(i2) = '20241103'"))
                .isEqualTo("[[1, 20241103, 1], [2, 20241103, 2]]");
    }

    private String sql(String sql, String key, String value) throws Exception {
        Session session =
                testSessionBuilder().setCatalogSessionProperty("paimon", key, value).build();
        MaterializedResult result = queryRunner.execute(session, sql);
        return result.getMaterializedRows().stream()
                .map(Object::toString)
                .sorted()
                .collect(Collectors.toList())
                .toString();
    }

    private String sql(String sql) throws Exception {
        MaterializedResult result = queryRunner.execute(sql);
        return result.getMaterializedRows().stream()
                .map(Object::toString)
                .sorted()
                .collect(Collectors.toList())
                .toString();
    }
}
