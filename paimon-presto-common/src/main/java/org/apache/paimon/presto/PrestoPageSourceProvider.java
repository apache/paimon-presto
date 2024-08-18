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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveTableLayoutHandle;
import com.facebook.presto.hive.HiveType;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VarCharType;

import static org.apache.paimon.presto.ClassLoaderUtils.runWithContextClassLoader;

/** Presto {@link ConnectorPageSourceProvider}. */
public class PrestoPageSourceProvider implements ConnectorPageSourceProvider {
    private static final Logger log = Logger.get(PrestoPageSourceProvider.class);

    private PrestoMetadata paimonMetadata;

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableLayoutHandle layout,
            List<ColumnHandle> columns,
            SplitContext splitContext) {
        return runWithContextClassLoader(
                () -> {
                    PrestoTableLayoutHandle prestoLayout;
                    PrestoSplit prestoSplit;
                        if(layout.getClass().getSimpleName().equals("HiveTableLayoutHandle")){
                            long start=System.currentTimeMillis();
                            try{// 如果直接调用会报错：没在同一个classLoader,因此用反射
                                prestoSplit=new PrestoSplit(split.getClass().getMethod("getSplitSerialized").invoke(split)+"");
                            }catch (Exception e){
                                throw new RuntimeException(e);
                            }

                            // 初始化元数据
                            HiveTableLayoutHandle hiveLayout = (HiveTableLayoutHandle) layout;
                            PrestoTableHandle tableHandle = paimonMetadata.getTableHandle(hiveLayout.getSchemaTableName());
                            prestoLayout = new PrestoTableLayoutHandle(tableHandle, hiveLayout.getPartitionColumnPredicate());
                            log.info("初始化paimon表:%s相关信息,耗时:%s",hiveLayout.getSchemaTableName(),System.currentTimeMillis()-start);
                        }else{
                            prestoLayout=(PrestoTableLayoutHandle) layout;
                            prestoSplit=(PrestoSplit) split;
                        }

                        return createPageSource(prestoLayout.getTableHandle(),prestoSplit, columns);
                    },
                PrestoPageSourceProvider.class.getClassLoader());
    }

    // hive catalog中进行设置此值
    public void setPaimonMetadata(PrestoMetadata paimonMetadata) {
        this.paimonMetadata = paimonMetadata;
    }

    private ConnectorPageSource createPageSource(
            PrestoTableHandle tableHandle, PrestoSplit split, List<ColumnHandle> columns) {
        Table table = tableHandle.table();
        ReadBuilder read = table.newReadBuilder();
        RowType rowType = table.rowType();
        List<String> fieldNames = FieldNameUtils.fieldNames(rowType);
        List<ColumnHandle> prestoColumns=columns;
        if(!columns.isEmpty() && columns.get(0).getClass().getSimpleName().equals("HiveColumnHandle")){
            // 如果使用hive查询paimon表
            prestoColumns =
                    columns.stream()
                            .map(HiveColumnHandle.class::cast)
                            .map((hiveColumn)-> hiveColumn2PrestoColumn(hiveColumn))
                            .collect(Collectors.toList());
        }
        List<String> projectedFields =
                prestoColumns.stream()
                        .map(PrestoColumnHandle.class::cast)
                        .map(PrestoColumnHandle::getColumnName)
                        .collect(Collectors.toList());

        if (!fieldNames.equals(projectedFields)) {
            int[] projected = projectedFields.stream().mapToInt(fieldNames::indexOf).toArray();
            read.withProjection(projected);
        }

        new PrestoFilterConverter(rowType)
                .convert(tableHandle.getFilter())
                .ifPresent(read::withFilter);

        try {
            return new PrestoPageSource(
                    read.newRead().executeFilter().createReader(split.decodeSplit()), prestoColumns);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private PrestoColumnHandle hiveColumn2PrestoColumn(HiveColumnHandle hiveColumn){
        HiveType hiveType = hiveColumn.getHiveType();
        DataType dataType = new VarCharType();
        if(hiveType.getHiveTypeName().equals(HiveType.HIVE_INT.getHiveTypeName())){
            dataType=new IntType();
        }
        if(hiveType.getHiveTypeName().equals(HiveType.HIVE_BOOLEAN.getHiveTypeName())){
            dataType=new BooleanType();
        }
        if(hiveType.getHiveTypeName().equals(HiveType.HIVE_BINARY.getHiveTypeName())){
            dataType=new BinaryType();
        }
        if(hiveType.getHiveTypeName().equals(HiveType.HIVE_DATE.getHiveTypeName())){
            dataType=new DateType();
        }
        if(hiveType.getHiveTypeName().equals(HiveType.HIVE_DOUBLE.getHiveTypeName())){
            dataType=new DoubleType();
        }
        if(hiveType.getHiveTypeName().equals(HiveType.HIVE_FLOAT.getHiveTypeName())){
            dataType=new FloatType();
        }
        if(hiveType.getHiveTypeName().equals(HiveType.HIVE_LONG.getHiveTypeName())){
            dataType=new BigIntType();
        }
        if(hiveType.getHiveTypeName().equals(HiveType.HIVE_SHORT.getHiveTypeName())){
            dataType=new SmallIntType();
        }
        if(hiveType.getHiveTypeName().equals(HiveType.HIVE_TIMESTAMP.getHiveTypeName())){
            dataType=new TimestampType();
        }
        return PrestoColumnHandle.create(hiveColumn.getName(),dataType, paimonMetadata.getTypeManager());
    }
}
