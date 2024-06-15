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
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.shade.guava30.com.google.common.base.Verify;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.InternalRowUtils;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.PrestoException;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.Decimals.encodeShortScaledValue;
import static com.facebook.presto.common.type.Decimals.isLongDecimal;
import static com.facebook.presto.common.type.Decimals.isShortDecimal;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.String.format;

/** Presto {@link ConnectorPageSource}. */
public abstract class PrestoPageSourceBase implements ConnectorPageSource {

    private static final int ROWS_PER_REQUEST = 4096;

    private final CloseableIterator<InternalRow> iterator;
    private final PageBuilder pageBuilder;
    private final List<Type> prestoColumnTypes;
    private final List<DataType> paimonColumnTypes;

    private boolean isFinished = false;
    private long numReturn = 0;

    public PrestoPageSourceBase(
            RecordReader<InternalRow> reader, List<ColumnHandle> projectedColumns) {
        this.iterator = reader.toCloseableIterator();
        this.prestoColumnTypes = new ArrayList<>();
        this.paimonColumnTypes = new ArrayList<>();
        for (ColumnHandle handle : projectedColumns) {
            PrestoColumnHandle prestoColumnHandle = (PrestoColumnHandle) handle;
            prestoColumnTypes.add(prestoColumnHandle.getPrestoType());
            paimonColumnTypes.add(prestoColumnHandle.paimonType());
        }

        this.pageBuilder = new PageBuilder(prestoColumnTypes);
    }

    @Override
    public long getCompletedBytes() {
        return 0;
    }

    @Override
    public long getCompletedPositions() {
        return 0;
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public boolean isFinished() {
        return isFinished;
    }

    @Override
    public Page getNextPage() {
        return ClassLoaderUtils.runWithContextClassLoader(
                () -> {
                    try {
                        return nextPage();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                },
                PrestoPageSourceBase.class.getClassLoader());
    }

    @Override
    public long getSystemMemoryUsage() {
        return 0;
    }

    @Nullable
    private Page nextPage() throws IOException {
        int count = 0;
        while (count < ROWS_PER_REQUEST && !pageBuilder.isFull()) {
            if (!iterator.hasNext()) {
                isFinished = true;
                return returnPage(count);
            }

            InternalRow row = iterator.next();
            pageBuilder.declarePosition();
            count++;
            for (int i = 0; i < prestoColumnTypes.size(); i++) {
                BlockBuilder output = pageBuilder.getBlockBuilder(i);
                appendTo(
                        prestoColumnTypes.get(i),
                        paimonColumnTypes.get(i),
                        InternalRowUtils.get(row, i, paimonColumnTypes.get(i)),
                        output);
            }
        }

        return returnPage(count);
    }

    private Page returnPage(int count) {
        if (count == 0) {
            return null;
        }
        numReturn += count;
        Page page = pageBuilder.build();
        pageBuilder.reset();
        return page;
    }

    @Override
    public void close() throws IOException {
        try {
            this.iterator.close();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private void appendTo(Type prestoType, DataType paimonType, Object value, BlockBuilder output) {
        if (value == null) {
            output.appendNull();
            return;
        }

        Class<?> javaType = prestoType.getJavaType();
        if (javaType == boolean.class) {
            prestoType.writeBoolean(output, (Boolean) value);
        } else if (javaType == long.class) {
            if (prestoType.equals(BIGINT)
                    || prestoType.equals(INTEGER)
                    || prestoType.equals(TINYINT)
                    || prestoType.equals(SMALLINT)
                    || prestoType.equals(DATE)) {
                prestoType.writeLong(output, ((Number) value).longValue());
            } else if (prestoType.equals(REAL)) {
                prestoType.writeLong(output, Float.floatToIntBits((Float) value));
            } else if (prestoType instanceof DecimalType) {
                Verify.verify(isShortDecimal(prestoType), "The type should be short decimal");
                DecimalType decimalType = (DecimalType) prestoType;
                BigDecimal decimal = ((Decimal) value).toBigDecimal();
                prestoType.writeLong(
                        output, encodeShortScaledValue(decimal, decimalType.getScale()));
            } else if (prestoType.equals(TIMESTAMP)) {
                prestoType.writeLong(
                        output,
                        ((Timestamp) value)
                                .toLocalDateTime()
                                .atZone(ZoneId.systemDefault())
                                .toInstant()
                                .toEpochMilli());
            } else if (prestoType.equals(TIME)) {
                prestoType.writeLong(output, (int) value * 1_000);
            } else {
                throw new PrestoException(
                        GENERIC_INTERNAL_ERROR,
                        format("Unhandled type for %s: %s", javaType.getSimpleName(), prestoType));
            }
        } else if (javaType == double.class) {
            prestoType.writeDouble(output, ((Number) value).doubleValue());
        } else if (prestoType instanceof DecimalType) {
            writeObject(output, prestoType, value);
        } else if (javaType == Slice.class) {
            writeSlice(output, prestoType, value);
        } else if (javaType == Block.class) {
            writeBlock(output, prestoType, paimonType, value);
        } else {
            throw new PrestoException(
                    GENERIC_INTERNAL_ERROR,
                    format("Unhandled type for %s: %s", javaType.getSimpleName(), prestoType));
        }
    }

    private static void writeSlice(BlockBuilder output, Type type, Object value) {
        if (type instanceof VarcharType || type instanceof CharType) {
            type.writeSlice(output, wrappedBuffer(((BinaryString) value).toBytes()));
        } else if (type instanceof VarbinaryType) {
            type.writeSlice(output, wrappedBuffer((byte[]) value));
        } else {
            throw new PrestoException(
                    GENERIC_INTERNAL_ERROR, "Unhandled type for Slice: " + type.getTypeSignature());
        }
    }

    private static void writeObject(BlockBuilder output, Type type, Object value) {
        if (type instanceof DecimalType) {
            Verify.verify(isLongDecimal(type), "The type should be long decimal");
            DecimalType decimalType = (DecimalType) type;
            BigDecimal decimal = ((Decimal) value).toBigDecimal();
            type.writeSlice(output, Decimals.encodeScaledValue(decimal, decimalType.getScale()));
        } else {
            throw new PrestoException(
                    GENERIC_INTERNAL_ERROR,
                    "Unhandled type for Object: " + type.getTypeSignature());
        }
    }

    private void writeBlock(
            BlockBuilder output, Type prestoType, DataType paimonType, Object value) {
        if (prestoType instanceof ArrayType) {
            BlockBuilder builder = output.beginBlockEntry();

            InternalArray arrayData = (InternalArray) value;
            DataType elementType = DataTypeChecks.getNestedTypes(paimonType).get(0);
            for (int i = 0; i < arrayData.size(); i++) {
                appendTo(
                        prestoType.getTypeParameters().get(0),
                        elementType,
                        InternalRowUtils.get(arrayData, i, elementType),
                        builder);
            }

            output.closeEntry();
            return;
        }
        if (prestoType instanceof RowType) {
            InternalRow rowData = (InternalRow) value;
            BlockBuilder builder = output.beginBlockEntry();
            for (int index = 0; index < prestoType.getTypeParameters().size(); index++) {
                Type fieldType = prestoType.getTypeParameters().get(index);
                DataType fieldLogicalType =
                        ((org.apache.paimon.types.RowType) paimonType).getTypeAt(index);
                appendTo(
                        fieldType,
                        fieldLogicalType,
                        InternalRowUtils.get(rowData, index, fieldLogicalType),
                        builder);
            }
            output.closeEntry();
            return;
        }
        if (prestoType instanceof MapType) {
            InternalMap mapData = (InternalMap) value;
            InternalArray keyArray = mapData.keyArray();
            InternalArray valueArray = mapData.valueArray();
            DataType keyType = ((org.apache.paimon.types.MapType) paimonType).getKeyType();
            DataType valueType = ((org.apache.paimon.types.MapType) paimonType).getValueType();
            BlockBuilder builder = output.beginBlockEntry();
            for (int i = 0; i < keyArray.size(); i++) {
                appendTo(
                        prestoType.getTypeParameters().get(0),
                        keyType,
                        InternalRowUtils.get(keyArray, i, keyType),
                        builder);
                appendTo(
                        prestoType.getTypeParameters().get(1),
                        valueType,
                        InternalRowUtils.get(valueArray, i, valueType),
                        builder);
            }
            output.closeEntry();
            return;
        }
        throw new PrestoException(
                GENERIC_INTERNAL_ERROR,
                "Unhandled type for Block: " + prestoType.getTypeSignature());
    }
}
