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
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.RowType;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Marker;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.SqlTimestampWithTimeZone;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import io.airlift.slice.Slice;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Presto filter to Paimon predicate. */
public class PrestoFilterConverter {

    private final RowType rowType;
    private final PredicateBuilder builder;

    public PrestoFilterConverter(RowType rowType) {
        this.rowType = rowType;
        this.builder = new PredicateBuilder(rowType);
    }

    public Optional<Predicate> convert(TupleDomain<PrestoColumnHandle> tupleDomain) {
        if (tupleDomain.isAll()) {
            return Optional.empty();
        }

        if (!tupleDomain.getDomains().isPresent()) {
            return Optional.empty();
        }

        Map<PrestoColumnHandle, Domain> domainMap = tupleDomain.getDomains().get();
        List<Predicate> conjuncts = new ArrayList<>();
        for (Map.Entry<PrestoColumnHandle, Domain> entry : domainMap.entrySet()) {
            PrestoColumnHandle columnHandle = entry.getKey();
            Domain domain = entry.getValue();
            int index = rowType.getFieldNames().indexOf(columnHandle.getColumnName());
            if (index != -1) {
                try {
                    conjuncts.add(toPredicate(index, columnHandle.getPrestoType(), domain));
                } catch (UnsupportedOperationException ignored) {
                }
            }
        }

        if (conjuncts.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(PredicateBuilder.and(conjuncts));
    }

    private Predicate toPredicate(int columnIndex, Type type, Domain domain) {
        if (domain.isAll()) {
            // TODO alwaysTrue
            throw new UnsupportedOperationException();
        }
        if (domain.getValues().isNone()) {
            if (domain.isNullAllowed()) {
                return builder.isNull((columnIndex));
            }
            // TODO alwaysFalse
            throw new UnsupportedOperationException();
        }

        if (domain.getValues().isAll()) {
            if (domain.isNullAllowed()) {
                // TODO alwaysTrue
                throw new UnsupportedOperationException();
            }
            return builder.isNotNull((columnIndex));
        }

        // TODO support structural types
        if (type instanceof ArrayType
                || type instanceof MapType
                || type instanceof com.facebook.presto.common.type.RowType) {
            // Fail fast. Ignoring expression could lead to data loss in case of deletions.
            throw new UnsupportedOperationException();
        }

        if (type.isOrderable()) {
            List<Range> orderedRanges = domain.getValues().getRanges().getOrderedRanges();
            List<Object> values = new ArrayList<>();
            List<Predicate> predicates = new ArrayList<>();
            for (Range range : orderedRanges) {
                if (range.isSingleValue()) {
                    values.add(getLiteralValue(type, range.getLow().getValue()));
                } else {
                    predicates.add(toPredicate(columnIndex, range));
                }
            }

            if (!values.isEmpty()) {
                predicates.add(builder.in(columnIndex, values));
            }

            if (domain.isNullAllowed()) {
                predicates.add(builder.isNull(columnIndex));
            }
            return PredicateBuilder.or(predicates);
        }

        throw new UnsupportedOperationException();
    }

    private Predicate toPredicate(int columnIndex, Range range) {
        Type type = range.getType();

        if (range.isSingleValue()) {
            Object value = getLiteralValue(type, range.getSingleValue());
            return builder.equal(columnIndex, value);
        }

        List<Predicate> conjuncts = new ArrayList<>(2);
        if (!range.getLow().isLowerUnbounded()) {
            Object low = getLiteralValue(type, range.getLow().getValue());
            Predicate lowBound;
            if (range.getLow().getBound() == Marker.Bound.EXACTLY) {
                lowBound = builder.greaterOrEqual(columnIndex, low);
            } else {
                lowBound = builder.greaterThan(columnIndex, low);
            }
            conjuncts.add(lowBound);
        }

        if (!range.getHigh().isUpperUnbounded()) {
            Object high = getLiteralValue(type, range.getHigh().getValue());
            Predicate highBound;
            if (range.getHigh().getBound() == Marker.Bound.EXACTLY) {
                highBound = builder.lessOrEqual(columnIndex, high);
            } else {
                highBound = builder.lessThan(columnIndex, high);
            }
            conjuncts.add(highBound);
        }

        return PredicateBuilder.and(conjuncts);
    }

    private Object getLiteralValue(Type type, Object prestoNativeValue) {
        Objects.requireNonNull(prestoNativeValue, "prestoNativeValue is null");

        if (type instanceof BooleanType) {
            return prestoNativeValue;
        }

        if (type instanceof IntegerType) {
            return Math.toIntExact((long) prestoNativeValue);
        }

        if (type instanceof BigintType) {
            return prestoNativeValue;
        }

        if (type instanceof RealType) {
            return Float.intBitsToFloat(Math.toIntExact((long) prestoNativeValue));
        }

        if (type instanceof DoubleType) {
            return prestoNativeValue;
        }

        if (type instanceof DateType) {
            return Math.toIntExact(((Long) prestoNativeValue));
        }

        if (type instanceof TimeType) {
            return (int) ((long) prestoNativeValue / 1_000);
        }

        if (type instanceof TimestampType) {
            return Timestamp.fromLocalDateTime(
                    Instant.ofEpochMilli((Long) prestoNativeValue)
                            .atZone(ZoneId.systemDefault())
                            .toLocalDateTime());
        }

        if (type instanceof TimestampWithTimeZoneType) {
            if (prestoNativeValue instanceof Long) {
                return prestoNativeValue;
            }
            return Timestamp.fromEpochMillis(
                    ((SqlTimestampWithTimeZone) prestoNativeValue).getMillisUtc());
        }

        if (type instanceof VarcharType || type instanceof CharType) {
            return BinaryString.fromBytes(((Slice) prestoNativeValue).getBytes());
        }

        if (type instanceof VarbinaryType) {
            return ((Slice) prestoNativeValue).getBytes();
        }

        if (type instanceof DecimalType) {
            // Refer to trino.
            DecimalType decimalType = (DecimalType) type;
            BigDecimal bigDecimal;
            if (prestoNativeValue instanceof Long) {
                bigDecimal =
                        BigDecimal.valueOf((long) prestoNativeValue)
                                .movePointLeft(decimalType.getScale());
            } else {
                bigDecimal =
                        new BigDecimal(
                                Decimals.decodeUnscaledValue((Slice) prestoNativeValue),
                                decimalType.getScale());
            }
            return Decimal.fromBigDecimal(
                    bigDecimal, decimalType.getPrecision(), decimalType.getScale());
        }

        throw new UnsupportedOperationException("Unsupported type: " + type);
    }
}
