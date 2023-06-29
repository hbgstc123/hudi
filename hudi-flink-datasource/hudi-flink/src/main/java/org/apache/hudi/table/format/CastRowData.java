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

package org.apache.hudi.table.format;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;
import org.apache.hudi.common.util.collection.Pair;

import java.util.Map;
import java.util.function.Function;

/**
 * Cast data type of `oldData` according to `colChangeInfo` when reading data.
 */
public final class CastRowData implements RowData {

    private final RowData oldData;
    private final Map<Integer, Pair<LogicalType, Function<Object, Object>>> colChangeInfo;

    public CastRowData(RowData oldData, Map<Integer, Pair<LogicalType, Function<Object, Object>>> colChangeInfo) {
        this.oldData = oldData;
        this.colChangeInfo = colChangeInfo;
    }

    private Object convertDataType(int pos) {
        assert colChangeInfo.containsKey(pos);
        Object oldValue = RowData.createFieldGetter(colChangeInfo.get(pos).getLeft(), pos).getFieldOrNull(oldData);
        return colChangeInfo.get(pos).getRight().apply(oldValue);
    }

    @Override
    public RowKind getRowKind() {
        return oldData.getRowKind();
    }

    @Override
    public void setRowKind(RowKind kind) {
        oldData.setRowKind(kind);
    }

    @Override
    public int getArity() {
        return oldData.getArity();
    }

    @Override
    public boolean isNullAt(int pos) {
        return oldData.isNullAt(pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        return colChangeInfo.containsKey(pos)
                ? (boolean) convertDataType(pos)
                : oldData.getBoolean(pos);
    }

    @Override
    public byte getByte(int pos) {
        return colChangeInfo.containsKey(pos)
                ? (byte) convertDataType(pos)
                : oldData.getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        return colChangeInfo.containsKey(pos)
                ? (short) convertDataType(pos)
                : oldData.getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        return colChangeInfo.containsKey(pos)
                ? (int) convertDataType(pos)
                : oldData.getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        return colChangeInfo.containsKey(pos)
                ? (long) convertDataType(pos)
                : oldData.getLong(pos);
    }

    @Override
    public float getFloat(int pos) {
        return colChangeInfo.containsKey(pos)
                ? (float) convertDataType(pos)
                : oldData.getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        return colChangeInfo.containsKey(pos)
                ? (double) convertDataType(pos)
                : oldData.getDouble(pos);
    }

    @Override
    public StringData getString(int pos) {
        return colChangeInfo.containsKey(pos)
                ? (StringData) convertDataType(pos)
                : oldData.getString(pos);
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
        return colChangeInfo.containsKey(pos)
                ? (DecimalData) convertDataType(pos)
                : oldData.getDecimal(pos, precision, scale);
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
        return colChangeInfo.containsKey(pos)
                ? (TimestampData) convertDataType(pos)
                : oldData.getTimestamp(pos, precision);
    }

    @Override
    public <T> RawValueData<T> getRawValue(int pos) {
        throw new UnsupportedOperationException("RawValueData is not supported.");
    }

    @Override
    public byte[] getBinary(int pos) {
        return colChangeInfo.containsKey(pos)
                ? (byte[]) convertDataType(pos)
                : oldData.getBinary(pos);
    }

    @Override
    public RowData getRow(int pos, int numFields) {
        return colChangeInfo.containsKey(pos)
                ? (RowData) convertDataType(pos)
                : oldData.getRow(pos, numFields);
    }

    @Override
    public ArrayData getArray(int pos) {
        return colChangeInfo.containsKey(pos)
                ? (ArrayData) convertDataType(pos)
                : oldData.getArray(pos);
    }

    @Override
    public MapData getMap(int pos) {
        return colChangeInfo.containsKey(pos)
                ? (MapData) convertDataType(pos)
                : oldData.getMap(pos);
    }

    @Override
    public boolean equals(Object o) {
        throw new UnsupportedOperationException(
                "ColumnarRowData do not support equals, please compare fields one by one!");
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException(
                "ColumnarRowData do not support hashCode, please hash fields one by one!");
    }
}
