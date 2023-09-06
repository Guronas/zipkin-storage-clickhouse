/*
 * Copyright (c) 2023 Maksim Frolov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.guronas.zipkin.storage.clickhouse.query;

import org.jooq.DataType;
import org.jooq.impl.DefaultDataType;

import java.sql.Timestamp;
import java.util.Map;

public class ClickhouseDataType {

    public static final DataType<String> STRING = new DefaultDataType<>(null, String.class, "String");

    public static final DataType<Short> UINT8 = new DefaultDataType<>(null, Short.class, "Uint8");

    public static final DataType<Integer> INT32 = new DefaultDataType<>(null, Integer.class, "Int32");

    public static final DataType<Long> INT64 = new DefaultDataType<>(null, Long.class, "Int64");

    public static final DataType<Timestamp> DATE_TIME = new DefaultDataType<>(null, Timestamp.class, "DateTime");

    public static <T extends Enum<T>> DataType<T> enumDataType(Class<T> clazz) {
        return new DefaultDataType<>(null, String.class, "Enum8").asConvertedDataType(new ClickHouseEnumConverter<>(clazz));
    }

    public static <K, V> DataType<Map<K, V>> mapDataType() {
        return new DefaultDataType(null, Map.class, "Map");
    }

}
