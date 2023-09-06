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

import org.jooq.Context;
import org.jooq.impl.CustomField;
import org.jooq.impl.DSL;
import com.github.guronas.zipkin.storage.clickhouse.ClickHouseStorageException;

import java.util.Map;
import java.util.stream.Collectors;

public class MapFieldValue<K, V> extends CustomField<Map<K, V>> {
    private static final String MAP_STRING_TEMPLATE = "{%s}";
    private final Map<K, V> value;

    private MapFieldValue(Map<K, V> value) {
        super("mapField", ClickhouseDataType.mapDataType());
        this.value = value;
    }

    @Override
    public void accept(Context<?> ctx) {
        String stringValue = value.entrySet()
                .stream()
                .map(entry -> append(entry.getKey(), entry.getValue()))
                .collect(Collectors.joining(","));
        String formattedString = MAP_STRING_TEMPLATE.formatted(stringValue);
        ctx.visit(DSL.val(formattedString));
    }

    public static <K, V> MapFieldValue<K, V> map(Map<K, V> value) {
        return new MapFieldValue<>(value);
    }

    private String append(K key, V value) {
        StringBuilder builder = new StringBuilder();
        append(builder, key);
        builder.append(':');
        append(builder, value);
        return builder.toString();
    }

    private <T> void append(StringBuilder builder, T value) {
        if (value instanceof String) {
            builder.append('\'');
            builder.append(value);
            builder.append('\'');
        } else if (value instanceof Number) {
            builder.append(value);
        } else {
            throw new ClickHouseStorageException("Unable to convert value with type: %s", value.getClass());
        }
    }
}
