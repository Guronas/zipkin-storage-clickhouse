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

package com.github.guronas.zipkin.storage.clickhouse.query.condition;

import lombok.RequiredArgsConstructor;
import org.jooq.Condition;
import org.jooq.Context;
import org.jooq.Record;
import org.jooq.TableField;
import org.jooq.impl.CustomCondition;

import java.util.Map;

@RequiredArgsConstructor
public class MapContainsCondition<K, V> extends CustomCondition {
    private final TableField<Record, Map<K, V>> field;
    private final String key;

    @Override
    public void accept(Context<?> ctx) {
        ctx.sql("mapContains(")
                .visit(field)
                .sql(",'")
                .sql(key)
                .sql("')");
    }

    public static <K, V> Condition mapContains(TableField<Record, Map<K, V>> field, String key) {
        return new MapContainsCondition<>(field, key);
    }
}
