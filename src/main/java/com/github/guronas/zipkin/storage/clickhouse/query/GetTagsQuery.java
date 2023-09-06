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

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.*;
import org.jooq.impl.DSL;

import java.util.List;
import java.util.function.Supplier;

import static com.github.guronas.zipkin.storage.clickhouse.query.condition.NotEmptyCondition.notEmpty;

@Slf4j
@RequiredArgsConstructor
public class GetTagsQuery implements Supplier<List<String>> {
    private static final Field<String> MAP_VALUE_ALIAS = DSL.field("value", String.class);
    private final DSLContext dslContext;
    private final String key;

    @Override
    public List<String> get() {
        log.trace("Getting tags from DB by key [{}]", key);
        GetMapValueExpression<String, String> value = GetMapValueExpression.getMapValue(ZipkinSpans.TAGS, key, ClickhouseDataType.STRING);
        return dslContext.selectDistinct(value.as(MAP_VALUE_ALIAS))
                .from(ZipkinSpans.ZIPKIN_SPANS_TABLE)
                .where(notEmpty(DSL.field(MAP_VALUE_ALIAS)))
                .fetch(MAP_VALUE_ALIAS);
    }
}
