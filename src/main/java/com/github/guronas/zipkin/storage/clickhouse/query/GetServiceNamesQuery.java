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

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;

import java.util.List;
import java.util.function.Supplier;

import static com.github.guronas.zipkin.storage.clickhouse.query.ZipkinSpans.LOCAL_SERVICE_NAME;
import static com.github.guronas.zipkin.storage.clickhouse.query.ZipkinSpans.ZIPKIN_SPANS_TABLE;
import static com.github.guronas.zipkin.storage.clickhouse.query.condition.NotEmptyCondition.notEmpty;

@Slf4j
@AllArgsConstructor
public class GetServiceNamesQuery implements Supplier<List<String>> {
    private final DSLContext dslContext;

    @Override
    public List<String> get() {
        log.trace("Getting all local services from DB");
        return dslContext.selectDistinct(LOCAL_SERVICE_NAME)
                .from(ZIPKIN_SPANS_TABLE)
                .where(notEmpty(LOCAL_SERVICE_NAME))
                .orderBy(LOCAL_SERVICE_NAME)
                .fetch(LOCAL_SERVICE_NAME);
    }
}
