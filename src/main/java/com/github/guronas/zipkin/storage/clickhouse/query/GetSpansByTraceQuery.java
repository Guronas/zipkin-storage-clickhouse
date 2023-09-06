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
import zipkin2.Span;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import static com.github.guronas.zipkin.storage.clickhouse.query.ZipkinSpans.TRACE_ID;
import static com.github.guronas.zipkin.storage.clickhouse.query.ZipkinSpans.ZIPKIN_SPANS_TABLE;

@Slf4j
@AllArgsConstructor
public class GetSpansByTraceQuery implements Supplier<List<Span>> {
    private final DSLContext dslContext;
    private final Collection<String> traceIds;

    @Override
    public List<Span> get() {
        log.trace("Getting spans by trace ids {} from DB", traceIds);
        return dslContext.selectFrom(ZIPKIN_SPANS_TABLE)
                .where(TRACE_ID.in(traceIds))
                .fetchStream()
                .map(ClickHouseQueryUtils::buildSpan)
                .toList();
    }
}
