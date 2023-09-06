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

package com.github.guronas.zipkin.storage.clickhouse;

import com.github.guronas.zipkin.storage.clickhouse.query.InsertSpansQuery;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import zipkin2.Call;
import zipkin2.Span;
import zipkin2.storage.SpanConsumer;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
public class ClickhouseSpanConsumer implements SpanConsumer {
    private final ThreadPoolTaskExecutor executor;
    private final DSLContext dslContext;

    @Override
    public Call<Void> accept(List<Span> spans) {
        if (spans.isEmpty()) {
            return Call.create(null);
        }

        log.trace("Creating ClickHouse call for insert spans into DB: {}", spans);
        return new ClickHouseCall<>(executor, new InsertSpansQuery(dslContext, spans));
    }
}
