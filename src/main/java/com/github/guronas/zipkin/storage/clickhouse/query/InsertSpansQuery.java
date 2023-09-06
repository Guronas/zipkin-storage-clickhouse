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
import org.jooq.Record;
import org.springframework.util.StringUtils;
import zipkin2.Annotation;
import zipkin2.Endpoint;
import zipkin2.Span;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.github.guronas.zipkin.storage.clickhouse.query.MapFieldValue.map;
import static com.github.guronas.zipkin.storage.clickhouse.query.ZipkinSpans.*;

@Slf4j
@RequiredArgsConstructor
public class InsertSpansQuery implements Supplier<Void> {
    private final DSLContext dslContext;
    private final Collection<Span> spans;

    @Override
    public Void get() {
        log.trace("Inserting new spans into DB: {}", spans);
        InsertSetStep<Record> insert = dslContext.insertInto(ZIPKIN_SPANS_TABLE);
        spans.stream()
                .map(span -> createInsert(span, insert))
                .peek(InsertSetMoreStep::newRecord)
                .reduce((previousInsert, nextInsert) -> nextInsert)
                .ifPresent(Query::execute);

        return null;
    }

    private InsertSetMoreStep<Record> createInsert(Span span, InsertSetStep<Record> insert) {
        //We should store timestamp in epoch micros
        long timestamp = span.timestamp() == null ? TimeUnit.SECONDS.toMicros(Instant.now().getEpochSecond()) : span.timestamp();
        Endpoint localEndpoint = span.localEndpoint();
        Endpoint remoteEndpoint = span.remoteEndpoint();
        Map<String, Long> annotations = span.annotations()
                .stream()
                .collect(Collectors.toMap(Annotation::value, Annotation::timestamp));
        String parentId = span.parentId();
        if (!StringUtils.hasLength(parentId)) {
            parentId = span.id();
        }

        String name = span.name();
        if (name == null) {
            Record1<String> rec = dslContext.selectDistinct(NAME)
                    .from(ZIPKIN_SPANS_TABLE)
                    .where((ID.eq(span.id())
                            .and(NAME.isNotNull()))
                            .or(ID.eq(span.parentId()))
                            .or(PARENT_ID.eq(span.id())))
                    .fetchAny();

            name = (rec != null) ? rec.component1() : "processing task";
        }

        Optional.ofNullable(localEndpoint)
                .ifPresent(endpoint -> setEndpoint(insert, endpoint));

        Optional.ofNullable(remoteEndpoint)
                .ifPresent(endpoint -> setEndpoint(insert, endpoint));

        Timestamp dateTime = Timestamp.from(Instant.ofEpochMilli(TimeUnit.MICROSECONDS.toMillis(timestamp)));
        return insert.set(TRACE_ID, span.traceId())
                .set(PARENT_ID, parentId)
                .set(ID, span.id())
                .set(KIND, span.kind())
                .set(NAME, name)
                .set(TIMESTAMP, timestamp)
                .set(DATE_TIME, dateTime)
                .set(DURATION, span.duration())
                .set(LOCAL_SERVICE_NAME, span.localServiceName())
                .set(REMOTE_SERVICE_NAME, span.remoteServiceName())
                .set(ANNOTATIONS, map(annotations))
                .set(TAGS, map(span.tags()))
                .set(SHARED, ClickHouseQueryUtils.convertBooleanToShort(span.shared()))
                .set(DEBUG, ClickHouseQueryUtils.convertBooleanToShort(span.debug()));
    }

    private void setEndpoint(InsertSetStep<Record> insert, Endpoint endpoint) {
        insert.set(LOCAL_IPV4, endpoint.ipv4())
                .set(LOCAL_IPV6, endpoint.ipv6())
                .set(LOCAL_PORT, endpoint.port());
    }
}
