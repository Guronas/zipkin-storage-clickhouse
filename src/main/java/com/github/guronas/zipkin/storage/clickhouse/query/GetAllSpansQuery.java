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
import org.jooq.*;
import org.springframework.util.StringUtils;
import zipkin2.Span;
import zipkin2.storage.QueryRequest;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.jooq.impl.DSL.val;
import static com.github.guronas.zipkin.storage.clickhouse.query.condition.MapContainsCondition.mapContains;
import static com.github.guronas.zipkin.storage.clickhouse.query.condition.TwoValueEqualsCondition.twoValueEquals;
import static com.github.guronas.zipkin.storage.clickhouse.query.ZipkinSpans.*;

@Slf4j
@AllArgsConstructor
public class GetAllSpansQuery implements Supplier<List<Span>> {
    private final DSLContext dslContext;
    private final QueryRequest queryRequest;

    @Override
    public List<Span> get() {
        log.trace("Getting spans from DB by request: {}", queryRequest);
        long end = queryRequest.endTs();
        Timestamp endTimestamp = Timestamp.from(Instant.ofEpochMilli(end));
        long lookBack = end - queryRequest.lookback();
        long begin = Math.max(0L, lookBack);
        Timestamp beginTimestamp = Timestamp.from(Instant.ofEpochMilli(begin));
        SelectConditionStep<Record1<String>> query = dslContext.selectDistinct(TRACE_ID)
                .from(ZIPKIN_SPANS_TABLE)
                .where(DATE_TIME.between(beginTimestamp, endTimestamp));

        String serviceName = queryRequest.serviceName();
        if (serviceName != null) {
            query.and(LOCAL_SERVICE_NAME.eq(serviceName));
        }

        String remoteServiceName = queryRequest.remoteServiceName();
        if (remoteServiceName != null) {
            query.and(REMOTE_SERVICE_NAME.eq(remoteServiceName));
        }

        String spanName = queryRequest.spanName();
        if (spanName != null) {
            query.and(NAME.eq(spanName));
        }

        queryRequest.annotationQuery().forEach((key, value) -> {
            if (!StringUtils.hasLength(value)) {
                query.and(mapContains(ANNOTATIONS, key)
                        .or(mapContains(TAGS, key)));
            } else {
                query.and(twoValueEquals(GetMapValueExpression.getMapValue(TAGS, key, ClickhouseDataType.STRING), val(value, ClickhouseDataType.STRING)));
            }
        });

        Long minDuration = queryRequest.minDuration();
        if (minDuration != null) {
            query.and(DURATION.greaterOrEqual(minDuration));
        }
        Long maxDuration = queryRequest.maxDuration();
        if (maxDuration != null) {
            query.and(DURATION.lessOrEqual(maxDuration));
        }

        Result<Record1<String>> traces = query.orderBy(TIMESTAMP.desc())
                .limit(queryRequest.limit())
                .fetch();

        if (traces.isEmpty()) {
            return Collections.emptyList();
        }

        return dslContext.selectFrom(ZIPKIN_SPANS_TABLE)
                .where(DATE_TIME.between(beginTimestamp, endTimestamp))
                .and(TRACE_ID.in(traces))
                .fetchStream()
                .map(ClickHouseQueryUtils::buildSpan)
                .toList();
    }
}
