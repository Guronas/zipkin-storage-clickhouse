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

import com.github.guronas.zipkin.storage.clickhouse.query.*;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.TableField;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.StringUtils;
import zipkin2.Call;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.storage.*;

import java.util.*;

@Slf4j
public class ClickHouseSpanStore implements SpanStore, Traces, ServiceAndSpanNames {
    private final ThreadPoolTaskExecutor clickHouseExecutor;
    private final DSLContext dslContext;
    private final boolean strictTraceId;
    private final ClickHouseCall<List<String>> serviceNamesCall;
    private final Call.Mapper<List<Span>, List<List<Span>>> groupByTraceId;

    public ClickHouseSpanStore(ThreadPoolTaskExecutor clickHouseExecutor,
                               DSLContext dslContext,
                               boolean strictTraceId) {
        this.clickHouseExecutor = clickHouseExecutor;
        this.dslContext = dslContext;
        this.strictTraceId = strictTraceId;
        this.serviceNamesCall = new ClickHouseCall<>(clickHouseExecutor, new GetServiceNamesQuery(dslContext));
        this.groupByTraceId = GroupByTraceId.create(strictTraceId);
    }

    @Override
    public Call<List<List<Span>>> getTraces(QueryRequest request) {
        log.debug("Creating ClickHouse call for getting traces");
        GetAllSpansQuery query = new GetAllSpansQuery(dslContext, request);
        Call<List<List<Span>>> result = new ClickHouseCall<>(clickHouseExecutor, query).map(groupByTraceId);

        result = strictTraceId ? result.map(StrictTraceId.filterTraces(request)) : result;
        return result;
    }

    @Override
    public Call<List<Span>> getTrace(String traceId) {
        log.debug("Creating ClickHouse call for getting trace by trace id [{}]", traceId);

        // make sure we have a 16 or 32 character trace ID
        traceId = Span.normalizeTraceId(traceId);

        // Unless we are strict, truncate the trace ID to 64bit (encoded as 16 characters)
        if (!strictTraceId && traceId.length() == 32) traceId = traceId.substring(16);

        GetSpansByTraceQuery query = new GetSpansByTraceQuery(dslContext, Collections.singleton(traceId));
        return new ClickHouseCall<>(clickHouseExecutor, query);
    }

    @Override
    public Call<List<List<Span>>> getTraces(Iterable<String> traceIds) {
        log.debug("Creating ClickHouse call for getting trace by trace ids [{}]", traceIds);
        Set<String> normalizedTraceIds = new LinkedHashSet<>();
        for (String traceId : traceIds) {
            // make sure we have a 16 or 32 character trace ID
            traceId = Span.normalizeTraceId(traceId);

            // Unless we are strict, truncate the trace ID to 64bit (encoded as 16 characters)
            if (!strictTraceId && traceId.length() == 32) traceId = traceId.substring(16);

            normalizedTraceIds.add(traceId);
        }

        if (normalizedTraceIds.isEmpty()) {
            return Call.emptyList();
        }
        GetSpansByTraceQuery query = new GetSpansByTraceQuery(dslContext, normalizedTraceIds);
        return new ClickHouseCall<>(clickHouseExecutor, query).map(groupByTraceId);
    }

    @Override
    public Call<List<String>> getServiceNames() {
        log.debug("Creating ClickHouse call for getting local service names");
        return serviceNamesCall.clone();
    }

    @Override
    public Call<List<String>> getSpanNames(String serviceName) {
        log.debug("Creating ClickHouse call for getting span names by service name [{}]", serviceName);
        return aggregateFieldsByServiceName(serviceName, ZipkinSpans.NAME);
    }

    @Override
    public Call<List<DependencyLink>> getDependencies(long endTs, long lookback) {
        //We don't use this functionality
        log.warn("Dependencies functionality is not supported in this version");
        return Call.emptyList();
    }

    @Override
    public Call<List<String>> getRemoteServiceNames(String serviceName) {
        log.debug("Creating ClickHouse call for getting remote service names");
        return aggregateFieldsByServiceName(serviceName, ZipkinSpans.REMOTE_SERVICE_NAME);
    }

    private Call<List<String>> aggregateFieldsByServiceName(String serviceName, TableField<Record, String> field) {
        if (StringUtils.hasLength(serviceName)) {
            return Call.emptyList();
        }
        AggregationByServiceNameQuery query = new AggregationByServiceNameQuery(dslContext, serviceName, field);
        return new ClickHouseCall<>(clickHouseExecutor, query);
    }
}
