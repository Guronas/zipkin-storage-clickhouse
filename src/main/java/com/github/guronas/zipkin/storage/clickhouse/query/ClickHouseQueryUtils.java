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

import org.jooq.Field;
import org.jooq.Record;
import org.springframework.lang.Nullable;
import zipkin2.Endpoint;
import zipkin2.Span;

import java.util.Map;

import static java.lang.Boolean.TRUE;
import static com.github.guronas.zipkin.storage.clickhouse.query.ZipkinSpans.*;

public class ClickHouseQueryUtils {

    public static Span buildSpan(Record record) {
        Endpoint localEndpoint = buildEndpoint(record, LOCAL_SERVICE_NAME, LOCAL_IPV4, LOCAL_IPV6, LOCAL_PORT);
        Endpoint remoteEndpoint = buildEndpoint(record, REMOTE_SERVICE_NAME, REMOTE_IPV4, REMOTE_IPV6, REMOTE_PORT);
        Span.Builder spanBuilder = Span.newBuilder()
                .traceId(record.getValue(TRACE_ID))
                .parentId(record.getValue(PARENT_ID))
                .id(record.getValue(ID))
                .kind(record.getValue(KIND))
                .name(record.getValue(NAME))
                .timestamp(record.getValue(TIMESTAMP))
                .duration(record.getValue(DURATION))
                .localEndpoint(localEndpoint)
                .remoteEndpoint(remoteEndpoint)
                .shared(record.getValue(SHARED) == 1)
                .debug(record.getValue(DEBUG) == 1);

        Map<String, Long> annotations = record.getValue(ANNOTATIONS);
        annotations.forEach((annotation, timestamp) -> spanBuilder.addAnnotation(timestamp, annotation));

        Map<String, String> tags = record.getValue(TAGS);
        tags.forEach(spanBuilder::putTag);

        return spanBuilder.build();
    }

    public static Endpoint buildEndpoint(Record record,
                                         Field<String> serviceNameField,
                                         Field<String> ipv4Field,
                                         Field<String> ipv6Field,
                                         Field<Integer> portField) {
        return Endpoint.newBuilder()
                .serviceName(record.getValue(serviceNameField))
                .ip(record.getValue(ipv4Field))
                .ip(record.getValue(ipv6Field))
                .port(record.getValue(portField))
                .build();
    }

    public static short convertBooleanToShort(@Nullable Boolean flag) {
        return (short) (flag == TRUE ? 1 : 0);
    }
}
