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

import com.github.guronas.zipkin.storage.clickhouse.query.ZipkinSpans;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import zipkin2.Call;
import zipkin2.CheckResult;
import zipkin2.storage.*;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.List;

@Slf4j
public class ClickHouseStorage extends StorageComponent {
    private final ThreadPoolTaskExecutor clickHouseExecutor;
    private final DSLContext dslContext;
    private final List<String> autocompleteKeys;

    public ClickHouseStorage(ThreadPoolTaskExecutor clickHouseExecutor,
                             DataSource dataSource,
                             List<String> autocompleteKeys) {
        this.clickHouseExecutor = clickHouseExecutor;
        this.autocompleteKeys = autocompleteKeys;

        //We pretend here that ClickHouse is just MySQL DB
        //because jooq doesn't support ClickHouse dialect but also ClickHouse has quite the same syntax in general
        this.dslContext = DSL.using(dataSource, SQLDialect.MYSQL);
    }

    @Override
    public SpanStore spanStore() {
        return new ClickHouseSpanStore(clickHouseExecutor, dslContext, false);
    }

    @Override
    public SpanConsumer spanConsumer() {
        return new ClickhouseSpanConsumer(clickHouseExecutor, dslContext);
    }

    /**
     * Method for compatibility
     */
    @Override
    public Traces traces() {
        return (Traces) spanStore();
    }

    @Override
    public AutocompleteTags autocompleteTags() {
        return new ClickHouseAutocompleteTags(clickHouseExecutor, dslContext, autocompleteKeys);
    }

    /**
     * Method for compatibility
     */
    @Override
    public ServiceAndSpanNames serviceAndSpanNames() {
        return (ServiceAndSpanNames) spanStore();
    }

    @Override
    public CheckResult check() {
        try {
            log.trace("Checking connection with Clickhouse...");
            dslContext.selectFrom(ZipkinSpans.ZIPKIN_SPANS_TABLE).limit(1);
            log.trace("Connection has been checked successfully");
        } catch (Exception e) {
            log.error("Failed to check connection with Clickhouse", e);
            Call.propagateIfFatal(e);
            return CheckResult.failed(e);
        }
        return CheckResult.OK;
    }

    @Override
    public void close() throws IOException {
        clickHouseExecutor.shutdown();
        super.close();
    }
}
