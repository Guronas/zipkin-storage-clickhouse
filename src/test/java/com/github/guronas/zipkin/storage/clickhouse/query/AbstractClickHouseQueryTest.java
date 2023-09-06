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

import org.jooq.DSLContext;
import org.jooq.tools.jdbc.MockDataProvider;
import org.jooq.tools.jdbc.MockFileDatabase;

import java.io.IOException;
import java.io.InputStream;

import static com.github.guronas.zipkin.storage.clickhouse.query.TestJooqUtils.getTestDSLContext;

public abstract class AbstractClickHouseQueryTest {
    protected final DSLContext dslContext;

    public AbstractClickHouseQueryTest() {
        this(null);
    }

    public AbstractClickHouseQueryTest(MockDataProvider mockDataProvider) {
        try {
            if (mockDataProvider == null) {
                InputStream is = getClass().getClassLoader().getResourceAsStream("ClickHouseTestDB");
                mockDataProvider = new MockFileDatabase(is);
            }
            dslContext = getTestDSLContext(mockDataProvider);
        } catch (IOException e) {
            throw new RuntimeException("Failed to run test because problem with test ClickHouse DB occurred", e);
        }
    }
}
