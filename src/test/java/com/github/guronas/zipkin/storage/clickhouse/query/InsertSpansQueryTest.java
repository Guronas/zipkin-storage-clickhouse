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

import org.jooq.tools.jdbc.MockDataProvider;
import org.jooq.tools.jdbc.MockResult;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static com.github.guronas.zipkin.storage.clickhouse.query.SpansMockDataProvider.TEST_SPANS;

public class InsertSpansQueryTest extends AbstractClickHouseQueryTest {
    private static final MockDataProvider dataProvider = spy(new SpansMockDataProvider());

    public InsertSpansQueryTest() {
        super(dataProvider);
    }

    @Test
    public void testInsertSpansQuery() throws SQLException {
        doAnswer(invocation -> {
            MockResult[] queryResult = (MockResult[]) invocation.callRealMethod();
            assertEquals(1, queryResult.length);
            assertEquals(TEST_SPANS.values().size(), queryResult[0].rows);
            return queryResult;
        }).when(dataProvider).execute(any());
        InsertSpansQuery query = new InsertSpansQuery(dslContext, TEST_SPANS.values());
        query.get();
    }
}
