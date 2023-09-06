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

import com.github.guronas.zipkin.storage.clickhouse.query.GetTagsQuery;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.StringUtils;
import zipkin2.Call;
import zipkin2.storage.AutocompleteTags;

import java.util.List;

import static org.springframework.util.Assert.isTrue;

@RequiredArgsConstructor
public class ClickHouseAutocompleteTags implements AutocompleteTags {
    private final ThreadPoolTaskExecutor executor;
    private final DSLContext dslContext;
    private final List<String> autocompleteKeys;

    @Override
    public Call<List<String>> getKeys() {
        return Call.create(autocompleteKeys);
    }

    @Override
    public Call<List<String>> getValues(String key) {
        isTrue(StringUtils.hasLength(key), "Key is empty");
        return new ClickHouseCall<>(executor, new GetTagsQuery(dslContext, key));
    }
}
