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

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import zipkin2.Call;
import zipkin2.Callback;

import java.util.function.Supplier;

@Slf4j
@AllArgsConstructor
public class ClickHouseCall<V> extends Call.Base<V> {
    private final ThreadPoolTaskExecutor executor;
    private final Supplier<V> querySupplier;

    @Override
    protected V doExecute() {
        try {
            log.trace("Executing call with query [{}]", querySupplier);
            return querySupplier.get();
        } catch (Exception e) {
            log.error("Failed to execute query [{}]", querySupplier, e);
            throw e;
        }
    }

    @Override
    protected void doEnqueue(Callback<V> callback) {
        log.trace("Trying to enqueue call with query [{}]", querySupplier);
        executor.execute(() -> {
            try {
                callback.onSuccess(doExecute());
            } catch (Exception e) {
                propagateIfFatal(e);
                callback.onError(e);
            }
        });
    }

    @Override
    public Call<V> clone() {
        return new ClickHouseCall<>(executor, querySupplier);
    }
}
