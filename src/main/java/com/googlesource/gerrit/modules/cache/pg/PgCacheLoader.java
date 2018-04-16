// Copyright (C) 2018 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.googlesource.gerrit.modules.cache.pg;

import com.google.common.cache.CacheLoader;
import com.google.gerrit.common.TimeUtil;
import java.util.concurrent.Executor;

public class PgCacheLoader<K, V> extends CacheLoader<K, ValueHolder<V>> {
  private final Executor executor;
  private final PgSqlStore<K, V> store;
  private final CacheLoader<K, V> loader;

  PgCacheLoader(Executor executor, PgSqlStore<K, V> store, CacheLoader<K, V> loader) {
    this.executor = executor;
    this.store = store;
    this.loader = loader;
  }

  @Override
  public ValueHolder<V> load(K key) throws Exception {
    if (store.mightContain(key)) {
      ValueHolder<V> h = store.getIfPresent(key);
      if (h != null) {
        return h;
      }
    }

    final ValueHolder<V> h = new ValueHolder<>(loader.load(key));
    h.created = TimeUtil.nowMs();
    executor.execute(() -> store.put(key, h));
    return h;
  }
}
