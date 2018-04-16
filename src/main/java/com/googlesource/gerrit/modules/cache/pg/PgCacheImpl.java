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

import com.google.common.cache.AbstractLoadingCache;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import com.google.gerrit.common.TimeUtil;
import com.google.gerrit.server.cache.PersistentCache;
import com.google.inject.TypeLiteral;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PgCacheImpl<K, V> extends AbstractLoadingCache<K, V> implements PersistentCache {
  private static final Logger log = LoggerFactory.getLogger(PgCacheImpl.class);

  private final String name;
  private final Executor executor;
  private final PgSqlStore<K, V> store;
  private final TypeLiteral<K> keyType;
  private final Cache<K, ValueHolder<V>> mem;

  public PgCacheImpl(
      String name,
      Executor executor,
      PgSqlStore<K, V> store,
      TypeLiteral<K> keyType,
      Cache<K, ValueHolder<V>> mem) {
    this.name = name;
    this.executor = executor;
    this.store = store;
    this.keyType = keyType;
    this.mem = mem;
  }

  @Override
  public V get(K key) throws ExecutionException {
    if (mem instanceof LoadingCache) {
      return ((LoadingCache<K, ValueHolder<V>>) mem).get(key).value;
    }

    log.error("Memory cache for persistent backend {} should be of LoadingCache type", name);
    throw new UnsupportedOperationException();
  }

  @Override
  public V getIfPresent(Object objKey) {
    if (!keyType.getRawType().isInstance(objKey)) {
      log.warn("Invalid object key type [{}] was provided for cache {}", objKey, name);
      return null;
    }

    @SuppressWarnings("unchecked")
    K key = (K) objKey;

    ValueHolder<V> h = mem.getIfPresent(key);
    if (h != null) {
      return h.value;
    }

    if (store.mightContain(key)) {
      h = store.getIfPresent(key);
      if (h != null) {
        mem.put(key, h);
        return h.value;
      }
    }
    return null;
  }

  @Override
  public V get(K key, Callable<? extends V> valueLoader) throws ExecutionException {
    return mem.get(
            key,
            () -> {
              if (store.mightContain(key)) {
                ValueHolder<V> h = store.getIfPresent(key);
                if (h != null) {
                  return h;
                }
              }

              ValueHolder<V> h = new ValueHolder<>(valueLoader.call());
              h.created = TimeUtil.nowMs();
              executor.execute(() -> store.put(key, h));
              return h;
            })
        .value;
  }

  @Override
  public void put(K key, V val) {
    final ValueHolder<V> h = new ValueHolder<>(val);
    h.created = TimeUtil.nowMs();
    mem.put(key, h);
    executor.execute(() -> store.put(key, h));
  }

  @SuppressWarnings("unchecked")
  @Override
  public void invalidate(Object key) {
    if (keyType.getRawType().isInstance(key) && store.mightContain((K) key)) {
      executor.execute(() -> store.invalidate((K) key));
    }
    mem.invalidate(key);
  }

  @Override
  public void invalidateAll() {
    store.invalidateAll();
    mem.invalidateAll();
  }

  @Override
  public long size() {
    return mem.size();
  }

  @Override
  public CacheStats stats() {
    return mem.stats();
  }

  @Override
  public DiskStats diskStats() {
    return store.diskStats();
  }

  void start() {
    store.open();
  }

  void stop() {
    for (Map.Entry<K, ValueHolder<V>> e : mem.asMap().entrySet()) {
      ValueHolder<V> h = e.getValue();
      if (!h.clean) {
        store.put(e.getKey(), h);
      }
    }
    store.close();
  }

  void prune(ScheduledExecutorService service) {
    store.prune(mem);

    long delay =
        LocalDateTime.now()
            .with(TemporalAdjusters.firstDayOfMonth())
            .withHour(01)
            .truncatedTo(ChronoUnit.HOURS)
            .toInstant(OffsetDateTime.now().getOffset())
            .minusMillis(TimeUtil.nowMs())
            .toEpochMilli();
    @SuppressWarnings("unused")
    Future<?> possiblyIgnoredError =
        service.schedule(() -> prune(service), delay, TimeUnit.MILLISECONDS);
  }
}
