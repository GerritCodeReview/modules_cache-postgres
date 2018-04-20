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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gerrit.extensions.events.LifecycleListener;
import com.google.gerrit.extensions.registration.DynamicMap;
import com.google.gerrit.server.cache.CacheBinding;
import com.google.gerrit.server.cache.MemoryCacheFactory;
import com.google.gerrit.server.cache.PersistentCacheFactory;
import com.google.gerrit.server.config.GerritServerConfig;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.eclipse.jgit.lib.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class PgCacheFactory implements PersistentCacheFactory, LifecycleListener {
  private static final Logger log = LoggerFactory.getLogger(PgCacheFactory.class);

  private final PgSqlSource ds;
  private final Config cfg;
  private final MemoryCacheFactory memFactory;
  private final List<PgCacheImpl<?, ?>> caches;
  private final DynamicMap<Cache<?, ?>> cacheMap;
  private final ExecutorService executor;
  private final ScheduledExecutorService cleanup;

  @Inject
  PgCacheFactory(
      PgSqlSource ds,
      @GerritServerConfig Config cfg,
      MemoryCacheFactory memFactory,
      DynamicMap<Cache<?, ?>> cacheMap) {
    this.ds = ds;
    this.cfg = cfg;
    this.memFactory = memFactory;
    this.caches = new LinkedList<>();
    this.cacheMap = cacheMap;

    executor =
        Executors.newFixedThreadPool(
            1, new ThreadFactoryBuilder().setNameFormat("PgCache-Store-%d").build());
    cleanup =
        Executors.newScheduledThreadPool(
            1,
            new ThreadFactoryBuilder().setNameFormat("PgCache-Prune-%d").setDaemon(true).build());
  }

  @Override
  public void start() {
    if (executor != null) {
      for (PgCacheImpl<?, ?> cache : caches) {
        @SuppressWarnings("unused")
        Future<?> possiblyIgnoredError =
            cleanup.schedule(() -> cache.prune(cleanup), 30, TimeUnit.SECONDS);
      }
    }
  }

  @Override
  public void stop() {
    if (executor != null) {
      try {
        cleanup.shutdownNow();

        List<Runnable> pending = executor.shutdownNow();
        if (executor.awaitTermination(15, TimeUnit.MINUTES)) {
          if (pending != null && !pending.isEmpty()) {
            log.info("Finishing {} disk cache updates", pending.size());
            for (Runnable update : pending) {
              update.run();
            }
          }
        } else {
          log.info("Timeout waiting for disk cache to close");
        }
      } catch (InterruptedException e) {
        log.warn("Interrupted waiting for disk cache to shutdown");
      }
    }
    synchronized (caches) {
      for (PgCacheImpl<?, ?> cache : caches) {
        cache.stop();
      }
    }
  }

  @SuppressWarnings({"unchecked"})
  @Override
  public <K, V> Cache<K, V> build(CacheBinding<K, V> in) {
    long limit = cfg.getLong("cache", in.name(), "diskLimit", in.diskLimit());

    if (limit <= 0) {
      return memFactory.build(in);
    }

    PgCacheBindingProxy<K, V> def = new PgCacheBindingProxy<>(in);
    PgSqlStore<K, V> store =
        newSqlStore(def.name(), def.keyType(), limit, def.expireAfterWrite(TimeUnit.SECONDS));
    PgCacheImpl<K, V> cache =
        new PgCacheImpl<>(
            def.name(),
            executor,
            store,
            def.keyType(),
            (Cache<K, ValueHolder<V>>) memFactory.build(def));
    synchronized (caches) {
      caches.add(cache);
    }
    return cache;
  }

  @SuppressWarnings({"unchecked"})
  @Override
  public <K, V> LoadingCache<K, V> build(CacheBinding<K, V> in, CacheLoader<K, V> loader) {
    long limit = cfg.getLong("cache", in.name(), "diskLimit", in.diskLimit());

    if (limit <= 0) {
      return memFactory.build(in, loader);
    }

    PgCacheBindingProxy<K, V> def = new PgCacheBindingProxy<>(in);
    PgSqlStore<K, V> store =
        newSqlStore(def.name(), def.keyType(), limit, def.expireAfterWrite(TimeUnit.SECONDS));
    Cache<K, ValueHolder<V>> mem =
        (Cache<K, ValueHolder<V>>)
            memFactory.build(def, (CacheLoader<K, V>) new PgCacheLoader<>(executor, store, loader));
    PgCacheImpl<K, V> cache = new PgCacheImpl<>(def.name(), executor, store, def.keyType(), mem);
    caches.add(cache);
    return cache;
  }

  @Override
  public void onStop(String plugin) {
    synchronized (caches) {
      for (Map.Entry<String, Provider<Cache<?, ?>>> entry : cacheMap.byPlugin(plugin).entrySet()) {
        Cache<?, ?> cache = entry.getValue().get();
        if (caches.remove(cache)) {
          ((PgCacheImpl<?, ?>) cache).stop();
        }
      }
    }
  }

  private <V, K> PgSqlStore<K, V> newSqlStore(
      String name, TypeLiteral<K> keyType, long maxSize, Long expireAfterWrite) {
    return new PgSqlStore<>(
        ds, name, keyType, maxSize, expireAfterWrite == null ? 0 : expireAfterWrite.longValue());
  }
}
