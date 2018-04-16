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
import com.google.common.hash.BloomFilter;
import com.google.gerrit.common.TimeUtil;
import com.google.gerrit.server.cache.PersistentCache.DiskStats;
import com.google.inject.TypeLiteral;
import java.io.InvalidClassException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PgSqlStore<K, V> {
  static final Logger log = LoggerFactory.getLogger(PgSqlStore.class);

  private final PgSqlSource source;
  private final String name;
  private final KeyType<K> keyType;
  private final long maxSize;
  private final long expireAfterWrite;
  private final BlockingQueue<PgSqlHandle> handles;
  private final AtomicLong hitCount = new AtomicLong();
  private final AtomicLong missCount = new AtomicLong();
  private volatile BloomFilter<K> bloomFilter;
  private int estimatedSize;

  private final String qCount;
  private final String qKeys;
  private final String qValue;
  private final String qTouch;
  private final String qPut;
  private final String qInvalidateKey;
  private final String qInvalidateAll;
  private final String qSum;
  private final String qOrderByAccessed;
  private final String qStats;

  PgSqlStore(
      PgSqlSource source,
      String name,
      TypeLiteral<K> keyType,
      long maxSize,
      long expireAfterWrite) {
    this.source = source;
    this.name = name;
    this.keyType = KeyType.create(keyType);
    this.maxSize = maxSize;
    this.expireAfterWrite = expireAfterWrite;

    int cores = Runtime.getRuntime().availableProcessors();
    int keep = Math.min(cores, 16);
    this.handles = new ArrayBlockingQueue<>(keep);

    // initiate all query strings
    this.qCount = "SELECT COUNT(*) FROM \"data_" + this.name + "\"";
    this.qKeys = "SELECT k FROM \"data_" + this.name + "\"";
    this.qValue = "SELECT v, created FROM \"data_" + this.name + "\" WHERE k=?";
    this.qTouch = "UPDATE \"data_" + this.name + "\" SET accessed=? WHERE k=?";
    this.qPut =
        "INSERT INTO \"data_"
            + this.name
            + "\" (k, v, created, accessed) VALUES(?,?,?,?) "
            + "ON CONFLICT (k) DO UPDATE "
            + "SET v = EXCLUDED.v, created = EXCLUDED.created, accessed = EXCLUDED.accessed";
    this.qInvalidateKey = "DELETE FROM \"data_" + this.name + "\" WHERE k=?";
    this.qInvalidateAll = "DELETE FROM \"data_" + this.name + "\"";
    this.qSum = "SELECT SUM(space) FROM \"data_" + this.name + "\"";
    this.qOrderByAccessed =
        "SELECT k,space,created FROM \"data_" + this.name + "\" ORDER BY accessed";
    this.qStats = "SELECT COUNT(*),SUM(space) FROM \"data_" + this.name + "\"";
  }

  synchronized void open() {
    if (bloomFilter == null) {
      bloomFilter = buildBloomFilter();
    }
  }

  void close() {
    PgSqlHandle h;
    while ((h = handles.poll()) != null) {
      h.close();
    }
  }

  boolean mightContain(K key) {
    BloomFilter<K> b = bloomFilter;
    if (b == null) {
      synchronized (this) {
        b = bloomFilter;
        if (b == null) {
          b = buildBloomFilter();
          bloomFilter = b;
        }
      }
    }
    return b == null || b.mightContain(key);
  }

  private BloomFilter<K> buildBloomFilter() {
    PgSqlHandle c = null;
    try {
      c = acquire();
      try (Statement s = c.conn.createStatement()) {
        if (estimatedSize <= 0) {
          try (ResultSet r = s.executeQuery(qCount)) {
            estimatedSize = r.next() ? r.getInt(1) : 0;
          }
        }

        BloomFilter<K> b = newBloomFilter();
        try (ResultSet r = s.executeQuery(qKeys)) {
          while (r.next()) {
            b.put(keyType.get(r, 1));
          }
        } catch (SQLException e) {
          if (e.getCause() instanceof InvalidClassException) {
            log.warn(
                "Entries cached for "
                    + name
                    + " have an incompatible class and can't be deserialized. "
                    + "Cache is flushed.");
            invalidateAll();
          } else {
            throw e;
          }
        }
        return b;
      }
    } catch (SQLException e) {
      log.warn("Cannot build BloomFilter for " + name + ": " + e.getMessage());
      c = close(c);
      return null;
    } finally {
      release(c);
    }
  }

  ValueHolder<V> getIfPresent(K key) {
    PgSqlHandle c = null;
    try {
      c = acquire();
      if (c.get == null) {
        c.get = c.conn.prepareStatement(qValue);
      }
      keyType.set(c.get, 1, key);
      try (ResultSet r = c.get.executeQuery()) {
        if (!r.next()) {
          missCount.incrementAndGet();
          return null;
        }

        Timestamp created = r.getTimestamp(2);
        if (expired(created)) {
          invalidate(key);
          missCount.incrementAndGet();
          return null;
        }

        @SuppressWarnings("unchecked")
        V val = (V) PgSqlHandle.deserialize(r.getBytes(1));
        ValueHolder<V> h = new ValueHolder<>(val);
        h.clean = true;
        hitCount.incrementAndGet();
        touch(c, key);
        return h;
      } finally {
        c.get.clearParameters();
      }
    } catch (SQLException e) {
      log.warn("Cannot read cache " + name + " for " + key, e);
      c = close(c);
      return null;
    } finally {
      release(c);
    }
  }

  private boolean expired(Timestamp created) {
    if (expireAfterWrite == 0) {
      return false;
    }
    long age = TimeUtil.nowMs() - created.getTime();
    return 1000 * expireAfterWrite < age;
  }

  private void touch(PgSqlHandle c, K key) throws SQLException {
    if (c.touch == null) {
      c.touch = c.conn.prepareStatement(qTouch);
    }
    try {
      c.touch.setTimestamp(1, TimeUtil.nowTs());
      keyType.set(c.touch, 2, key);
      c.touch.executeUpdate();
    } finally {
      c.touch.clearParameters();
    }
  }

  void put(K key, ValueHolder<V> holder) {
    if (holder.clean) {
      return;
    }

    BloomFilter<K> b = bloomFilter;
    if (b != null) {
      b.put(key);
      bloomFilter = b;
    }

    PgSqlHandle c = null;
    try {
      c = acquire();
      if (c.put == null) {
        c.put = c.conn.prepareStatement(qPut);
      }
      try {
        keyType.set(c.put, 1, key);
        c.put.setObject(2, PgSqlHandle.serialize(holder.value), Types.BINARY);
        c.put.setTimestamp(3, new Timestamp(holder.created));
        c.put.setTimestamp(4, TimeUtil.nowTs());
        c.put.executeUpdate();
        holder.clean = true;
      } finally {
        c.put.clearParameters();
      }
    } catch (SQLException e) {
      log.warn("Cannot put into cache " + name, e);
      c = close(c);
    } finally {
      release(c);
    }
  }

  void invalidate(K key) {
    PgSqlHandle c = null;
    try {
      c = acquire();
      invalidate(c, key);
    } catch (SQLException e) {
      log.warn("Cannot invalidate cache " + name, e);
      c = close(c);
    } finally {
      release(c);
    }
  }

  private void invalidate(PgSqlHandle c, K key) throws SQLException {
    if (c.invalidate == null) {
      c.invalidate = c.conn.prepareStatement(qInvalidateKey);
    }
    try {
      keyType.set(c.invalidate, 1, key);
      c.invalidate.executeUpdate();
    } finally {
      c.invalidate.clearParameters();
    }
  }

  void invalidateAll() {
    PgSqlHandle c = null;
    try {
      c = acquire();
      try (Statement s = c.conn.createStatement()) {
        s.executeUpdate(qInvalidateAll);
      }
      bloomFilter = newBloomFilter();
    } catch (SQLException e) {
      log.warn("Cannot invalidate cache " + name, e);
      c = close(c);
    } finally {
      release(c);
    }
  }

  void prune(Cache<K, ?> mem) {
    PgSqlHandle c = null;
    try {
      c = acquire();
      try (Statement s = c.conn.createStatement()) {
        long used = 0;
        try (ResultSet r = s.executeQuery(qSum)) {
          used = r.next() ? r.getLong(1) : 0;
        }
        if (used <= maxSize) {
          return;
        }

        try (ResultSet r = s.executeQuery(qOrderByAccessed)) {
          while (maxSize < used && r.next()) {
            K key = keyType.get(r, 1);
            Timestamp created = r.getTimestamp(3);
            if (mem.getIfPresent(key) != null && !expired(created)) {
              touch(c, key);
            } else {
              invalidate(c, key);
              used -= r.getLong(2);
            }
          }
        }
      }
    } catch (SQLException e) {
      log.warn("Cannot prune cache " + name, e);
      c = close(c);
    } finally {
      release(c);
    }
  }

  DiskStats diskStats() {
    long size = 0;
    long space = 0;
    PgSqlHandle c = null;
    try {
      c = acquire();
      try (Statement s = c.conn.createStatement();
          ResultSet r = s.executeQuery(qStats)) {
        if (r.next()) {
          size = r.getLong(1);
          space = r.getLong(2);
        }
      }
    } catch (SQLException e) {
      log.warn("Cannot get DiskStats for " + name, e);
      c = close(c);
    } finally {
      release(c);
    }
    return new DiskStats(size, space, hitCount.get(), missCount.get());
  }

  private PgSqlHandle acquire() throws SQLException {
    PgSqlHandle h = handles.poll();
    return h != null ? h : new PgSqlHandle(source, name, keyType);
  }

  private void release(PgSqlHandle h) {
    if (h != null && !handles.offer(h)) {
      h.close();
    }
  }

  private PgSqlHandle close(PgSqlHandle h) {
    if (h != null) {
      h.close();
    }
    return null;
  }

  private BloomFilter<K> newBloomFilter() {
    int cnt = Math.max(64 * 1024, 2 * estimatedSize);
    return BloomFilter.create(keyType.funnel(), cnt);
  }
}
