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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.gerrit.server.config.ConfigUtil;
import com.google.gerrit.server.config.GerritServerConfig;
import com.google.gerrit.server.config.ThreadSettingsConfig;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.apache.commons.dbcp.BasicDataSource;
import org.eclipse.jgit.lib.Config;

@Singleton
public class PgSqlSource {
  private static final String CACHE = "cache";

  private final DataSource ds;

  @Inject
  PgSqlSource(@GerritServerConfig Config cfg, ThreadSettingsConfig threadSettingsConfig) {
    this.ds = createDs(cfg, threadSettingsConfig);
  }

  Connection getConnection() throws SQLException {
    return ds.getConnection();
  }

  private static final DataSource createDs(Config cfg, ThreadSettingsConfig threadSettingsConfig) {
    BasicDataSource ds = new BasicDataSource();
    ds.setUrl(cfg.getString(CACHE, null, "url"));
    int poolLimit = threadSettingsConfig.getDatabasePoolLimit();
    ds.setMaxActive(cfg.getInt(CACHE, "poolLimit", poolLimit));
    ds.setDriverClassName("org.postgresql.Driver");
    ds.setMinIdle(cfg.getInt(CACHE, "poolminidle", 4));
    ds.setMaxIdle(cfg.getInt(CACHE, "poolmaxidle", Math.min(poolLimit, 16)));
    ds.setInitialSize(ds.getMinIdle());
    ds.setMaxWait(
        ConfigUtil.getTimeUnit(
            cfg, CACHE, null, "poolmaxwait", MILLISECONDS.convert(30, SECONDS), MILLISECONDS));
    long evictIdleTimeMs = 1000L * 60;
    ds.setMinEvictableIdleTimeMillis(evictIdleTimeMs);
    ds.setTimeBetweenEvictionRunsMillis(evictIdleTimeMs / 2);
    return ds;
  }
}
