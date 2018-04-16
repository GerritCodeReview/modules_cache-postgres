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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PgSqlHandle {
  private static final Logger log = LoggerFactory.getLogger(PgSqlHandle.class);

  Connection conn;
  PreparedStatement get;
  PreparedStatement put;
  PreparedStatement touch;
  PreparedStatement invalidate;

  private final String name;

  PgSqlHandle(PgSqlSource store, String name, KeyType<?> type) throws SQLException {
    this.name = name;
    this.conn = store.getConnection();
    try (Statement stmt = conn.createStatement()) {
      stmt.addBatch(
          "CREATE TABLE IF NOT EXISTS \"data_"
              + this.name
              + "\" (k "
              + type.columnType()
              + " NOT NULL PRIMARY KEY" // H2 uses HASH however it is discouraged in Postgres doc
              + ",v BYTEA NOT NULL"
              + ",created TIMESTAMP NOT NULL"
              + ",accessed TIMESTAMP NOT NULL"
              + ",space BIGINT"
              + ")");
      // Postgres has no 'computed columns' concept, however one can achieve that
      // with trigger
      stmt.addBatch(
          "CREATE OR REPLACE FUNCTION compute_space()\n"
              + "RETURNS trigger\n"
              + "LANGUAGE plpgsql\n"
              + "SECURITY DEFINER\n"
              + "AS $BODY$\n"
              + "BEGIN\n"
              + "    NEW.space = OCTET_LENGTH(NEW.k) + OCTET_LENGTH(NEW.v);\n"
              + "    RETURN NEW;\n"
              + "END\n"
              + "$BODY$;");
      stmt.addBatch("DROP TRIGGER IF EXISTS computed_space ON \"data_" + this.name + "\"");
      stmt.addBatch(
          "CREATE TRIGGER computed_space "
              + "BEFORE INSERT OR UPDATE "
              + "ON \"data_"
              + this.name
              + "\" "
              + "FOR EACH ROW "
              + "EXECUTE PROCEDURE compute_space()");
      stmt.executeBatch();
    }
  }

  void close() {
    get = closeStatement(get);
    put = closeStatement(put);
    touch = closeStatement(touch);
    invalidate = closeStatement(invalidate);

    if (conn != null) {
      try {
        conn.close();
      } catch (SQLException e) {
        log.warn("Cannot close connection to " + name, e);
      } finally {
        conn = null;
      }
    }
  }

  private PreparedStatement closeStatement(PreparedStatement ps) {
    if (ps != null) {
      try {
        ps.close();
      } catch (SQLException e) {
        log.warn("Cannot close statement for " + name, e);
      }
    }
    return null;
  }

  static Object deserialize(byte[] d) {
    try {
      ByteArrayInputStream in = new ByteArrayInputStream(d);
      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      ObjectInputStream is =
          new ObjectInputStream(in) {
            @Override
            protected Class<?> resolveClass(ObjectStreamClass desc)
                throws IOException, ClassNotFoundException {
              try {
                return Class.forName(desc.getName(), true, loader);
              } catch (ClassNotFoundException e) {
                return super.resolveClass(desc);
              }
            }
          };
      return is.readObject();
    } catch (Throwable e) {
      log.error("Object deserialization failed", e);
      throw new RuntimeException(e);
    }
  }

  static byte[] serialize(Object o) {
    try (ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(out)) {
      os.writeObject(o);
      return out.toByteArray();
    } catch (Throwable e) {
      log.error("Object serialization failed {}", o, e);
      throw new RuntimeException(e);
    }
  }
}
