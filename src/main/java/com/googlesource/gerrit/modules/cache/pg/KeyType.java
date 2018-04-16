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

import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.google.common.hash.PrimitiveSink;
import com.google.inject.TypeLiteral;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

class KeyType<K> {
  String columnType() {
    return "BYTEA"; // H2 uses OTHER
  }

  @SuppressWarnings("unchecked")
  K get(ResultSet rs, int col) throws SQLException {
    return (K) PgSqlHandle.deserialize(rs.getBytes(col)); // H2 doesn't require deserialization
  }

  void set(PreparedStatement ps, int col, K value) throws SQLException {
    ps.setObject(
        col,
        PgSqlHandle.serialize(value),
        Types.BINARY); // H2 uses JAVA_OBJECT and doesn't require serialization
  }

  Funnel<K> funnel() {
    return new Funnel<K>() {
      private static final long serialVersionUID = 1L;

      @Override
      public void funnel(K from, PrimitiveSink into) {
        try (ObjectOutputStream ser = new ObjectOutputStream(new SinkOutputStream(into))) {
          ser.writeObject(from);
          ser.flush();
        } catch (IOException err) {
          throw new RuntimeException("Cannot hash as Serializable", err);
        }
      }
    };
  }

  @SuppressWarnings("unchecked")
  static <K> KeyType<K> create(TypeLiteral<K> type) {
    if (type.getRawType() == String.class) {
      return (KeyType<K>) STRING;
    }
    return (KeyType<K>) OTHER;
  }

  static final KeyType<?> OTHER = new KeyType<>();
  static final KeyType<String> STRING =
      new KeyType<String>() {
        @Override
        String columnType() {
          return "text";
        }

        @Override
        String get(ResultSet rs, int col) throws SQLException {
          return rs.getString(col);
        }

        @Override
        void set(PreparedStatement ps, int col, String value) throws SQLException {
          ps.setString(col, value);
        }

        @SuppressWarnings("unchecked")
        @Override
        Funnel<String> funnel() {
          Funnel<?> s = Funnels.unencodedCharsFunnel();
          return (Funnel<String>) s;
        }
      };
}
