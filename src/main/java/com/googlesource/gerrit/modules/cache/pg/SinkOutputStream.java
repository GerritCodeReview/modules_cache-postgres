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

import com.google.common.hash.PrimitiveSink;
import java.io.OutputStream;

class SinkOutputStream extends OutputStream {
  private final PrimitiveSink sink;

  SinkOutputStream(PrimitiveSink sink) {
    this.sink = sink;
  }

  @Override
  public void write(int b) {
    sink.putByte((byte) b);
  }

  @Override
  public void write(byte[] b, int p, int n) {
    sink.putBytes(b, p, n);
  }
}
