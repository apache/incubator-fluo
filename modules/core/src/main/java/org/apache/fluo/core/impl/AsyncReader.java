/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.fluo.core.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.Collections2;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;

public class AsyncReader {
  private BlockingQueue<AsyncGet> asyncGetsQueue;
  private ExecutorService executorService;
  private TransactionImpl tx;

  public AsyncReader(TransactionImpl tx) {
    this.tx = tx;
    asyncGetsQueue = new LinkedBlockingQueue<>();
    executorService = Executors.newSingleThreadExecutor();
  }

  public CompletableFuture<Bytes> get(Bytes row, Column column) {
    return get(row, column, null);
  }

  public CompletableFuture<Bytes> get(Bytes row, Column column, Bytes defaultValue) {
    AsyncGet curAsyncGet = new AsyncGet(row, column, defaultValue);
    asyncGetsQueue.add(curAsyncGet);

    executorService.submit(() -> {
      List<AsyncGet> getsList = new ArrayList<>();
      asyncGetsQueue.drainTo(getsList);

      try {
        Collection<RowColumn> rowColumns = Collections2.transform(getsList, ag -> ag.rc);

        Map<RowColumn, Bytes> getsMap = tx.get(rowColumns);

        for (AsyncGet asyncGet : getsList) {
          Bytes result = getsMap.get(asyncGet.rc);
          asyncGet.res.complete(result == null ? asyncGet.defaultValue : result);
        }
      } catch (Exception e) {
        for (AsyncGet asyncGet : getsList) {
          asyncGet.res.completeExceptionally(e);
        }
      }
    });

    return curAsyncGet.res;
  }

  public CompletableFuture<String> gets(String row, Column column) {
    return gets(row, column, null);
  }

  public CompletableFuture<String> gets(String row, Column column, String defaultValue) {
    Bytes defaultValueBytes = defaultValue == null ? new Bytes() : Bytes.of(defaultValue);
    return get(Bytes.of(row), column, defaultValueBytes).thenApply(b -> b.toString());
  }

  public void close() {
    executorService.shutdown();
  }

  static class AsyncGet {
    RowColumn rc;
    CompletableFuture<Bytes> res;
    Bytes defaultValue;

    public AsyncGet(Bytes row, Column column, Bytes defaultValue) {
      rc = new RowColumn(row, column);
      res = new CompletableFuture<>();
      this.defaultValue = defaultValue;
    }
  }
}
