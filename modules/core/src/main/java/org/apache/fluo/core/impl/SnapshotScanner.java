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
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import com.google.common.collect.ImmutableSet;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.fluo.accumulo.iterators.SnapshotIterator;
import org.apache.fluo.accumulo.util.ColumnType;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.data.Span;
import org.apache.fluo.core.util.ByteUtil;
import org.apache.fluo.core.util.SpanUtil;
import org.apache.fluo.core.util.UtilWaitThread;

/**
 * Allows users to iterate over entries of a {@link org.apache.fluo.api.client.Snapshot}
 */
public class SnapshotScanner implements Iterable<Entry<Key, Value>> {

  /**
   * Immutable options for a SnapshotScanner
   */
  public static final class Opts {
    private final Span span;
    private final Collection<Column> columns;
    private final boolean showReadLocks;

    public Opts(Span span, Collection<Column> columns, boolean showReadLocks) {
      this.span = span;
      this.columns = ImmutableSet.copyOf(columns);
      this.showReadLocks = showReadLocks;
    }

    public Span getSpan() {
      return span;
    }

    public Collection<Column> getColumns() {
      return columns;
    }

    public boolean getShowReadLocks() {
      return showReadLocks;
    }
  }

  private final long startTs;
  private final Environment env;
  private final TxStats stats;
  private final Opts config;

  static final long INITIAL_WAIT_TIME = 50;
  // TODO make configurable
  static final long MAX_WAIT_TIME = 60000;



  static void setupScanner(ScannerBase scanner, Collection<Column> columns, long startTs,
      boolean showReadLocks) {
    for (Column col : columns) {
      if (col.isQualifierSet()) {
        scanner.fetchColumn(ByteUtil.toText(col.getFamily()), ByteUtil.toText(col.getQualifier()));
      } else {
        scanner.fetchColumnFamily(ByteUtil.toText(col.getFamily()));
      }
    }

    IteratorSetting iterConf = new IteratorSetting(10, SnapshotIterator.class);
    SnapshotIterator.setSnaptime(iterConf, startTs);
    SnapshotIterator.setReturnReadLockPresent(iterConf, showReadLocks);
    scanner.addScanIterator(iterConf);
  }

  private class SnapIter implements Iterator<Entry<Key, Value>> {

    private Iterator<Entry<Key, Value>> iterator;
    private Entry<Key, Value> next;
    private Opts snapIterConfig;

    SnapIter(Opts config) {
      this.snapIterConfig = config;
      setUpIterator();
    }

    private void setUpIterator() {
      Scanner scanner;
      try {
        scanner = env.getAccumuloClient().createScanner(env.getTable(), env.getAuthorizations());
      } catch (TableNotFoundException e) {
        throw new RuntimeException(e);
      }
      scanner.clearColumns();
      scanner.clearScanIterators();
      scanner.setRange(SpanUtil.toRange(snapIterConfig.getSpan()));

      setupScanner(scanner, snapIterConfig.getColumns(), startTs, snapIterConfig.showReadLocks);

      this.iterator = scanner.iterator();
    }

    @Override
    public boolean hasNext() {
      if (next == null) {
        next = getNext();
      }

      return next != null;
    }

    @Override
    public Entry<Key, Value> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      Entry<Key, Value> tmp = next;
      next = null;
      return tmp;
    }

    private void resetScanner(Span span) {
      snapIterConfig = new Opts(span, snapIterConfig.columns, snapIterConfig.showReadLocks);
      setUpIterator();
    }

    public void resolveLock(Entry<Key, Value> lockEntry) {

      // read ahead a little bit looking for other locks to resolve

      long startTime = System.currentTimeMillis();
      long waitTime = INITIAL_WAIT_TIME;

      List<Entry<Key, Value>> locks = new ArrayList<>();
      locks.add(lockEntry);
      int amountRead = 0;
      int numRead = 0;

      RowColumn origEnd = snapIterConfig.getSpan().getEnd();
      boolean isEndInclusive = snapIterConfig.getSpan().isEndInclusive();

      while (true) {
        while (iterator.hasNext()) {
          Entry<Key, Value> entry = iterator.next();

          if (ColumnType.from(entry.getKey()) == ColumnType.LOCK) {
            locks.add(entry);
          }

          amountRead += entry.getKey().getSize() + entry.getValue().getSize();
          numRead++;

          if (numRead > 100 || amountRead > 1 << 12) {
            break;
          }
        }

        boolean resolvedLocks = LockResolver.resolveLocks(env, startTs, stats, locks, startTime);

        if (!resolvedLocks) {
          UtilWaitThread.sleep(waitTime);
          stats.incrementLockWaitTime(waitTime);
          waitTime = Math.min(MAX_WAIT_TIME, waitTime * 2);

          RowColumn start = SpanUtil.toRowColumn(locks.get(0).getKey());
          RowColumn end = SpanUtil.toRowColumn(locks.get(locks.size() - 1).getKey()).following();

          resetScanner(new Span(start, true, end, false));

          locks.clear();

        } else {
          break;
        }
      }

      RowColumn start = SpanUtil.toRowColumn(lockEntry.getKey());

      resetScanner(new Span(start, true, origEnd, isEndInclusive));
    }

    public Entry<Key, Value> getNext() {
      mloop: while (true) {
        // its possible a next could exist then be rolled back
        if (!iterator.hasNext()) {
          return null;
        }

        Entry<Key, Value> entry = iterator.next();

        ColumnType colType = ColumnType.from(entry.getKey());

        switch (colType) {
          case LOCK:
            resolveLock(entry);
            continue mloop;
          case DATA:
            stats.incrementEntriesReturned(1);
            return entry;
          case RLOCK:
            return entry;
          default:
            throw new IllegalArgumentException("Unexpected column type " + colType);
        }
      }
    }

    @Override
    public void remove() {
      iterator.remove();
    }
  }

  SnapshotScanner(Environment env, Opts config, long startTs, TxStats stats) {
    this.env = env;
    this.config = config;
    this.startTs = startTs;
    this.stats = stats;
  }

  @Override
  public Iterator<Entry<Key, Value>> iterator() {
    return new SnapIter(config);
  }
}
