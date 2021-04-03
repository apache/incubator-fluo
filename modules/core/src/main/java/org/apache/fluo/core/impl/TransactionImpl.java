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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriter.Result;
import org.apache.accumulo.core.client.ConditionalWriter.Status;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.fluo.accumulo.iterators.PrewriteIterator;
import org.apache.fluo.accumulo.util.ColumnConstants;
import org.apache.fluo.accumulo.util.ColumnType;
import org.apache.fluo.accumulo.util.ReadLockUtil;
import org.apache.fluo.accumulo.values.DelLockValue;
import org.apache.fluo.accumulo.values.DelReadLockValue;
import org.apache.fluo.accumulo.values.LockValue;
import org.apache.fluo.accumulo.values.ReadLockValue;
import org.apache.fluo.api.client.AbstractTransactionBase;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.SnapshotBase;
import org.apache.fluo.api.client.scanner.ScannerBuilder;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.data.Span;
import org.apache.fluo.api.exceptions.AlreadySetException;
import org.apache.fluo.api.exceptions.CommitException;
import org.apache.fluo.api.exceptions.FluoException;
import org.apache.fluo.core.async.AsyncCommitObserver;
import org.apache.fluo.core.async.AsyncConditionalWriter;
import org.apache.fluo.core.async.AsyncTransaction;
import org.apache.fluo.core.async.SyncCommitObserver;
import org.apache.fluo.core.exceptions.AlreadyAcknowledgedException;
import org.apache.fluo.core.exceptions.StaleScanException;
import org.apache.fluo.core.impl.scanner.ScannerBuilderImpl;
import org.apache.fluo.core.oracle.Stamp;
import org.apache.fluo.core.util.ByteUtil;
import org.apache.fluo.core.util.ColumnUtil;
import org.apache.fluo.core.util.ConditionalFlutation;
import org.apache.fluo.core.util.FluoCondition;
import org.apache.fluo.core.util.Flutation;
import org.apache.fluo.core.util.Hex;
import org.apache.fluo.core.util.SpanUtil;
import org.apache.fluo.core.util.UtilWaitThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.fluo.api.observer.Observer.NotificationType.STRONG;
import static org.apache.fluo.api.observer.Observer.NotificationType.WEAK;

/**
 * Transaction implementation
 */
public class TransactionImpl extends AbstractTransactionBase implements AsyncTransaction, Snapshot {

  public static final byte[] EMPTY = new byte[0];
  public static final Bytes EMPTY_BS = Bytes.of(EMPTY);
  private static final Bytes DELETE =
      Bytes.of("special delete object f804266bf94935edd45ae3e6c287b93c1814295c");
  private static final Bytes NTFY_VAL =
      Bytes.of("special ntfy value ce0c523e6e4dc093be8a2736b82eca1b95f97ed4");
  private static final Bytes RLOCK_VAL =
      Bytes.of("special rlock value 94da84e7796ff3b23b779805d820a33f1997cb8b");

  // added to avoid findbugs false positive
  private static final Supplier<Void> NULLS = () -> null;

  private static boolean isWrite(Bytes val) {
    return val != NTFY_VAL && val != RLOCK_VAL;
  }

  private static boolean isDelete(Bytes val) {
    return val == DELETE;
  }

  private static boolean isReadLock(Bytes val) {
    return val == RLOCK_VAL;
  }

  private enum TxStatus {
    OPEN, COMMIT_STARTED, COMMITTED, CLOSED
  }

  private final long startTs;
  private final Map<Bytes, Map<Column, Bytes>> updates = new HashMap<>();
  private final Map<Bytes, Set<Column>> weakNotifications = new HashMap<>();
  private final Set<Column> observedColumns;
  private final Environment env;
  private final Map<Bytes, Set<Column>> columnsRead = new HashMap<>();
  // Tracks row columns that were observed to have had a read lock in the past.
  private final Map<Bytes, Set<Column>> readLocksSeen = new HashMap<>();
  private final TxStats stats;
  private Notification notification;
  private Notification weakNotification;
  private TransactorNode tnode = null;
  private TxStatus status = TxStatus.OPEN;
  private boolean commitAttempted = false;
  private AsyncReader asyncReader = null;


  public TransactionImpl(Environment env, Notification trigger, long startTs) {
    Objects.requireNonNull(env, "environment cannot be null");
    Preconditions.checkArgument(startTs >= 0, "startTs cannot be negative");
    this.env = env;
    this.stats = new TxStats(env);
    this.startTs = startTs;
    this.observedColumns = env.getConfiguredObservers().getObservedColumns(STRONG);

    if (trigger != null
        && env.getConfiguredObservers().getObservedColumns(WEAK).contains(trigger.getColumn())) {
      this.weakNotification = trigger;
    } else {
      this.notification = trigger;
    }

    if (notification != null) {
      Map<Column, Bytes> colUpdates = new HashMap<>();
      colUpdates.put(notification.getColumn(), NTFY_VAL);
      updates.put(notification.getRow(), colUpdates);
    }
  }

  public TransactionImpl(Environment env, Notification trigger) {
    this(env, trigger, allocateTimestamp(env).getTxTimestamp());
  }

  public TransactionImpl(Environment env) {
    this(env, null, allocateTimestamp(env).getTxTimestamp());
  }

  public TransactionImpl(Environment env, long startTs) {
    this(env, null, startTs);
  }

  private static Stamp allocateTimestamp(Environment env) {
    return env.getSharedResources().getTimestampTracker().allocateTimestamp();
  }

  @Override
  public Bytes get(Bytes row, Column column) {
    checkIfOpen();
    // TODO cache? precache?
    return get(row, Collections.singleton(column)).get(column);
  }

  @Override
  public Map<Column, Bytes> get(Bytes row, Set<Column> columns) {
    checkIfOpen();
    return getImpl(row, columns);
  }

  @Override
  public Map<Bytes, Map<Column, Bytes>> get(Collection<Bytes> rows, Set<Column> columns) {
    checkIfOpen();

    if (rows.isEmpty() || columns.isEmpty()) {
      return Collections.emptyMap();
    }

    env.getSharedResources().getVisCache().validate(columns);

    ParallelSnapshotScanner pss =
        new ParallelSnapshotScanner(rows, columns, env, startTs, stats, readLocksSeen, kve -> {
        });

    Map<Bytes, Map<Column, Bytes>> ret = pss.scan();

    for (Entry<Bytes, Map<Column, Bytes>> entry : ret.entrySet()) {
      updateColumnsRead(entry.getKey(), entry.getValue().keySet());
    }

    return ret;
  }

  @Override
  public Map<RowColumn, Bytes> get(Collection<RowColumn> rowColumns) {
    checkIfOpen();
    return getImpl(rowColumns, kve -> {
    });
  }

  private Map<Column, Bytes> getImpl(Bytes row, Set<Column> columns) {

    // TODO push visibility filtering to server side?

    env.getSharedResources().getVisCache().validate(columns);

    boolean shouldCopy = false;

    for (Column column : columns) {
      if (column.isVisibilitySet()) {
        shouldCopy = true;
      }
    }

    SnapshotScanner.Opts opts;
    if (shouldCopy) {
      HashSet<Column> cols = new HashSet<>();
      for (Column column : columns) {
        if (column.isVisibilitySet()) {
          cols.add(new Column(column.getFamily(), column.getQualifier()));
        } else {
          cols.add(column);
        }
      }
      opts = new SnapshotScanner.Opts(Span.exact(row), columns, true);
    } else {
      opts = new SnapshotScanner.Opts(Span.exact(row), columns, true);
    }

    Map<Column, Bytes> ret = new HashMap<>();
    Set<Column> readLockCols = null;

    for (Entry<Key, Value> kve : new SnapshotScanner(env, opts, startTs, stats)) {

      Column col = ColumnUtil.convert(kve.getKey());
      if (shouldCopy && !columns.contains(col)) {
        continue;
      }

      if (ColumnType.from(kve.getKey()) == ColumnType.RLOCK) {
        if (readLockCols == null) {
          readLockCols = readLocksSeen.computeIfAbsent(row, k -> new HashSet<>());
        }
        readLockCols.add(col);
      } else {
        ret.put(col, Bytes.of(kve.getValue().get()));
      }
    }

    // only update columns read after successful read
    updateColumnsRead(row, columns);

    return ret;
  }

  private Map<RowColumn, Bytes> getImpl(Collection<RowColumn> rowColumns,
      Consumer<Entry<Key, Value>> writeLocksSeen) {
    if (rowColumns.isEmpty()) {
      return Collections.emptyMap();
    }

    ParallelSnapshotScanner pss =
        new ParallelSnapshotScanner(rowColumns, env, startTs, stats, readLocksSeen, writeLocksSeen);

    Map<Bytes, Map<Column, Bytes>> scan = pss.scan();
    Map<RowColumn, Bytes> ret = new HashMap<>();

    for (Entry<Bytes, Map<Column, Bytes>> entry : scan.entrySet()) {
      updateColumnsRead(entry.getKey(), entry.getValue().keySet());
      for (Entry<Column, Bytes> colVal : entry.getValue().entrySet()) {
        ret.put(new RowColumn(entry.getKey(), colVal.getKey()), colVal.getValue());
      }
    }

    return ret;
  }

  @Override
  public CompletableFuture<Bytes> getAsync(Bytes row, Column column) {
    return getAsyncReader().get(row, column);
  }

  @Override
  public CompletableFuture<Bytes> getAsync(Bytes row, Column column, Bytes defaultValue) {
    return getAsyncReader().get(row, column, defaultValue);
  }

  @Override
  public CompletableFuture<String> getsAsync(String row, Column column) {
    return getAsyncReader().gets(row, column);
  }

  @Override
  public CompletableFuture<String> getsAsync(String row, Column column, String defaultValue) {
    return getAsyncReader().gets(row, column, defaultValue);
  }

  private AsyncReader getAsyncReader() {
    if (asyncReader == null) {
      asyncReader = new AsyncReader(this);
    }
    return asyncReader;
  }

  @Override
  public ScannerBuilder scanner() {
    checkIfOpen();
    return new ScannerBuilderImpl(this);
  }

  private void updateColumnsRead(Bytes row, Set<Column> columns) {
    Set<Column> colsRead = columnsRead.get(row);
    if (colsRead == null) {
      colsRead = new HashSet<>();
      columnsRead.put(row, colsRead);
    }
    colsRead.addAll(columns);
  }

  void setReadLock(Bytes row, Column col) {
    checkIfOpen();
    Objects.requireNonNull(row);
    Objects.requireNonNull(col);

    if (col.getFamily().equals(ColumnConstants.NOTIFY_CF)) {
      throw new IllegalArgumentException(ColumnConstants.NOTIFY_CF + " is a reserved family");
    }

    env.getSharedResources().getVisCache().validate(col);

    Map<Column, Bytes> colUpdates = updates.computeIfAbsent(row, k -> new HashMap<>());
    Bytes curVal = colUpdates.get(col);
    if (curVal != null && (isWrite(curVal) || isDelete(curVal))) {
      throw new AlreadySetException("Attemped read lock after write lock " + row + " " + col);
    }

    colUpdates.put(col, RLOCK_VAL);
  }

  @Override
  public SnapshotBase withReadLock() {
    return new ReadLockSnapshot(this);
  }

  @Override
  public void set(Bytes row, Column col, Bytes value) throws AlreadySetException {
    checkIfOpen();
    Objects.requireNonNull(row);
    Objects.requireNonNull(col);
    Objects.requireNonNull(value);

    if (col.getFamily().equals(ColumnConstants.NOTIFY_CF)) {
      throw new IllegalArgumentException(ColumnConstants.NOTIFY_CF + " is a reserved family");
    }

    env.getSharedResources().getVisCache().validate(col);

    Map<Column, Bytes> colUpdates = updates.computeIfAbsent(row, k -> new HashMap<>());

    Bytes curVal = colUpdates.get(col);
    if (curVal != null && isWrite(curVal)) {
      throw new AlreadySetException("Value already set " + row + " " + col);
    }
    colUpdates.put(col, value);
  }

  @Override
  public void setWeakNotification(Bytes row, Column col) {
    checkIfOpen();
    Objects.requireNonNull(row);
    Objects.requireNonNull(col);

    if (!env.getConfiguredObservers().getObservedColumns(WEAK).contains(col)) {
      throw new IllegalArgumentException("Column not configured for weak notifications " + col);
    }

    env.getSharedResources().getVisCache().validate(col);

    Set<Column> columns = weakNotifications.get(row);
    if (columns == null) {
      columns = new HashSet<>();
      weakNotifications.put(row, columns);
    }

    columns.add(col);
  }

  @Override
  public void delete(Bytes row, Column col) throws AlreadySetException {
    checkIfOpen();
    Objects.requireNonNull(row);
    Objects.requireNonNull(col);
    set(row, col, DELETE);
  }

  private ConditionalFlutation prewrite(ConditionalFlutation cm, Bytes row, Column col, Bytes val,
      Bytes primaryRow, Column primaryColumn, boolean isTriggerRow) {
    IteratorSetting iterConf = new IteratorSetting(10, PrewriteIterator.class);
    PrewriteIterator.setSnaptime(iterConf, startTs);
    boolean isTrigger = isTriggerRow && col.equals(notification.getColumn());
    if (isTrigger) {
      PrewriteIterator.enableAckCheck(iterConf, notification.getTimestamp());
    }

    if (isReadLock(val)) {
      PrewriteIterator.setReadlock(iterConf);
    }

    Condition cond = new FluoCondition(env, col).setIterators(iterConf);

    if (cm == null) {
      cm = new ConditionalFlutation(env, row, cond);
    } else {
      cm.addCondition(cond);
    }

    if (isWrite(val) && !isDelete(val)) {
      cm.put(col, ColumnType.DATA.encode(startTs), val.toArray());
    }

    if (isReadLock(val)) {
      cm.put(col, ColumnType.RLOCK.encode(ReadLockUtil.encodeTs(startTs, false)),
          ReadLockValue.encode(primaryRow, primaryColumn, getTransactorID()));
    } else {
      cm.put(col, ColumnType.LOCK.encode(startTs), LockValue.encode(primaryRow, primaryColumn,
          isWrite(val), isDelete(val), isTriggerRow, getTransactorID()));
    }

    return cm;
  }

  private ConditionalFlutation prewrite(Bytes row, Column col, Bytes val, Bytes primaryRow,
      Column primaryColumn, boolean isTriggerRow) {
    return prewrite(null, row, col, val, primaryRow, primaryColumn, isTriggerRow);
  }

  private void prewrite(ConditionalFlutation cm, Column col, Bytes val, Bytes primaryRow,
      Column primaryColumn, boolean isTriggerRow) {
    prewrite(cm, null, col, val, primaryRow, primaryColumn, isTriggerRow);
  }

  public static class CommitData {
    ConditionalWriter cw;
    private Bytes prow;
    private Column pcol;
    private Bytes pval;

    private HashSet<Bytes> acceptedRows;
    private Map<Bytes, Set<Column>> rejected = null;

    private void addPrimaryToRejected() {
      rejected = Collections.singletonMap(prow, Collections.singleton(pcol));
    }

    private void addToRejected(Bytes row, Set<Column> columns) {
      if (rejected == null) {
        rejected = new HashMap<>();
      }

      Set<Column> ret = rejected.put(row, columns);
      if (ret != null) {
        throw new IllegalStateException();
      }
    }

    private Map<Bytes, Set<Column>> getRejected() {
      if (rejected == null) {
        return Collections.emptyMap();
      }

      return rejected;
    }

    @Override
    public String toString() {
      return prow + " " + pcol + " " + pval + " " + getRejected().size();
    }

    public String getShortCollisionMessage() {
      StringBuilder sb = new StringBuilder();
      if (!getRejected().isEmpty()) {
        int numCollisions = 0;
        for (Set<Column> cols : getRejected().values()) {
          numCollisions += cols.size();
        }

        sb.append("Collisions(");
        sb.append(numCollisions);
        sb.append("):");

        String sep = "";
        outer: for (Entry<Bytes, Set<Column>> entry : getRejected().entrySet()) {
          Bytes row = entry.getKey();
          for (Column col : entry.getValue()) {
            sb.append(sep);
            sep = ", ";
            Hex.encNonAscii(sb, row);
            sb.append(" ");
            Hex.encNonAscii(sb, col, " ");
            if (sb.length() > 100) {
              sb.append(" ...");
              break outer;
            }
          }
        }
      }

      return sb.toString();
    }

    // async stuff
    private AsyncConditionalWriter acw;
    private AsyncConditionalWriter bacw;
    private AsyncCommitObserver commitObserver;

  }

  private boolean isTriggerRow(Bytes row) {
    return notification != null && notification.getRow().equals(row);
  }

  public boolean preCommit(CommitData cd) {
    return preCommit(cd, null);
  }

  @VisibleForTesting
  public boolean preCommit(CommitData cd, RowColumn primary) {

    synchronized (this) {
      checkIfOpen();
      status = TxStatus.COMMIT_STARTED;
      commitAttempted = true;
    }

    SyncCommitObserver sco = new SyncCommitObserver();
    cd = setUpBeginCommitAsync(cd, sco, primary);
    if (cd != null) {
      beginCommitAsyncTest(cd);
    }
    try {
      sco.waitForCommit();
    } catch (AlreadyAcknowledgedException e) {
      throw e;
    } catch (CommitException e) {
      return false;
    }
    return true;
  }

  /**
   * This function helps handle the following case
   *
   * <OL>
   * <LI>TX1 locks r1 col1
   * <LI>TX1 fails before unlocking
   * <LI>TX2 attempts to write r1:col1 w/o reading it
   * </OL>
   *
   * <p>
   * In this case TX2 would not roll back TX1, because it never read the column. This function
   * attempts to handle this case if TX2 fails. Only doing this in case of failures is cheaper than
   * trying to always read unread columns.
   *
   * @param cd Commit data
   */
  private void readUnread(CommitData cd, Consumer<Entry<Key, Value>> writeLocksSeen) {
    // TODO need to keep track of ranges read (not ranges passed in, but actual data read... user
    // may not iterate over entire range
    Collection<RowColumn> rowColumnsToRead = new ArrayList<>();

    for (Entry<Bytes, Set<Column>> entry : cd.getRejected().entrySet()) {
      Set<Column> rowColsRead = columnsRead.get(entry.getKey());
      if (rowColsRead == null) {
        for (Column column : entry.getValue()) {
          rowColumnsToRead.add(new RowColumn(entry.getKey(), column));
        }
      } else {
        HashSet<Column> colsToRead = new HashSet<>(entry.getValue());
        colsToRead.removeAll(rowColsRead);
        if (!colsToRead.isEmpty()) {
          for (Column column : colsToRead) {
            rowColumnsToRead.add(new RowColumn(entry.getKey(), column));
          }
        }
      }
    }

    getImpl(rowColumnsToRead, writeLocksSeen);
  }

  private void checkForOrphanedReadLocks(CommitData cd, Map<Bytes, Set<Column>> locksResolved)
      throws Exception {

    if (readLocksSeen.isEmpty()) {
      return;
    }

    Map<Bytes, Set<Column>> rowColsToCheck = new HashMap<>();

    for (Entry<Bytes, Set<Column>> entry : cd.getRejected().entrySet()) {

      Set<Column> resolvedColumns =
          locksResolved.getOrDefault(entry.getKey(), Collections.emptySet());

      Set<Column> colsToCheck = null;
      Set<Column> readLockCols = readLocksSeen.get(entry.getKey());
      if (readLockCols != null) {
        for (Column candidate : Sets.intersection(readLockCols, entry.getValue())) {
          if (resolvedColumns.contains(candidate)) {
            // A write lock was seen and this is probably what caused the collision, no need to
            // check this column for read locks.
            continue;
          }

          if (!isReadLock(updates.getOrDefault(entry.getKey(), Collections.emptyMap())
              .getOrDefault(candidate, EMPTY_BS))) {
            if (colsToCheck == null) {
              colsToCheck = new HashSet<>();
            }
            colsToCheck.add(candidate);
          }
        }

        if (colsToCheck != null) {
          rowColsToCheck.put(entry.getKey(), colsToCheck);
        }
      }
    }

    if (!rowColsToCheck.isEmpty()) {

      long waitTime = SnapshotScanner.INITIAL_WAIT_TIME;

      boolean resolved = false;

      List<Entry<Key, Value>> openReadLocks = LockResolver.getOpenReadLocks(env, rowColsToCheck);

      long startTime = System.currentTimeMillis();

      while (!resolved) {
        resolved = LockResolver.resolveLocks(env, startTs, stats, openReadLocks, startTime);
        if (!resolved) {
          UtilWaitThread.sleep(waitTime);
          stats.incrementLockWaitTime(waitTime);
          waitTime = Math.min(SnapshotScanner.MAX_WAIT_TIME, waitTime * 2);

          openReadLocks = LockResolver.getOpenReadLocks(env, rowColsToCheck);
        }
      }
    }
  }

  private void checkForOrphanedLocks(CommitData cd) throws Exception {

    Map<Bytes, Set<Column>> writeLocksSeen = new HashMap<>();

    readUnread(cd, kve -> {
      Bytes row = ByteUtil.toBytes(kve.getKey().getRowData());
      Column col = ColumnUtil.convert(kve.getKey());
      writeLocksSeen.computeIfAbsent(row, k -> new HashSet<>()).add(col);
    });

    checkForOrphanedReadLocks(cd, writeLocksSeen);
  }

  private boolean checkForAckCollision(ConditionalMutation cm) {
    Bytes row = Bytes.of(cm.getRow());

    if (isTriggerRow(row)) {
      List<ColumnUpdate> updates = cm.getUpdates();

      for (ColumnUpdate cu : updates) {
        // TODO avoid create col vis object
        Column col = new Column(Bytes.of(cu.getColumnFamily()), Bytes.of(cu.getColumnQualifier()),
            Bytes.of(cu.getColumnVisibility()));

        if (notification.getColumn().equals(col)) {
          // check to see if ACK exist after notification
          Key startKey = SpanUtil.toKey(notification.getRowColumn());
          startKey.setTimestamp(ColumnType.ACK.first());

          Key endKey = SpanUtil.toKey(notification.getRowColumn());
          endKey.setTimestamp(ColumnType.ACK.encode(notification.getTimestamp() + 1));

          Range range = new Range(startKey, endKey);

          try (Scanner scanner =
              env.getAccumuloClient().createScanner(env.getTable(), env.getAuthorizations())) {
            scanner.setRange(range);

            // TODO could use iterator that stops after 1st ACK. thought of using versioning iter
            // but
            // it scans to ACK
            if (scanner.iterator().hasNext()) {
              env.getSharedResources().getBatchWriter()
                  .writeMutationAsync(notification.newDelete(env));
              return true;
            }
          } catch (TableNotFoundException e) {
            // TODO proper exception handling
            throw new RuntimeException(e);
          }
        }
      }
    }

    return false;
  }

  public CommitData createCommitData() {
    CommitData cd = new CommitData();
    cd.cw = env.getSharedResources().getConditionalWriter();
    cd.acw = env.getSharedResources().getAsyncConditionalWriter();
    cd.bacw = env.getSharedResources().getBulkAsyncConditionalWriter();
    return cd;
  }

  @Override
  public synchronized void commit() throws CommitException {
    try {
      SyncCommitObserver sco = new SyncCommitObserver();
      commitAsync(sco);
      sco.waitForCommit();
    } finally {
      updates.clear();
      weakNotification = null;
      columnsRead.clear();
    }
  }

  void deleteWeakRow() {
    if (weakNotification != null) {
      env.getSharedResources().getBatchWriter()
          .writeMutation(weakNotification.newDelete(env, startTs));
    }
  }

  @Override
  public TxStats getStats() {
    return stats;
  }

  public long getStartTs() {
    return startTs;
  }

  /**
   * Sets the transactor of this transaction
   *
   * @param tnode TransactorNode
   * @return this Transaction
   */
  @VisibleForTesting
  public TransactionImpl setTransactor(TransactorNode tnode) {
    this.tnode = tnode;
    return this;
  }

  /**
   * Retrieves transactor ID by first getting/creating transactor (which is only done until
   * necessary)
   */
  private Long getTransactorID() {
    if (tnode == null) {
      tnode = env.getSharedResources().getTransactorNode();
    }
    return tnode.getTransactorID().getLongID();
  }

  private synchronized void close(boolean checkForStaleScan) {
    if (asyncReader != null) {
      asyncReader.close();
    }

    if (status != TxStatus.CLOSED) {
      status = TxStatus.CLOSED;

      if (checkForStaleScan && !commitAttempted) {
        Stamp stamp = env.getSharedResources().getOracleClient().getStamp();
        if (startTs < stamp.getGcTimestamp()) {
          throw new StaleScanException();
        }
      }

      env.getSharedResources().getTimestampTracker().removeTimestamp(startTs);
    }
  }

  @Override
  public void close() {
    close(true);
  }

  private synchronized void checkIfOpen() {
    if (status != TxStatus.OPEN) {
      throw new IllegalStateException("Transaction is no longer open! status = " + status);
    }
  }

  // CHECKSTYLE:OFF
  @Override
  protected void finalize() {
    // CHECKSTYLE:ON
    // TODO Log an error if transaction is not closed (See FLUO-486)
    close(false);
  }

  @Override
  public long getStartTimestamp() {
    return startTs;
  }

  @Override
  public int getSize() {
    // TODO could calculate as items are added/set
    int size = 0;

    for (Entry<Bytes, Map<Column, Bytes>> entry : updates.entrySet()) {
      size += entry.getKey().length();
      for (Entry<Column, Bytes> entry2 : entry.getValue().entrySet()) {
        Column c = entry2.getKey();
        size += c.getFamily().length();
        size += c.getQualifier().length();
        size += c.getVisibility().length();
        size += entry2.getValue().length();
      }
    }

    for (Entry<Bytes, Set<Column>> entry : columnsRead.entrySet()) {
      size += entry.getKey().length();
      for (Column c : entry.getValue()) {
        size += c.getFamily().length();
        size += c.getQualifier().length();
        size += c.getVisibility().length();
      }
    }

    return size;
  }

  abstract class CommitStep {
    private CommitStep nextStep;

    // the boolean indicates if the operation was successful.
    abstract CompletableFuture<Boolean> getMainOp(CommitData cd);

    // create and run this op in the event that the main op was a failure
    abstract CompletableFuture<Void> getFailureOp(CommitData cd);

    // set the next step to run if this step is successful
    CommitStep andThen(CommitStep next) {
      this.nextStep = next;
      return next;
    }


    CompletableFuture<Void> compose(CommitData cd) {
      return getMainOp(cd).thenComposeAsync(successful -> {
        if (successful) {
          if (nextStep != null) {
            return nextStep.compose(cd);
          } else {
            return CompletableFuture.completedFuture(null);
          }
        } else {
          return getFailureOp(cd);
        }
      }, env.getSharedResources().getAsyncCommitExecutor());
    }

  }

  abstract class ConditionalStep extends CommitStep {

    public abstract Collection<ConditionalMutation> createMutations(CommitData cd);

    public abstract Iterator<Result> handleUnknown(CommitData cd, Iterator<Result> results)
        throws Exception;

    public abstract boolean processResults(CommitData cd, Iterator<Result> results)
        throws Exception;

    public AsyncConditionalWriter getACW(CommitData cd) {
      return cd.acw;
    }

    @Override
    CompletableFuture<Boolean> getMainOp(CommitData cd) {
      // TODO not sure threading is correct
      Executor ace = env.getSharedResources().getAsyncCommitExecutor();
      return getACW(cd).apply(createMutations(cd)).thenCompose(results -> {
        // ugh icky that this is an iterator, forces copy to inspect.. could refactor async CW to
        // return collection
        ArrayList<Result> resultsList = new ArrayList<>();
        Iterators.addAll(resultsList, results);
        boolean containsUknown = false;
        for (Result result : resultsList) {
          try {
            containsUknown |= result.getStatus() == Status.UNKNOWN;
          } catch (Exception e) {
            throw new CompletionException(e);
          }
        }
        if (containsUknown) {
          // process unknown in sync executor
          Executor se = env.getSharedResources().getSyncCommitExecutor();
          return CompletableFuture.supplyAsync(() -> {
            try {
              return handleUnknown(cd, resultsList.iterator());
            } catch (Exception e) {
              throw new CompletionException(e);
            }
          }, se);
        } else {
          return CompletableFuture.completedFuture(resultsList.iterator());
        }
      }).thenApplyAsync(results -> {
        try {
          return processResults(cd, results);
        } catch (Exception e) {
          throw new CompletionException(e);
        }
      }, ace);
    }


  }

  class LockPrimaryStep extends ConditionalStep {

    @Override
    public Collection<ConditionalMutation> createMutations(CommitData cd) {
      return Collections
          .singleton(prewrite(cd.prow, cd.pcol, cd.pval, cd.prow, cd.pcol, isTriggerRow(cd.prow)));
    }

    @Override
    public Iterator<Result> handleUnknown(CommitData cd, Iterator<Result> results)
        throws Exception {

      Result result = Iterators.getOnlyElement(results);
      Status mutationStatus = result.getStatus();
      // TODO convert this code to async
      while (mutationStatus == Status.UNKNOWN) {
        TxInfo txInfo = TxInfo.getTransactionInfo(env, cd.prow, cd.pcol, startTs);

        switch (txInfo.status) {
          case LOCKED:
            return Collections
                .singleton(
                    new Result(Status.ACCEPTED, result.getMutation(), result.getTabletServer()))
                .iterator();
          case ROLLED_BACK:
            return Collections
                .singleton(
                    new Result(Status.REJECTED, result.getMutation(), result.getTabletServer()))
                .iterator();
          case UNKNOWN:
            // TODO async
            Result newResult = cd.cw.write(result.getMutation());
            mutationStatus = newResult.getStatus();
            if (mutationStatus != Status.UNKNOWN) {
              return Collections.singleton(newResult).iterator();
            }
            // TODO handle case were data other tx has lock
            break;
          case COMMITTED:
          default:
            throw new IllegalStateException(
                "unexpected tx state " + txInfo.status + " " + cd.prow + " " + cd.pcol);

        }
      }

      // TODO
      throw new IllegalStateException();
    }

    @Override
    public boolean processResults(CommitData cd, Iterator<Result> results) throws Exception {
      Result result = Iterators.getOnlyElement(results);
      return result.getStatus() == Status.ACCEPTED;
    }

    @Override
    CompletableFuture<Void> getFailureOp(CommitData cd) {
      // TODO can this be simplified by pushing some code to the superclass?
      return CompletableFuture.supplyAsync(() -> {
        final ConditionalMutation pcm = Iterables.getOnlyElement(createMutations(cd));

        cd.addPrimaryToRejected();
        getStats().setRejected(cd.getRejected());
        // TODO do async
        try {
          checkForOrphanedLocks(cd);
        } catch (Exception e) {
          throw new CompletionException(e);
        }
        if (checkForAckCollision(pcm)) {
          cd.commitObserver.alreadyAcknowledged();
        } else {
          cd.commitObserver.commitFailed(cd.getShortCollisionMessage());
        }

        return null;
      }, env.getSharedResources().getSyncCommitExecutor());
    }

  }

  class LockOtherStep extends ConditionalStep {

    @Override
    public AsyncConditionalWriter getACW(CommitData cd) {
      return cd.bacw;
    }


    @Override
    public Collection<ConditionalMutation> createMutations(CommitData cd) {

      ArrayList<ConditionalMutation> mutations = new ArrayList<>();

      for (Entry<Bytes, Map<Column, Bytes>> rowUpdates : updates.entrySet()) {
        ConditionalFlutation cm = null;

        for (Entry<Column, Bytes> colUpdates : rowUpdates.getValue().entrySet()) {
          if (cm == null) {
            cm = prewrite(rowUpdates.getKey(), colUpdates.getKey(), colUpdates.getValue(), cd.prow,
                cd.pcol, false);
          } else {
            prewrite(cm, colUpdates.getKey(), colUpdates.getValue(), cd.prow, cd.pcol, false);
          }
        }

        mutations.add(cm);
      }

      cd.acceptedRows = new HashSet<>();

      return mutations;
    }

    @Override
    public Iterator<Result> handleUnknown(CommitData cd, Iterator<Result> results) {
      // TODO this step does not currently handle unknown
      return results;
    }

    @Override
    public boolean processResults(CommitData cd, Iterator<Result> results) throws Exception {

      while (results.hasNext()) {
        Result result = results.next();
        // TODO handle unknown?
        Bytes row = Bytes.of(result.getMutation().getRow());
        if (result.getStatus() == Status.ACCEPTED) {
          cd.acceptedRows.add(row);
        } else {
          cd.addToRejected(row, updates.get(row).keySet());
        }
      }

      return cd.getRejected().isEmpty();
    }

    @Override
    CompletableFuture<Void> getFailureOp(CommitData cd) {
      return CompletableFuture.supplyAsync(() -> {
        getStats().setRejected(cd.getRejected());
        try {
          // Does this need to be async?
          checkForOrphanedLocks(cd);
        } catch (Exception e) {
          throw new CompletionException(e);
        }
        return null;
      }, env.getSharedResources().getSyncCommitExecutor()).thenCompose(v -> rollbackLocks(cd));
    }
  }

  abstract class BatchWriterStep extends CommitStep {
    public abstract Collection<Mutation> createMutations(CommitData cd);

    @Override
    CompletableFuture<Boolean> getMainOp(CommitData cd) {
      return env.getSharedResources().getBatchWriter()
          .writeMutationsAsyncFuture(createMutations(cd)).thenApply(v -> true);
    }

    @Override
    CompletableFuture<Void> getFailureOp(CommitData cd) {
      throw new IllegalStateException("Failure not expected");
    }
  }



  private CompletableFuture<Void> rollbackLocks(CommitData cd) {
    CommitStep firstStep = new RollbackOtherLocks();
    firstStep.andThen(new RollbackPrimaryLock());

    return firstStep.compose(cd)
        .thenRun(() -> cd.commitObserver.commitFailed(cd.getShortCollisionMessage()));

  }


  class RollbackOtherLocks extends BatchWriterStep {

    @Override
    public Collection<Mutation> createMutations(CommitData cd) {
      // roll back locks

      // TODO let rollback be done lazily? this makes GC more difficult

      Flutation m;

      ArrayList<Mutation> mutations = new ArrayList<>(cd.acceptedRows.size());
      for (Bytes row : cd.acceptedRows) {
        m = new Flutation(env, row);
        for (Entry<Column, Bytes> entry : updates.get(row).entrySet()) {
          if (isReadLock(entry.getValue())) {
            m.put(entry.getKey(), ColumnType.RLOCK.encode(ReadLockUtil.encodeTs(startTs, true)),
                DelReadLockValue.encodeRollback());
          } else {
            m.put(entry.getKey(), ColumnType.DEL_LOCK.encode(startTs),
                DelLockValue.encodeRollback(false, true));
          }
        }
        mutations.add(m);
      }

      return mutations;
    }
  }

  class RollbackPrimaryLock extends BatchWriterStep {

    @Override
    public Collection<Mutation> createMutations(CommitData cd) {
      // mark transaction as complete for garbage collection purposes
      Flutation m = new Flutation(env, cd.prow);

      m.put(cd.pcol, ColumnType.DEL_LOCK.encode(startTs),
          DelLockValue.encodeRollback(startTs, true, true));
      m.put(cd.pcol, ColumnType.TX_DONE.encode(startTs), EMPTY);

      return Collections.singletonList(m);
    }
  }

  class CommittedTestStep extends CommitStep {
    @Override
    CompletableFuture<Boolean> getMainOp(CommitData cd) {
      cd.commitObserver.committed();
      return CompletableFuture.completedFuture(true);
    }

    @Override
    CompletableFuture<Void> getFailureOp(CommitData cd) {
      throw new IllegalStateException("Failure not expected");
    }
  }

  @VisibleForTesting
  public boolean commitPrimaryColumn(CommitData cd, Stamp commitStamp) {

    SyncCommitObserver sco = new SyncCommitObserver();
    cd.commitObserver = sco;
    try {
      CommitStep firstStep = new GetCommitStampStepTest(commitStamp);

      firstStep.andThen(new WriteNotificationsStep()).andThen(new CommitPrimaryStep())
          .andThen(new CommittedTestStep());

      firstStep.compose(cd).exceptionally(throwable -> {
        setFailed(cd, throwable);
        return null;
      });
      sco.waitForCommit();
    } catch (CommitException e) {
      return false;
    } catch (Exception e) {
      throw new FluoException(e);
    }
    return true;
  }

  class GetCommitStampStep extends CommitStep {
    protected CompletableFuture<Stamp> getStampOp() {
      return env.getSharedResources().getOracleClient().getStampAsync();
    }

    @Override
    CompletableFuture<Boolean> getMainOp(CommitData cd) {

      return getStampOp().thenApply(commitStamp -> {
        if (startTs < commitStamp.getGcTimestamp()) {
          return false;
        } else {
          getStats().setCommitTs(commitStamp.getTxTimestamp());
          return true;
        }
      });
    }

    @Override
    CompletableFuture<Void> getFailureOp(CommitData cd) {
      return rollbackLocks(cd);
    }

  }

  class GetCommitStampStepTest extends GetCommitStampStep {
    private final Stamp testStamp;

    public GetCommitStampStepTest(Stamp testStamp) {
      this.testStamp = testStamp;
    }

    @Override
    protected CompletableFuture<Stamp> getStampOp() {
      return CompletableFuture.completedFuture(testStamp);
    }

  }

  class WriteNotificationsStep extends BatchWriterStep {

    @Override
    public Collection<Mutation> createMutations(CommitData cd) {
      long commitTs = getStats().getCommitTs();
      HashMap<Bytes, Mutation> mutations = new HashMap<>();

      if (observedColumns.contains(cd.pcol) && isWrite(cd.pval) && !isDelete(cd.pval)) {
        Flutation m = new Flutation(env, cd.prow);
        Notification.put(env, m, cd.pcol, commitTs);
        mutations.put(cd.prow, m);
      }

      for (Entry<Bytes, Map<Column, Bytes>> rowUpdates : updates.entrySet()) {

        for (Entry<Column, Bytes> colUpdates : rowUpdates.getValue().entrySet()) {
          if (observedColumns.contains(colUpdates.getKey())) {
            Bytes val = colUpdates.getValue();
            if (isWrite(val) && !isDelete(val)) {
              Mutation m = mutations.get(rowUpdates.getKey());
              if (m == null) {
                m = new Flutation(env, rowUpdates.getKey());
                mutations.put(rowUpdates.getKey(), m);
              }
              Notification.put(env, m, colUpdates.getKey(), commitTs);
            }
          }
        }
      }

      for (Entry<Bytes, Set<Column>> entry : weakNotifications.entrySet()) {
        Mutation m = mutations.get(entry.getKey());
        if (m == null) {
          m = new Flutation(env, entry.getKey());
          mutations.put(entry.getKey(), m);
        }
        for (Column col : entry.getValue()) {
          Notification.put(env, m, col, commitTs);
        }
      }
      return mutations.values();
    }

  }

  class CommitPrimaryStep extends ConditionalStep {

    @Override
    public Collection<ConditionalMutation> createMutations(CommitData cd) {
      long commitTs = getStats().getCommitTs();
      IteratorSetting iterConf = new IteratorSetting(10, PrewriteIterator.class);
      PrewriteIterator.setSnaptime(iterConf, startTs);
      boolean isTrigger = isTriggerRow(cd.prow) && cd.pcol.equals(notification.getColumn());

      Condition lockCheck =
          new FluoCondition(env, cd.pcol).setIterators(iterConf).setValue(LockValue.encode(cd.prow,
              cd.pcol, isWrite(cd.pval), isDelete(cd.pval), isTrigger, getTransactorID()));
      final ConditionalMutation delLockMutation = new ConditionalFlutation(env, cd.prow, lockCheck);

      ColumnUtil.commitColumn(env, isTrigger, true, cd.pcol, isWrite(cd.pval), isDelete(cd.pval),
          isReadLock(cd.pval), startTs, commitTs, observedColumns, delLockMutation);

      return Collections.singletonList(delLockMutation);
    }

    @Override
    public Iterator<Result> handleUnknown(CommitData cd, Iterator<Result> results)
        throws Exception {
      // the code for handing this is synchronous and needs to be handled in another thread pool
      // TODO - how do we do the above without return a CF?
      long commitTs = getStats().getCommitTs();
      Result result = Iterators.getOnlyElement(results);
      Status ms = result.getStatus();

      while (ms == Status.UNKNOWN) {

        // TODO async
        TxInfo txInfo = TxInfo.getTransactionInfo(env, cd.prow, cd.pcol, startTs);

        switch (txInfo.status) {
          case COMMITTED:
            if (txInfo.commitTs != commitTs) {
              throw new IllegalStateException(
                  cd.prow + " " + cd.pcol + " " + txInfo.commitTs + "!=" + commitTs);
            }
            ms = Status.ACCEPTED;
            break;
          case LOCKED:
            // TODO async
            ConditionalMutation delLockMutation = result.getMutation();
            ms = cd.cw.write(delLockMutation).getStatus();
            break;
          default:
            ms = Status.REJECTED;
        }
      }
      Result newResult = new Result(ms, result.getMutation(), result.getTabletServer());
      return Collections.singletonList(newResult).iterator();
    }

    @Override
    public boolean processResults(CommitData cd, Iterator<Result> results) throws Exception {
      Result result = Iterators.getOnlyElement(results);
      return result.getStatus() == Status.ACCEPTED;
    }

    @Override
    CompletableFuture<Void> getFailureOp(CommitData cd) {
      cd.commitObserver.commitFailed(cd.getShortCollisionMessage());
      return CompletableFuture.completedFuture(NULLS.get());
    }

  }

  @VisibleForTesting
  public boolean finishCommit(CommitData cd, Stamp commitStamp) {
    cd.commitObserver = new SyncCommitObserver();

    getStats().setCommitTs(commitStamp.getTxTimestamp());

    CommitStep firstStep = new DeleteLocksStep();
    firstStep.andThen(new FinishCommitStep());
    firstStep.compose(cd).exceptionally(throwable -> {
      System.err.println("Unexpected exception in finish commit test method : ");
      throwable.printStackTrace();
      return null;
    });

    return true;
  }


  class DeleteLocksStep extends BatchWriterStep {

    @Override
    public Collection<Mutation> createMutations(CommitData cd) {
      long commitTs = getStats().getCommitTs();
      ArrayList<Mutation> mutations = new ArrayList<>(updates.size() + 1);
      for (Entry<Bytes, Map<Column, Bytes>> rowUpdates : updates.entrySet()) {
        Flutation m = new Flutation(env, rowUpdates.getKey());
        boolean isTriggerRow = isTriggerRow(rowUpdates.getKey());
        for (Entry<Column, Bytes> colUpdates : rowUpdates.getValue().entrySet()) {
          ColumnUtil.commitColumn(env,
              isTriggerRow && colUpdates.getKey().equals(notification.getColumn()), false,
              colUpdates.getKey(), isWrite(colUpdates.getValue()), isDelete(colUpdates.getValue()),
              isReadLock(colUpdates.getValue()), startTs, commitTs, observedColumns, m);
        }

        mutations.add(m);
      }

      return mutations;
    }

  }

  class FinishCommitStep extends BatchWriterStep {

    @Override
    CompletableFuture<Boolean> getMainOp(CommitData cd) {
      return super.getMainOp(cd).thenApply(b -> {
        Preconditions.checkArgument(b);
        cd.commitObserver.committed();
        return true;
      });
    }

    @Override
    public Collection<Mutation> createMutations(CommitData cd) {
      long commitTs = getStats().getCommitTs();
      ArrayList<Mutation> afterFlushMutations = new ArrayList<>(2);

      Flutation m = new Flutation(env, cd.prow);
      // mark transaction as complete for garbage collection purposes
      m.put(cd.pcol, ColumnType.TX_DONE.encode(commitTs), EMPTY);
      afterFlushMutations.add(m);

      if (weakNotification != null) {
        afterFlushMutations.add(weakNotification.newDelete(env, startTs));
      }

      if (notification != null) {
        afterFlushMutations.add(notification.newDelete(env, startTs));
      }

      return afterFlushMutations;
    }

  }

  @Override
  public synchronized void commitAsync(AsyncCommitObserver commitCallback) {

    checkIfOpen();
    status = TxStatus.COMMIT_STARTED;
    commitAttempted = true;

    try {
      CommitData cd = createCommitData();
      cd = setUpBeginCommitAsync(cd, commitCallback, null);
      if (cd != null) {
        beginCommitAsync(cd);
      }
    } catch (Exception e) {
      e.printStackTrace();
      commitCallback.failed(e);
    }
  }

  private CommitData setUpBeginCommitAsync(CommitData cd, AsyncCommitObserver commitCallback,
      RowColumn primary) {
    if (updates.isEmpty()) {
      // TODO do async
      deleteWeakRow();
      commitCallback.committed();
      return null;
    }

    for (Map<Column, Bytes> cols : updates.values()) {
      stats.incrementEntriesSet(cols.size());
    }

    Bytes primRow = null;
    Column primCol = null;

    if (primary != null) {
      primRow = primary.getRow();
      primCol = primary.getColumn();
      if (notification != null && !primary.equals(notification.getRowColumn())) {
        throw new IllegalArgumentException("Primary must be notification");
      }
    } else if (notification != null) {
      primRow = notification.getRow();
      primCol = notification.getColumn();
    } else {

      outer: for (Entry<Bytes, Map<Column, Bytes>> entry : updates.entrySet()) {
        for (Entry<Column, Bytes> entry2 : entry.getValue().entrySet()) {
          if (!isReadLock(entry2.getValue())) {
            primRow = entry.getKey();
            primCol = entry2.getKey();
            break outer;
          }
        }
      }

      if (primRow == null) {
        // there are only read locks, so nothing to write
        deleteWeakRow();
        commitCallback.committed();
        return null;
      }
    }

    // get a primary column
    cd.prow = primRow;
    Map<Column, Bytes> colSet = updates.get(cd.prow);
    cd.pcol = primCol;
    cd.pval = colSet.remove(primCol);
    if (colSet.isEmpty()) {
      updates.remove(cd.prow);
    }

    cd.commitObserver = commitCallback;

    return cd;
  }

  private void setFailed(CommitData cd, Throwable throwable) {
    try {
      cd.commitObserver.failed(throwable);
    } catch (RuntimeException e) {
      Logger log = LoggerFactory.getLogger(TransactionImpl.class);
      log.error("Failed to set tx failure (startTs=" + startTs + ") cause ", e);
      log.error("Failed to set tx failure (startTs=" + startTs + ") lost throwable ", throwable);
    }
  }

  private void beginCommitAsync(CommitData cd) {

    // Notification are written between GetCommitStampStep and CommitPrimaryStep for the following
    // reasons :
    // * At this point all columns are locked, this guarantees that anything triggering as a
    // result of this transaction will see all of this transactions changes.
    // * The transaction is not yet committed. If the process dies at this point whatever
    // was running this transaction should rerun and recreate all of the notifications.
    // The next transactions will rerun because this transaction will have to be rolled back.
    // * If notifications are written in the 2nd phase of commit, then when the 2nd phase
    // partially succeeds notifications may never be written. Because in the case of failure
    // notifications would not be written until a column is read and it may never be read.
    // See https://github.com/fluo-io/fluo/issues/642
    //
    // Its very important the notifications which trigger an observer are deleted after the 2nd
    // phase of commit finishes.

    CommitStep firstStep = new LockPrimaryStep();

    firstStep.andThen(new LockOtherStep()).andThen(new GetCommitStampStep())
        .andThen(new WriteNotificationsStep()).andThen(new CommitPrimaryStep())
        .andThen(new DeleteLocksStep()).andThen(new FinishCommitStep());

    firstStep.compose(cd).exceptionally(throwable -> {
      setFailed(cd, throwable);
      return null;
    });
  }

  private void beginCommitAsyncTest(CommitData cd) {

    CommitStep firstStep = new LockPrimaryStep();

    firstStep.andThen(new LockOtherStep()).andThen(new CommittedTestStep());

    firstStep.compose(cd).exceptionally(throwable -> {
      setFailed(cd, throwable);
      return null;
    });
  }

  public SnapshotScanner newSnapshotScanner(Span span, Collection<Column> columns) {
    return new SnapshotScanner(env, new SnapshotScanner.Opts(span, columns, false), startTs, stats);
  }
}
