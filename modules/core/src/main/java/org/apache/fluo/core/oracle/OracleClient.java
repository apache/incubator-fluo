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

package org.apache.fluo.core.oracle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.fluo.accumulo.util.ZookeeperPath;
import org.apache.fluo.api.exceptions.FluoException;
import org.apache.fluo.core.impl.CuratorCnxnListener;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.metrics.MetricsUtil;
import org.apache.fluo.core.thrift.OracleService;
import org.apache.fluo.core.thrift.Stamps;
import org.apache.fluo.core.util.CuratorUtil;
import org.apache.fluo.core.util.UtilWaitThread;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.layered.TFastFramedTransport;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connects to an oracle to retrieve timestamps. If multiple oracle servers are run, it will
 * automatically fail over to different leaders.
 */
public class OracleClient implements AutoCloseable {

  public static final Logger log = LoggerFactory.getLogger(OracleClient.class);
  private static final int MAX_ORACLE_WAIT_PERIOD = 60;

  private final Timer responseTimer;
  private final Histogram stampsHistogram;

  private Participant currentLeader;

  private static final class TimeRequest {
    CountDownLatch cdl = new CountDownLatch(1);
    AtomicReference<Stamp> stampRef = new AtomicReference<>();
    CompletableFuture<Stamp> cf = null;
  }

  private class TimestampRetriever extends LeaderSelectorListenerAdapter
      implements Runnable, PathChildrenCacheListener {

    private LeaderLatch leaderLatch;
    private CuratorFramework curatorFramework;
    private OracleService.Client client;
    private PathChildrenCache pathChildrenCache;

    private TTransport transport;

    @Override
    public void run() {

      try {
        synchronized (this) {
          // want this code to be mutually exclusive with close() .. so if in middle of setup, close
          // method will wait till finished
          if (closed.get()) {
            return;
          }

          curatorFramework = CuratorUtil.newAppCurator(env.getConfiguration());
          CuratorCnxnListener cnxnListener = new CuratorCnxnListener();
          curatorFramework.getConnectionStateListenable().addListener(cnxnListener);
          curatorFramework.start();

          while (!cnxnListener.isConnected()) {
            UtilWaitThread.sleep(200);
          }

          leaderLatch = new LeaderLatch(curatorFramework, ZookeeperPath.ORACLE_SERVER);

          pathChildrenCache =
              new PathChildrenCache(curatorFramework, ZookeeperPath.ORACLE_SERVER, true);
          pathChildrenCache.getListenable().addListener(this);
          pathChildrenCache.start();

          connect();
        }
        doWork();
      } catch (Exception e) {
        if (!closed.get()) {
          log.error("Exception occurred in run() method", e);
        } else {
          log.debug("Exception occurred in run() method", e);
        }
      }
    }

    /**
     * It's possible an Oracle has gone into a bad state. Upon the leader being changed, we want to
     * update our state
     */
    @Override
    public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent event)
        throws Exception {

      if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)
          || event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)
          || event.getType().equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)) {

        Participant participant = leaderLatch.getLeader();
        synchronized (this) {
          if (isLeader(participant)) {
            currentLeader = leaderLatch.getLeader();
          } else {
            currentLeader = null;
          }
        }
      }
    }

    private void doWork() {

      ArrayList<TimeRequest> request = new ArrayList<>();

      while (true) {

        try {
          request.clear();
          TimeRequest trh = null;
          while (trh == null) {
            if (closed.get()) {
              return;
            }
            trh = queue.poll(1, TimeUnit.SECONDS);
          }
          request.add(trh);
          queue.drainTo(request);

          long txStampsStart;
          long gcStamp;

          while (true) {

            try {
              String currentLeaderId;
              OracleService.Client localClient;
              synchronized (this) {
                currentLeaderId = getOracle();
                localClient = client;
              }

              final Context timerContext = responseTimer.time();

              Stamps stamps = localClient.getTimestamps(env.getFluoApplicationID(), request.size());
              txStampsStart = stamps.txStampsStart;
              gcStamp = stamps.gcStamp;

              String leaderId = getOracle();
              if (leaderId != null && !leaderId.equals(currentLeaderId)) {
                reconnect();
                continue;
              }

              stampsHistogram.update(request.size());
              timerContext.close();

              break;

            } catch (TTransportException tte) {
              log.info("Oracle connection lost. Retrying...");
              reconnect();
            } catch (TException e) {
              log.error("TException occurred in doWork() method", e);
            }
          }

          for (int i = 0; i < request.size(); i++) {
            TimeRequest tr = request.get(i);
            Stamp stampRes = new Stamp(txStampsStart + i, gcStamp);
            tr.stampRef.set(stampRes);
            if (tr.cf == null) {
              tr.cdl.countDown();
            } else {
              tr.cf.complete(stampRes);
            }
          }
        } catch (InterruptedException e) {
          if (!closed.get()) {
            log.error("InterruptedException occurred in doWork() method", e);
          } else {
            log.debug("InterruptedException occurred in doWork() method", e);
          }
        } catch (Exception e) {
          log.error("Exception occurred in doWork() method", e);
        }
      }
    }

    private synchronized void connect()
        throws IOException, KeeperException, InterruptedException, TTransportException {

      getLeader();
      while (true) {
        log.debug("Connecting to oracle at " + currentLeader.getId());
        String[] hostAndPort = currentLeader.getId().split(":");

        String host = hostAndPort[0];
        int port = Integer.parseInt(hostAndPort[1]);

        try {
          transport = new TFastFramedTransport(new TSocket(host, port));
          transport.open();
          TProtocol protocol = new TCompactProtocol(transport);
          client = new OracleService.Client(protocol);
          log.info("Connected to oracle at " + getOracle());
          break;
        } catch (TTransportException e) {
          sleepRandom();
          getLeader();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }

    /**
     * Atomically closes current connection and connects to the current leader
     */
    private synchronized void reconnect()
        throws InterruptedException, TTransportException, KeeperException, IOException {
      if (transport.isOpen()) {
        transport.close();
      }
      connect();
    }

    private synchronized void close() {
      if (transport != null && transport.isOpen()) {
        transport.close();
      }
      try {
        if (pathChildrenCache != null) {
          pathChildrenCache.close();
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      if (curatorFramework != null) {
        curatorFramework.close();
      }

      transport = null;
      pathChildrenCache = null;
      leaderLatch = null;
      curatorFramework = null;
    }

    private boolean getLeaderAttempt() {
      Participant possibleLeader = null;
      try {
        possibleLeader = leaderLatch.getLeader();
      } catch (KeeperException e) {
        log.debug("Exception throw in getLeaderAttempt()", e);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      if (isLeader(possibleLeader)) {
        currentLeader = possibleLeader;
        return true;
      }
      return false;
    }

    /**
     * Attempt to retrieve a leader until one is found
     */
    private void getLeader() {
      boolean found = getLeaderAttempt();
      while (!found) {
        sleepRandom();
        found = getLeaderAttempt();
      }
    }

    /**
     * Sleep a random amount of time from 100ms to 1sec
     */
    private void sleepRandom() {
      UtilWaitThread.sleep(100 + (long) (1000 * Math.random()), closed);
    }

    private boolean isLeader(Participant participant) {
      return participant != null && participant.isLeader();
    }

    /**
     * NOTE: This isn't competing for leadership, so it doesn't need to be started.
     */
    @Override
    public void takeLeadership(CuratorFramework curatorFramework) throws Exception {}
  }

  private final Environment env;
  private final ArrayBlockingQueue<TimeRequest> queue = new ArrayBlockingQueue<>(10000);
  private final Thread thread;
  private AtomicBoolean closed = new AtomicBoolean(false);
  private final TimestampRetriever timestampRetriever;

  public OracleClient(Environment env) {
    this.env = env;
    responseTimer = MetricsUtil.getTimer(env.getConfiguration(),
        env.getSharedResources().getMetricRegistry(), env.getMetricNames().getOracleResponseTime());
    stampsHistogram = MetricsUtil.getHistogram(env.getConfiguration(),
        env.getSharedResources().getMetricRegistry(), env.getMetricNames().getOracleClientStamps());
    timestampRetriever = new TimestampRetriever();
    thread = new Thread(timestampRetriever);
    thread.setDaemon(true);
    thread.start();
  }

  /**
   * Retrieves time stamp from Oracle. Throws {@link FluoException} if timed out or interrupted.
   */
  public Stamp getStamp() {
    checkClosed();

    TimeRequest tr = new TimeRequest();
    try {
      queue.put(tr);
      int timeout = env.getConfiguration().getConnectionRetryTimeout();
      if (timeout < 0) {
        long waitPeriod = 1;
        long waitTotal = 0;
        while (!tr.cdl.await(waitPeriod, TimeUnit.SECONDS)) {
          checkClosed();
          waitTotal += waitPeriod;
          if (waitPeriod < MAX_ORACLE_WAIT_PERIOD) {
            waitPeriod *= 2;
          }
          log.warn(
              "Waiting for timestamp from Oracle. Is it running? Client has waited a total of {}s and will retry in {}s",
              waitTotal, waitPeriod);
        }
      } else if (!tr.cdl.await(timeout, TimeUnit.MILLISECONDS)) {
        throw new FluoException("Timed out (after " + timeout
            + "ms) trying to retrieve timestamp from Oracle.  Is the Oracle running?");
      }
    } catch (InterruptedException e) {
      throw new FluoException("Interrupted while retrieving timestamp from Oracle", e);
    }
    return tr.stampRef.get();
  }

  public CompletableFuture<Stamp> getStampAsync() {
    checkClosed();

    TimeRequest tr = new TimeRequest();
    CompletableFuture<Stamp> cf = new CompletableFuture<>();
    tr.cf = cf;
    try {
      queue.put(tr);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return cf;
  }

  /**
   * Return the oracle that the current client is connected to.
   */
  public synchronized String getOracle() {
    checkClosed();
    return currentLeader != null ? currentLeader.getId() : null;
  }

  private void checkClosed() {
    if (closed.get()) {
      throw new IllegalStateException(OracleClient.class.getSimpleName() + " is closed");
    }
  }

  @Override
  public void close() {
    if (!closed.get()) {
      closed.set(true);
      try {
        thread.interrupt();
        thread.join();
        timestampRetriever.close();
      } catch (InterruptedException e) {
        throw new FluoException("Interrupted during close", e);
      }
    }
  }
}
