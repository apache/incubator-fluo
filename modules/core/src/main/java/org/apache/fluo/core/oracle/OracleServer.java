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

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Histogram;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.fluo.accumulo.util.LongUtil;
import org.apache.fluo.accumulo.util.ZookeeperPath;
import org.apache.fluo.core.impl.CuratorCnxnListener;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.impl.FluoConfigurationImpl;
import org.apache.fluo.core.metrics.MetricsUtil;
import org.apache.fluo.core.thrift.OracleService;
import org.apache.fluo.core.thrift.Stamps;
import org.apache.fluo.core.util.CuratorUtil;
import org.apache.fluo.core.util.FluoThreadFactory;
import org.apache.fluo.core.util.Halt;
import org.apache.fluo.core.util.HostUtil;
import org.apache.fluo.core.util.PortUtils;
import org.apache.fluo.core.util.UtilWaitThread;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.layered.TFastFramedTransport;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Oracle server is the responsible for providing incrementing logical timestamps to clients. It
 * should never give the same timestamp to two clients and it should always provide an incrementing
 * timestamp.
 *
 * <p>
 * If multiple oracle servers are run, they will choose a leader and clients will automatically
 * connect to that leader. If the leader goes down, the client will automatically fail over to the
 * next leader. In the case where an oracle fails over, the next oracle will begin a new block of
 * timestamps.
 */
public class OracleServer implements OracleService.Iface, PathChildrenCacheListener {

  private static final Logger log = LoggerFactory.getLogger(OracleServer.class);

  private final Histogram stampsHistogram;

  public static final long ORACLE_MAX_READ_BUFFER_BYTES = 2048;

  private final Environment env;

  private Thread serverThread;
  private THsHaServer server;
  private volatile long currentTs = 0;
  private volatile long maxTs = 0;
  private volatile boolean started = false;
  private int port = 0;

  private LeaderLatch leaderLatch;
  private ExecutorService execService;
  private PathChildrenCache pathChildrenCache;
  private CuratorFramework curatorFramework;
  private CuratorCnxnListener cnxnListener;
  private Participant currentLeader;

  private final String maxTsPath;
  private final String oraclePath;

  private volatile boolean isLeader = false;

  private GcTimestampTracker gcTsTracker;

  private class GcTimestampTracker {
    private volatile long advertisedGcTimetamp;
    private CuratorFramework curator;
    private Timer timer;

    GcTimestampTracker() throws Exception {
      this.curator = env.getSharedResources().getCurator();
    }

    private void updateAdvertisedGcTimestamp(long newTs) throws Exception {
      if (newTs > advertisedGcTimetamp && isLeader) {
        // set volatile var before setting in ZK in case Oracle dies... this ensures that client
        // making a request for timestamps see the new GC time before GC iters
        advertisedGcTimetamp = newTs;
        curator.setData().forPath(ZookeeperPath.ORACLE_GC_TIMESTAMP,
            LongUtil.toByteArray(advertisedGcTimetamp));
      }
    }

    private void updateGcTimestamp() throws Exception {
      List<String> children;
      try {
        children = curator.getChildren().forPath(ZookeeperPath.TRANSACTOR_TIMESTAMPS);
      } catch (NoNodeException nne) {
        children = Collections.emptyList();
      }

      long oldestTs = Long.MAX_VALUE;
      boolean nodeFound = false;

      for (String child : children) {
        Long ts = LongUtil.fromByteArray(
            curator.getData().forPath(ZookeeperPath.TRANSACTOR_TIMESTAMPS + "/" + child));
        nodeFound = true;
        if (ts < oldestTs) {
          oldestTs = ts;
        }
      }

      if (nodeFound) {
        updateAdvertisedGcTimestamp(oldestTs);
      } else {
        updateAdvertisedGcTimestamp(currentTs);
      }
    }

    void start() throws Exception {
      advertisedGcTimetamp =
          LongUtil.fromByteArray(curator.getData().forPath(ZookeeperPath.ORACLE_GC_TIMESTAMP));
      TimerTask tt = new TimerTask() {
        @Override
        public void run() {
          try {
            updateGcTimestamp();
          } catch (Exception e) {
            log.warn("Failed to update GC timestamp.", e);
          }
        }
      };
      TimerTask logTask = new TimerTask() {
        @Override
        public void run() {
          log.info("Current timestamp: {}", currentTs);
        }
      };

      timer = new Timer("Oracle gc update timer", true);
      long updatePeriod =
          env.getConfiguration().getLong(FluoConfigurationImpl.ZK_UPDATE_PERIOD_PROP,
              FluoConfigurationImpl.ZK_UPDATE_PERIOD_MS_DEFAULT);
      long nextPeriod = 5 * 60 * 1000L;
      timer.schedule(tt, updatePeriod, updatePeriod);
      timer.schedule(logTask, 0L, nextPeriod);
    }

    void stop() {
      if (timer != null) {
        timer.cancel();
        timer = null;
      }
    }
  }

  public OracleServer(Environment env) throws Exception {
    this.env = env;
    stampsHistogram = MetricsUtil.getHistogram(env.getConfiguration(),
        env.getSharedResources().getMetricRegistry(), env.getMetricNames().getOracleServerStamps());
    this.cnxnListener = new CuratorCnxnListener();
    this.maxTsPath = ZookeeperPath.ORACLE_MAX_TIMESTAMP;
    this.oraclePath = ZookeeperPath.ORACLE_SERVER;
  }

  private void allocateTimestamp() throws Exception {
    Stat stat = new Stat();
    byte[] d = curatorFramework.getData().storingStatIn(stat).forPath(maxTsPath);

    // TODO check that d is expected
    // TODO check that still server when setting
    // TODO make num allocated variable... when a server first starts allocate a small amount... the
    // longer it runs and the busier it is, allocate bigger blocks

    long newMax = Long.parseLong(new String(d)) + 1000;

    curatorFramework.setData().withVersion(stat.getVersion()).forPath(maxTsPath,
        LongUtil.toByteArray(newMax));
    maxTs = newMax;

    if (!isLeader) {
      throw new IllegalStateException();
    }
  }

  @Override
  public Stamps getTimestamps(String id, int num) throws TException {
    long start = getTimestampsImpl(id, num);

    // do this outside of sync
    stampsHistogram.update(num);

    return new Stamps(start, gcTsTracker.advertisedGcTimetamp);
  }

  private synchronized long getTimestampsImpl(String id, int num) throws TException {
    if (!started) {
      throw new IllegalStateException("Received timestamp request but Oracle has not started");
    }

    if (!id.equals(env.getFluoApplicationID())) {
      throw new IllegalArgumentException("Received timestamp request with a Fluo application ID ["
          + id + "] that does not match the application ID [" + env.getFluoApplicationID()
          + "] of the Oracle");
    }

    if (!isLeader) {
      throw new IllegalStateException("Received timestamp request but Oracle is not leader");
    }

    try {
      while (num + currentTs >= maxTs) {
        allocateTimestamp();
      }

      long tmp = currentTs;
      currentTs += num;

      return tmp;
    } catch (Exception e) {
      throw new TException(e);
    }
  }

  @Override
  public boolean isLeader() {
    return isLeader;
  }

  private boolean isLeader(Participant participant) {
    return participant != null && participant.isLeader();
  }

  @VisibleForTesting
  public int getPort() {
    return port;
  }

  @VisibleForTesting
  public boolean isConnected() {
    return (started && cnxnListener.isConnected());
  }

  private InetSocketAddress startServer() throws TTransportException {
    Preconditions.checkState(
        curatorFramework != null && curatorFramework.getState() == CuratorFrameworkState.STARTED);

    if (env.getConfiguration().containsKey(FluoConfigurationImpl.ORACLE_PORT_PROP)) {
      port = env.getConfiguration().getInt(FluoConfigurationImpl.ORACLE_PORT_PROP);
      Preconditions.checkArgument(port >= 1 && port <= 65535,
          FluoConfigurationImpl.ORACLE_PORT_PROP + " must be valid port (1-65535)");
    } else {
      port = PortUtils.getRandomFreePort();
    }
    InetSocketAddress addr = new InetSocketAddress(port);

    TNonblockingServerSocket socket = new TNonblockingServerSocket(addr);

    THsHaServer.Args serverArgs = new THsHaServer.Args(socket);
    TProcessor processor = new OracleService.Processor<OracleService.Iface>(this);
    serverArgs.processor(processor);
    serverArgs.maxReadBufferBytes = ORACLE_MAX_READ_BUFFER_BYTES;
    serverArgs.inputProtocolFactory(new TCompactProtocol.Factory());
    serverArgs.outputProtocolFactory(new TCompactProtocol.Factory());
    server = new THsHaServer(serverArgs);

    serverThread = new Thread(server::serve);
    serverThread.setDaemon(true);
    serverThread.start();

    return addr;
  }

  public synchronized void start() throws Exception {
    if (started) {
      throw new IllegalStateException();
    }

    curatorFramework = CuratorUtil.newAppCurator(env.getConfiguration());
    curatorFramework.getConnectionStateListenable().addListener(cnxnListener);
    curatorFramework.start();

    while (!cnxnListener.isConnected()) {
      UtilWaitThread.sleep(200);
    }

    final InetSocketAddress addr = startServer();

    String leaderId = HostUtil.getHostName() + ":" + addr.getPort();
    leaderLatch = new LeaderLatch(curatorFramework, ZookeeperPath.ORACLE_SERVER, leaderId);
    log.info("Leader ID = " + leaderId);
    execService = Executors.newSingleThreadExecutor(new FluoThreadFactory("Oracle Server Worker"));
    leaderLatch.addListener(new LeaderLatchListener() {
      @Override
      public void notLeader() {
        isLeader = false;

        if (started) {
          // if we stopped the server manually, we shouldn't halt
          Halt.halt("Oracle has lost leadership unexpectedly and is now halting.");
        }
      }

      @Override
      public void isLeader() {
        assumeLeadership();
      }
    }, execService);
    leaderLatch.start();

    pathChildrenCache = new PathChildrenCache(curatorFramework, oraclePath, true);
    pathChildrenCache.getListenable().addListener(this);
    pathChildrenCache.start();

    while (!cnxnListener.isConnected()) {
      UtilWaitThread.sleep(200);
    }

    log.info("Listening " + addr);

    started = true;
  }

  private void assumeLeadership() {
    Preconditions.checkState(!isLeader);

    // sanity check- make sure previous oracle is no longer listening for connections
    if (currentLeader != null) {
      String[] address = currentLeader.getId().split(":");
      String host = address[0];
      int port = Integer.parseInt(address[1]);

      OracleService.Client client = getOracleClient(host, port);
      if (client != null) {
        try {
          while (client.isLeader()) {
            Thread.sleep(500);
          }
        } catch (Exception e) {
          log.debug("Exception thrown in takeLeadership()", e);
        }
      }
    }

    try {
      synchronized (this) {
        byte[] d = curatorFramework.getData().forPath(maxTsPath);
        currentTs = maxTs = LongUtil.fromByteArray(d);
      }

      gcTsTracker = new GcTimestampTracker();
      gcTsTracker.start();

      isLeader = true;
      log.info("Assumed leadership " + leaderLatch.getId());
    } catch (Exception e) {
      log.warn("Failed to become leader ", e);
    }
  }

  public synchronized void stop() throws Exception {
    if (started) {

      isLeader = false;

      server.stop();
      serverThread.join();

      if (gcTsTracker != null) {
        gcTsTracker.stop();
      }

      started = false;

      currentLeader = null;
      if (curatorFramework.getState().equals(CuratorFrameworkState.STARTED)) {
        pathChildrenCache.getListenable().removeListener(this);
        pathChildrenCache.close();
        leaderLatch.close();

        execService.shutdown();

        execService.awaitTermination(10, TimeUnit.SECONDS);

        curatorFramework.getConnectionStateListenable().removeListener(cnxnListener);

        // leaderLatch.close() schedules a background delete, give it a chance to process before
        // closing curator... this is done to avoid spurious exceptions, see CURATOR-467
        Uninterruptibles.sleepUninterruptibly(250, TimeUnit.MILLISECONDS);
        curatorFramework.close();
      }
      log.info("Oracle server has been stopped.");
    }
  }

  private OracleService.Client getOracleClient(String host, int port) {
    try {
      TTransport transport = new TFastFramedTransport(new TSocket(host, port));
      transport.open();
      TProtocol protocol = new TCompactProtocol(transport);
      log.info("Former leader was reachable at " + host + ":" + port);
      return new OracleService.Client(protocol);
    } catch (TTransportException e) {
      log.debug("Exception thrown in getOracleClient()", e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return null;
  }

  @Override
  public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent event)
      throws Exception {

    try {
      if (isConnected() && (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)
          || event.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)
          || event.getType().equals(PathChildrenCacheEvent.Type.CHILD_UPDATED))) {
        synchronized (this) {
          Participant participant = leaderLatch.getLeader();
          if (isLeader(participant) && !leaderLatch.hasLeadership()) {
            // in case current instance becomes leader, we want to know who came before it.
            currentLeader = participant;
          }
        }
      }
    } catch (InterruptedException e) {
      log.warn("Oracle leadership watcher has been interrupted unexpectedly");
    }
  }

  @VisibleForTesting
  public void awaitLeaderElection(long timeout, TimeUnit timeUnit) throws InterruptedException {
    leaderLatch.await(timeout, timeUnit);
  }
}
