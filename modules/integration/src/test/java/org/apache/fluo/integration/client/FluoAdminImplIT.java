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

package org.apache.fluo.integration.client;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.curator.framework.CuratorFramework;
import org.apache.fluo.accumulo.util.ColumnConstants;
import org.apache.fluo.accumulo.util.ZookeeperUtil;
import org.apache.fluo.api.client.FluoAdmin;
import org.apache.fluo.api.client.FluoAdmin.AlreadyInitializedException;
import org.apache.fluo.api.client.FluoAdmin.InitializationOptions;
import org.apache.fluo.api.client.FluoAdmin.TableExistsException;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.exceptions.FluoException;
import org.apache.fluo.core.client.FluoAdminImpl;
import org.apache.fluo.core.client.FluoClientImpl;
import org.apache.fluo.core.util.CuratorUtil;
import org.apache.fluo.core.util.OracleServerUtils;
import org.apache.fluo.integration.ITBaseImpl;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FluoAdminImplIT extends ITBaseImpl {
  @Rule
  public Timeout globalTimeout = Timeout.seconds(getTestTimeout());

  @Test
  public void testInitializeTwiceFails() throws Exception {

    // stop oracle to avoid spurious exceptions when initializing
    oserver.stop();

    try (FluoAdmin admin = new FluoAdminImpl(config)) {

      InitializationOptions opts =
          new InitializationOptions().setClearZookeeper(true).setClearTable(true);

      admin.initialize(opts);
      admin.initialize(opts);

      opts.setClearZookeeper(false).setClearTable(false);
      try {
        admin.initialize(opts);
        fail("This should have failed");
      } catch (AlreadyInitializedException e) {
      }

      opts.setClearZookeeper(false).setClearTable(true);
      try {
        admin.initialize(opts);
        fail("This should have failed");
      } catch (AlreadyInitializedException e) {
      }

      opts.setClearZookeeper(true).setClearTable(false);
      try {
        admin.initialize(opts);
        fail("This should have failed");
      } catch (TableExistsException e) {
      }
    }

    assertTrue(conn.tableOperations().exists(config.getAccumuloTable()));
  }

  @Test
  public void testInitializeConfig() throws Exception {

    // stop oracle to avoid spurious exceptions when initializing
    oserver.stop();

    FluoConfiguration localConfig = new FluoConfiguration(config);
    localConfig.setProperty("fluo.test123", "${fluo.connection.application.name}");
    Assert.assertEquals(localConfig.getApplicationName(), localConfig.getString("fluo.test123"));

    try (FluoAdmin admin = new FluoAdminImpl(localConfig)) {

      InitializationOptions opts =
          new InitializationOptions().setClearZookeeper(true).setClearTable(true);
      admin.initialize(opts);

      // verify locality groups were set on the table
      Instance inst =
          new ZooKeeperInstance(config.getAccumuloInstance(), config.getAccumuloZookeepers());
      Connector conn = inst.getConnector(config.getAccumuloUser(),
          new PasswordToken(config.getAccumuloPassword()));
      Map<String, Set<Text>> localityGroups =
          conn.tableOperations().getLocalityGroups(config.getAccumuloTable());
      Assert.assertEquals("Unexpected locality group count.", 1, localityGroups.size());
      Entry<String, Set<Text>> localityGroup = localityGroups.entrySet().iterator().next();
      Assert.assertEquals("'notify' locality group not found.",
          ColumnConstants.NOTIFY_LOCALITY_GROUP_NAME, localityGroup.getKey());
      Assert.assertEquals("'notify' locality group does not contain exactly 1 column family.", 1,
          localityGroup.getValue().size());
      Text colFam = localityGroup.getValue().iterator().next();
      Assert.assertTrue("'notify' locality group does not contain the correct column family.",
          ColumnConstants.NOTIFY_CF.contentEquals(colFam.getBytes(), 0, colFam.getLength()));
    }

    try (FluoClientImpl client = new FluoClientImpl(localConfig)) {
      FluoConfiguration sharedConfig = client.getSharedConfiguration();
      Assert.assertEquals(localConfig.getApplicationName(), sharedConfig.getString("fluo.test123"));
      Assert.assertEquals(localConfig.getApplicationName(), sharedConfig.getApplicationName());
    }
  }

  @Test
  public void testInitializeWithNoChroot() throws Exception {

    // stop oracle to avoid spurious exceptions when initializing
    oserver.stop();

    InitializationOptions opts =
        new InitializationOptions().setClearZookeeper(true).setClearTable(true);

    for (String host : new String[] {"localhost", "localhost/", "localhost:9999",
        "localhost:9999/"}) {
      config.setInstanceZookeepers(host);
      try (FluoAdmin fluoAdmin = new FluoAdminImpl(config)) {
        fluoAdmin.initialize(opts);
        fail("This should have failed");
      } catch (IllegalArgumentException e) {
      }
    }
  }

  @Test
  public void testInitializeLongChroot() throws Exception {

    // stop oracle to avoid spurious exceptions when initializing
    oserver.stop();

    String zk = config.getAppZookeepers();
    String longPath = "/very/long/path";
    config.setInstanceZookeepers(zk + longPath);

    InitializationOptions opts = new InitializationOptions();
    opts.setClearZookeeper(true).setClearTable(true);

    try (FluoAdmin admin = new FluoAdminImpl(config)) {
      admin.initialize(opts);
    }

    try (CuratorFramework curator = CuratorUtil.newRootFluoCurator(config)) {
      curator.start();
      Assert.assertNotNull(curator.checkExists().forPath(ZookeeperUtil.parseRoot(zk + longPath)));
    }

    String longPath2 = "/very/long/path2";
    config.setInstanceZookeepers(zk + longPath2);

    try (FluoAdmin admin = new FluoAdminImpl(config)) {
      admin.initialize(opts);
    }

    try (CuratorFramework curator = CuratorUtil.newRootFluoCurator(config)) {
      curator.start();
      Assert.assertNotNull(curator.checkExists().forPath(ZookeeperUtil.parseRoot(zk + longPath2)));
      Assert.assertNotNull(curator.checkExists().forPath(ZookeeperUtil.parseRoot(zk + longPath)));
    }
  }


  @Test
  public void testRemove() throws Exception {


    // oserver is started before every test so oserver is running
    try (FluoAdmin admin = new FluoAdminImpl(config)) {
      admin.remove();
      fail("expected remove() to fail with oracle server running");
    } catch (FluoException e) {
    }

    // TODO assert null or something below instead of sys.out
    oserver.stop();
    try (CuratorFramework curator = CuratorUtil.newAppCurator(config)) {
      curator.start();
      System.out.println("Does the oracle exist now?: " + OracleServerUtils.oracleExists(curator)
          + " (it shouldnt)");
    }
    // this should succeed now with the oracle server stopped
    try (FluoAdmin admin = new FluoAdminImpl(config)) {
      admin.remove();
    } catch (FluoException e) {
    }

    // should succeed without clearing anything
    try (FluoAdmin admin = new FluoAdminImpl(config)) {
      InitializationOptions opts =
          new InitializationOptions().setClearTable(false).setClearZookeeper(false);
      admin.initialize(opts);
    } catch (FluoException e) {

    }

    oserver.start();
    try (CuratorFramework curator = CuratorUtil.newAppCurator(config)) {
      curator.start();
      String appRootDir = ZookeeperUtil.parseRoot(config.getAppZookeepers());
      System.out.println("Does the oracle exist?: " + OracleServerUtils.oracleExists(curator));
      System.out.println(
          "Does the app directory exist in ZK?: " + CuratorUtil.pathExist(curator, appRootDir));
    }
    System.out.println("Current leading oracle: " + OracleServerUtils.getLeadingOracle(config));

    // write some data into the table and test remove again
    BatchWriter writer = conn.createBatchWriter(getCurTableName(), new BatchWriterConfig());
    Mutation mutation = new Mutation("id0001");
    mutation.put("hero", "alias", "Batman");
    mutation.put("hero", "name", "Bruce Wayne");
    mutation.put("hero", "wearsCape?", "true");

    writer.addMutation(mutation);
    writer.close();

    Scanner scan = conn.createScanner(getCurTableName(), Authorizations.EMPTY);

    int count = 0;
    for (Entry<Key, Value> entry : scan) {
      System.out.println("Entry number " + ++count + " with key " + entry.getKey() + " with value "
          + entry.getValue());
    }
    System.out.println("Found " + count + " entries in the table, expected 3");
    scan.close();

    oserver.stop();

    try (FluoAdmin admin = new FluoAdminImpl(config)) {
      admin.remove();
      InitializationOptions opts =
          new InitializationOptions().setClearTable(false).setClearZookeeper(false);
      admin.initialize(opts);
    } catch (FluoException e) {
    }

    oserver.start();
    count = 0; // reset the count
    scan = conn.createScanner(getCurTableName(), Authorizations.EMPTY);
    for (Entry<Key, Value> entry : scan) {
      System.out.println("Entry number " + ++count + " with key " + entry.getKey() + " with value "
          + entry.getValue());
    }
    scan.close();
    System.out.println("Found " + count + " entries in the table, expect 0");
    // TODO assert the count is zero, remove sys.out statements

  }
}
