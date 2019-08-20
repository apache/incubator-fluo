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

package org.apache.fluo.cluster.runnable;

import java.io.File;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.service.FluoOracle;
import org.apache.fluo.core.metrics.MetricNames;
import org.apache.fluo.core.oracle.FluoOracleImpl;
import org.apache.fluo.core.util.UtilWaitThread;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Run method of Fluo oracle that is called within a Twill/YARN application
 */
@Deprecated
public class OracleRunnable extends AbstractTwillRunnable {

  private static final Logger log = LoggerFactory.getLogger(OracleRunnable.class);
  public static String ORACLE_NAME = "FluoOracle";
  private AtomicBoolean shutdown = new AtomicBoolean(false);
  private static final String STDOUT = "STDOUT";

  @Override
  public void run() {
    System.out.println("Starting Oracle");
    String configDir = "./conf";
    String propsPath = configDir + "/fluo.properties";
    Objects.requireNonNull(propsPath);
    File propsFile = new File(propsPath);
    if (!propsFile.exists()) {
      System.err.println("ERROR - Fluo properties file does not exist: " + propsPath);
      System.exit(-1);
    }
    String logDir = System.getenv("LOG_DIRS");
    if (logDir == null) {
      System.err
          .println("LOG_DIRS env variable was not set by Twill.  Logging to console instead!");
      logDir = STDOUT;
    }

    try {
      if (!logDir.equals(STDOUT)) {
        org.apache.fluo.cluster.util.LogbackUtil.init("oracle", configDir, logDir);
      }
    } catch (Exception e) {
      System.err.println("Exception while starting FluoOracle: " + e.getMessage());
      log.error("Exception while starting FluoOracle: {}", e.getMessage(), e);
      System.exit(-1);
    }

    try {
      FluoConfiguration config = new FluoConfiguration(propsFile);

      TwillContext context = getContext();
      if (context != null && System.getProperty(MetricNames.METRICS_REPORTER_ID_PROP) == null) {
        System.setProperty(MetricNames.METRICS_REPORTER_ID_PROP,
            "oracle-" + context.getInstanceId());
      }

      // FluoFactory cannot be used to create FluoOracle as Twill will not load its dependencies
      // if it is loaded dynamically
      FluoOracle oracle = new FluoOracleImpl(config);
      oracle.start();
      while (!shutdown.get()) {
        UtilWaitThread.sleep(10000);
      }
      oracle.stop();
    } catch (Exception e) {
      log.error("Exception running FluoOracle: ", e);
    }

    log.info("FluoOracle is exiting.");
  }

  @Override
  public void stop() {
    log.info("Stopping Fluo oracle");
    shutdown.set(true);
  }
}
