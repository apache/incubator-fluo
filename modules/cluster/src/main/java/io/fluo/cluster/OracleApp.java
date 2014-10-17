/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fluo.cluster;

import java.io.File;

import io.fluo.api.config.FluoConfiguration;
import io.fluo.cluster.util.ClusterUtil;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.ResourceSpecification.SizeUnit;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.TwillSpecification.Builder.MoreFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents Fluo oracle application in Twill
 */
public class OracleApp implements TwillApplication {
  
  private static final Logger log = LoggerFactory.getLogger(OracleApp.class);
  
  private final FluoConfiguration config;
  private final String fluoHome;
  
  public OracleApp(FluoConfiguration config, String fluoHome) {
    this.config = config;
    this.fluoHome = fluoHome;
  }
       
  @Override
  public TwillSpecification configure() {   
    int maxMemoryMB = config.getOracleMaxMemory();
    
    log.info("Configuring "+config.getOracleInstances()+" Fluo oracles with " + maxMemoryMB + "MB of memory per instance.");
    
    ResourceSpecification oracleResources = ResourceSpecification.Builder.with()
        .setVirtualCores(1)
        .setMemory(maxMemoryMB, SizeUnit.MEGA)
        .setInstances(config.getOracleInstances()).build();

    MoreFile moreFile = TwillSpecification.Builder.with() 
        .setName(ClusterUtil.ORACLE_APP_NAME).withRunnable()
        .add(new OracleRunnable(), oracleResources)
        .withLocalFiles()
        .add("./conf/fluo.properties", new File(String.format("%s/conf/fluo.properties", fluoHome)));

    File confDir = new File(String.format("%s/conf", fluoHome));
    for (File f : confDir.listFiles()) {
      if (f.isFile() && (f.getName().equals("fluo.properties") == false)) {
        log.trace("Adding config file - " + f.getName());
        moreFile = moreFile.add(String.format("./conf/%s", f.getName()), f);
      }
    }

    return moreFile.apply().anyOrder().build();
  }
}
