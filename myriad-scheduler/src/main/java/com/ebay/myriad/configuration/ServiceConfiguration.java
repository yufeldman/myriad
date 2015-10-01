/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.ebay.myriad.configuration;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;

/**
 * Configuration for any service/task to be started from Myriad Scheduler
 *
 */
public class ServiceConfiguration {
  
  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceConfiguration.class);

  public static final Double DEFAULT_CPU = 0.1;
  
  public static final Double DEFAULT_MEMORY = 16.0;
  
  /**
   * Translates to -Xmx for the JVM.
   */
  @JsonProperty
  protected Double jvmMaxMemoryMB;

  /**
   * Amount of CPU share given to  JVM. 
   */
  @JsonProperty
  protected Double cpus;

  /**
   * Translates to jvm opts for the JVM.
   */
  @JsonProperty
  protected String jvmOpts;

  @JsonProperty
  protected List<Long> ports;
  
  @JsonProperty
  protected String taskFactoryImplName;
  
  @JsonProperty
  protected String envSettings;
  
  @JsonProperty
  protected String taskName;
  
  @JsonProperty
  protected Integer maxInstances;
  
  
  public Optional<Double> getJvmMaxMemoryMB() {
      return Optional.fromNullable(jvmMaxMemoryMB);
  }

  public Optional<String> getJvmOpts() {
      return Optional.fromNullable(jvmOpts);
  }

  public Optional<Double> getCpus() {
      return Optional.fromNullable(cpus);
  }

  public String getTaskName() {
    return taskName;
  }

  public void setTaskName(String taskName) {
    this.taskName = taskName;
  }

  public String getTaskFactoryImplName() {
    return taskFactoryImplName;
  }

  public String getEnvSettings() {
    return envSettings;
  }
  
  public List<Long> getPorts() {
    return ports;
  }
  
  public Optional<Integer> getMaxInstances() {
    return Optional.fromNullable(maxInstances);
  }

}
