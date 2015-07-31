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
public class AuxTaskConfiguration {
  
  private static final Logger LOGGER = LoggerFactory.getLogger(AuxTaskConfiguration.class);

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
}
