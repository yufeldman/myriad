package com.ebay.myriad;

import java.util.Map;

import javax.inject.Inject;

import com.ebay.myriad.scheduler.TaskFactory;

/**
 * Helper class to test multibindings
 *
 */
public class MultiBindingsUsage {

  @Inject
  private Map<String, TaskFactory> taskFactoryMap;

  public Map<String, TaskFactory> getMap() {
    return taskFactoryMap;
  }
}
