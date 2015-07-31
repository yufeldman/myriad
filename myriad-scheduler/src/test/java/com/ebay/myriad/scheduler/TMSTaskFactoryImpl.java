package com.ebay.myriad.scheduler;

import javax.inject.Inject;

import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;

import com.ebay.myriad.configuration.MyriadConfiguration;
import com.ebay.myriad.state.NodeTask;

/**
 * Test implementation of TaskFactory
 *
 */
public class TMSTaskFactoryImpl implements TaskFactory {

  private MyriadConfiguration cfg;
  private TaskUtils taskUtils;
  private TaskConstraints constraints;

  @Inject
  public TMSTaskFactoryImpl(MyriadConfiguration cfg, TaskUtils taskUtils) {
      this.setCfg(cfg);
      this.setTaskUtils(taskUtils);
      this.constraints = new TaskConstraints() {

        @Override
        public int portsCount() {
          return 0;
        }};
  }

  @Override
  public TaskInfo createTask(Offer offer, TaskID taskId, NodeTask nodeTask) {
    return null;
  }

  public MyriadConfiguration getCfg() {
    return cfg;
  }

  public void setCfg(MyriadConfiguration cfg) {
    this.cfg = cfg;
  }

  public TaskUtils getTaskUtils() {
    return taskUtils;
  }

  public void setTaskUtils(TaskUtils taskUtils) {
    this.taskUtils = taskUtils;
  }

  @Override
  public ExecutorInfo getExecutorInfoForSlave(SlaveID slave) {
    return null;
  }

  @Override
  public TaskConstraints getConstraints() {
    return constraints;
  }

}
