/**
 * Copyright 2015 PayPal
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.ebay.myriad.state;

import com.ebay.myriad.scheduler.ServiceResourceProfile;
import com.ebay.myriad.scheduler.TaskUtils;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Inject;

import org.apache.mesos.Protos;

/**
 * Represents a task to be launched by the executor
 */
public class NodeTask {
    @JsonProperty
    private String hostname;
    @JsonProperty
    private Protos.SlaveID slaveId;
    @JsonProperty
    private Protos.TaskStatus taskStatus;
    @JsonProperty
    private String taskPrefix;
    @JsonProperty
    private ServiceResourceProfile serviceresourceProfile;

    @Inject
    TaskUtils taskUtils;
    /**
     * Mesos executor for this node.
     */
    private Protos.ExecutorInfo executorInfo;

    public NodeTask(ServiceResourceProfile profile) {
      this.serviceresourceProfile = profile;
      this.hostname = "";
    }

    public Protos.SlaveID getSlaveId() {
        return slaveId;
    }

    public void setSlaveId(Protos.SlaveID slaveId) {
        this.slaveId = slaveId;
    }

    public String getHostname() {
        return this.hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public Protos.TaskStatus getTaskStatus() {
        return taskStatus;
    }

    public void setTaskStatus(Protos.TaskStatus taskStatus) {
        this.taskStatus = taskStatus;
    }

    public Protos.ExecutorInfo getExecutorInfo() {
        return executorInfo;
    }

    public void setExecutorInfo(Protos.ExecutorInfo executorInfo) {
        this.executorInfo = executorInfo;
    }

    public String getTaskPrefix() {
      return taskPrefix;
    }

    public void setTaskPrefix(String taskPrefix) {
      this.taskPrefix = taskPrefix;
    }

    public ServiceResourceProfile getProfile() {
      return serviceresourceProfile;
    }

    public void setProfile(ServiceResourceProfile serviceresourceProfile) {
      this.serviceresourceProfile = serviceresourceProfile;
    }
}
