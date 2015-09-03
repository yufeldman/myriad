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
package com.ebay.myriad.scheduler;

import com.ebay.myriad.configuration.AuxTaskConfiguration;
import com.ebay.myriad.configuration.MyriadBadConfigurationException;
import com.ebay.myriad.configuration.MyriadConfiguration;
import com.ebay.myriad.configuration.NodeManagerConfiguration;
import com.ebay.myriad.policy.NodeScaleDownPolicy;
import com.ebay.myriad.state.NodeTask;
import com.ebay.myriad.state.SchedulerState;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Myriad scheduler operations
 */
public class MyriadOperations {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyriadOperations.class);
    private final SchedulerState schedulerState;
    private MyriadConfiguration cfg;
    private NMProfileManager profileManager;
    private NodeScaleDownPolicy nodeScaleDownPolicy;

    @Inject
    public MyriadOperations(MyriadConfiguration cfg,
                            SchedulerState schedulerState,
                            NMProfileManager profileManager,
                            NodeScaleDownPolicy nodeScaleDownPolicy) {
        this.cfg = cfg;
        this.schedulerState = schedulerState;
        this.profileManager = profileManager;
        this.nodeScaleDownPolicy = nodeScaleDownPolicy;
    }

    public void flexUpCluster(int instances, String profile) {
        Collection<NodeTask> nodes = new HashSet<>();
        for (int i = 0; i < instances; i++) {
          NodeTask nodeTask = new NodeTask(profileManager.get(profile));
          nodeTask.setTaskPrefix(NodeManagerConfiguration.NM_TASK_PREFIX);
          nodes.add(nodeTask);
        }

        LOGGER.info("Adding {} NM instances to cluster", nodes.size());
        this.schedulerState.addNodes(nodes);
    }

    /**
     * Flexup a service
     * @param instances
     * @param profileName
     */
    public void flexUpAService(int instances, String profileName) throws MyriadBadConfigurationException {
      final NMProfile nmProfile;
      if (profileName == null || (nmProfile = profileManager.get(profileName)) == null) {
        throw new MyriadBadConfigurationException("Specified profile is invalid: " + profileName);
      }
      
      AuxTaskConfiguration auxTaskConf = cfg.getAuxTaskConfiguration(profileName);
      int totalflexInstances = instances + getFlexibleInstances(profileName);
      
      Integer maxInstances = auxTaskConf.getMaxInstances().orNull();
      if (maxInstances != null && maxInstances > 0) {
        // check number of instances
        // sum of active, staging, pending should be < maxInstances
        if (totalflexInstances > maxInstances) {
          LOGGER.error("Current number of active, staging, pending and requested instances: {}"
              + ", while it is greater then max instances allowed: {}", totalflexInstances, maxInstances);
          throw new MyriadBadConfigurationException("Current number of active, staging, pending instances and requested: "
              + totalflexInstances + ", while it is greater then max instances allowed: " + maxInstances);          
        }
      }

      Collection<NodeTask> nodes = new HashSet<>();
      for (int i = 0; i < instances; i++) {
        NodeTask nodeTask = new NodeTask(nmProfile);
        nodeTask.setTaskPrefix(profileName);
        nodes.add(nodeTask);
      }

      LOGGER.info("Adding {} {} instances to cluster", nodes.size(), profileName);
      this.schedulerState.addNodes(nodes);
    }
    
    /**
     * Flexing down any service defined in the configuration
     * @param numInstancesToScaleDown
     * @param serviceName - name of the service
     */
    public void flexDownAService(int numInstancesToScaleDown, String serviceName) throws MyriadBadConfigurationException {
      LOGGER.info("About to flex down {} instances of {}", numInstancesToScaleDown, serviceName);

      int numScaledDown = 0;
      Set<NodeTask> activeTasks = Sets.newHashSet(this.schedulerState.getActiveTasksByType(serviceName));

      for (NodeTask nodeTask : activeTasks) {
        this.schedulerState.makeTaskKillable(nodeTask.getTaskStatus().getTaskId());
        numScaledDown++;
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Marked NodeTask {} on host {} for kill.",
              nodeTask.getTaskStatus().getTaskId(), nodeTask.getHostname());
        }
        if (numScaledDown >= numInstancesToScaleDown) {
          break;
        }
      }
      
      int numActiveTasksScaledDown = numScaledDown;

      // Flex down Staging tasks, if any
      if (numScaledDown < numInstancesToScaleDown) {
          Set<Protos.TaskID> stagingTasks = Sets.newHashSet(this.schedulerState.getStagingTaskIds(serviceName));

          for (Protos.TaskID taskId : stagingTasks) {
              this.schedulerState.makeTaskKillable(taskId);
              numScaledDown++;
              if (numScaledDown == numInstancesToScaleDown) {
                  break;
              }
          }
      }
      int numStagingTasksScaledDown = numScaledDown - numActiveTasksScaledDown;

      // Flex down Pending tasks, if any
      if (numScaledDown < numInstancesToScaleDown) {
          Set<Protos.TaskID> pendingTasks = Sets.newHashSet(this.schedulerState.getPendingTaskIds(serviceName));

          for (Protos.TaskID taskId : pendingTasks) {
              this.schedulerState.makeTaskKillable(taskId);
              numScaledDown++;
              if (numScaledDown == numInstancesToScaleDown) {
                  break;
              }
          }
      }
      int numPendingTasksScaledDown = numScaledDown - numStagingTasksScaledDown;

      LOGGER.info("Flexed down {} of {} instances including {} staging instances, and {} pending instances of {}",
              numScaledDown, numInstancesToScaleDown, numStagingTasksScaledDown, numPendingTasksScaledDown, serviceName);
    }
    
    public void flexDownCluster(int numInstancesToScaleDown) {
        LOGGER.info("About to flex down {} instances", numInstancesToScaleDown);

        int numScaledDown = 0;
        Set<NodeTask> activeTasks = Sets.newHashSet(this.schedulerState.getActiveTasksByType(NodeManagerConfiguration.NM_TASK_PREFIX));
        List<String> nodesToScaleDown = nodeScaleDownPolicy.getNodesToScaleDown();
        if (activeTasks.size() > nodesToScaleDown.size()) {
            LOGGER.info("Will skip flexing down {} Node Manager instances that were launched but " +
                    "have not yet registered with Resource Manager.", activeTasks.size() - nodesToScaleDown.size());
        }
        
        // If a NM is flexed down it takes time for the RM to realize the NM is no longer up
        // We need to make sure we filter out nodes that have already been flexed down
        // but have not disappeared from the RM's view of the cluster
        for (Iterator<String> iterator = nodesToScaleDown.iterator(); iterator.hasNext();) {
            String nodeToScaleDown = iterator.next();
            boolean nodePresentInMyriad = false;
            for (NodeTask nodeTask : activeTasks) {
                if (nodeTask.getHostname().equals(nodeToScaleDown)) {
                    nodePresentInMyriad = true;    
                    break;
                }
            }
            if (!nodePresentInMyriad) {
                iterator.remove();
            }
        }

        // TODO(Santosh): Make this more efficient by using a Map<HostName, NodeTask> in scheduler state
        for (int i = 0; i < numInstancesToScaleDown; i++) {
            for (NodeTask nodeTask : activeTasks) {
                if (nodesToScaleDown.size() > i && nodesToScaleDown.get(i).equals(nodeTask.getHostname())) {
                    this.schedulerState.makeTaskKillable(nodeTask.getTaskStatus().getTaskId());
                    numScaledDown++;
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Marked NodeTask {} on host {} for kill.",
                                nodeTask.getTaskStatus().getTaskId(), nodeTask.getHostname());
                    }
                }
            }
        }
        int numActiveTasksScaledDown = numScaledDown;

        // Flex down Staging tasks, if any
        if (numScaledDown < numInstancesToScaleDown) {
            Set<Protos.TaskID> stagingTasks = Sets.newHashSet(this.schedulerState.getStagingTaskIds(NodeManagerConfiguration.NM_TASK_PREFIX));

            for (Protos.TaskID taskId : stagingTasks) {
                this.schedulerState.makeTaskKillable(taskId);
                numScaledDown++;
                if (numScaledDown == numInstancesToScaleDown) {
                    break;
                }
            }
        }
        int numStagingTasksScaledDown = numScaledDown - numActiveTasksScaledDown;

        // Flex down Pending tasks, if any
        if (numScaledDown < numInstancesToScaleDown) {
            Set<Protos.TaskID> pendingTasks = Sets.newHashSet(this.schedulerState.getPendingTaskIds(NodeManagerConfiguration.NM_TASK_PREFIX));

            for (Protos.TaskID taskId : pendingTasks) {
                this.schedulerState.makeTaskKillable(taskId);
                numScaledDown++;
                if (numScaledDown == numInstancesToScaleDown) {
                    break;
                }
            }
        }
        int numPendingTasksScaledDown = numScaledDown - numStagingTasksScaledDown;

        LOGGER.info("Flexed down {} of {} instances including {} staging instances, and {} pending instances.",
                numScaledDown, numInstancesToScaleDown, numStagingTasksScaledDown, numPendingTasksScaledDown);
    }
    
    public Integer getFlexibleInstances(String taskPrefix) {
      return this.schedulerState.getActiveTaskIds(taskPrefix).size()
              + this.schedulerState.getStagingTaskIds(taskPrefix).size()
              + this.schedulerState.getPendingTaskIds(taskPrefix).size();
  }

}
