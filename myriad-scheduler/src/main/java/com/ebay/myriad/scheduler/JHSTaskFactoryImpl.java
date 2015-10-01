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

package com.ebay.myriad.scheduler;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.inject.Inject;

import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.Value;
import org.apache.mesos.Protos.CommandInfo.URI;
import org.apache.mesos.Protos.Value.Scalar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.myriad.configuration.ServiceConfiguration;
import com.ebay.myriad.configuration.MyriadConfiguration;
import com.ebay.myriad.configuration.MyriadExecutorConfiguration;
import com.ebay.myriad.state.NodeTask;
import com.google.common.annotations.VisibleForTesting;

  /**
   * Task creation class for JobHistoryServer
   * TODO (yfeldman) if we add attributes on slaves that can run JHS
   * we can also restrict where JHS can be started
   */
    public class JHSTaskFactoryImpl implements TaskFactory {
      private static final Logger LOGGER = LoggerFactory.getLogger(JHSTaskFactoryImpl.class);
      
      private static final String[] JHS_ADDRESSES = new String[] {
        "myriad.mapreduce.jobhistory.admin.address",
        "myriad.mapreduce.jobhistory.address",
        "myriad.mapreduce.jobhistory.webapp.address"
      };
      
      
      private static final int JHS_PORTS_COUNT = 3;

      private MyriadConfiguration cfg;
      @SuppressWarnings("unused")
      private TaskUtils taskUtils;
      private TaskConstraints constraints;
      private ServiceCommandLineGenerator clGenerator;

      @Inject
      public JHSTaskFactoryImpl(MyriadConfiguration cfg, TaskUtils taskUtils) {
          this.cfg = cfg;
          this.taskUtils = taskUtils;
          this.constraints = new JHSTaskConstraints(cfg);
          this.clGenerator = new ServiceCommandLineGenerator(cfg, cfg.getMyriadExecutorConfiguration().getNodeManagerUri().orNull());
      }
      @Override
      public TaskInfo createTask(Offer offer, FrameworkID frameworkId, TaskID taskId, NodeTask nodeTask) {
        Objects.requireNonNull(offer, "Offer should be non-null");
        Objects.requireNonNull(nodeTask, "NodeTask should be non-null");

        ServiceConfiguration serviceConfig = 
            cfg.getServiceConfiguration(nodeTask.getTaskPrefix());
        
        if (serviceConfig == null) {
          return null;
        }
        
        final String jhsHostName = "0.0.0.0";
        final String jsEnv = serviceConfig.getEnvSettings();
        final String rmHostName = System.getProperty(YARN_RESOURCEMANAGER_HOSTNAME);
        
        final StringBuilder strB = new StringBuilder("env && export HADOOP_JOB_HISTORYSERVER_OPTS=");
        strB.append("\"");
        if (rmHostName != null && !rmHostName.isEmpty()) {
          strB.append("-D" + YARN_RESOURCEMANAGER_HOSTNAME + "=" + rmHostName + " ");
        }
        List<Long> ports = serviceConfig.getPorts();
        final boolean useOffersPorts;
        if (ports == null || ports.size() < JHS_PORTS_COUNT) {
          // use provided ports
          useOffersPorts = true;
          ports = getAvailablePorts(offer, JHS_PORTS_COUNT);
          LOGGER.info("No specified ports found or number of specified ports is not enough. Using ports from Mesos Offers: {}", ports);
        } else {
          useOffersPorts = false;
          LOGGER.info("Using ports from configuration: {}", ports);
        }
        
        int i = 0;
        for (Long port : ports) {
          strB.append("-D" + JHS_ADDRESSES[i++] + "=" + jhsHostName + ":" + port + " ");
        }
        
        strB.append(jsEnv);
        strB.append("\"");
        strB.append(" && $YARN_HOME/bin/mapred historyserver");
        
        CommandInfo commandInfo = createCommandInfo(nodeTask.getProfile(), strB.toString());

        LOGGER.info("Command line for JHS: {}", strB.toString());
        
        Scalar taskMemory = Scalar.newBuilder()
            .setValue(nodeTask.getProfile().getMemory())
            .build();
        Scalar taskCpus = Scalar.newBuilder()
            .setValue(nodeTask.getProfile().getCpus())
            .build();

        TaskInfo.Builder taskBuilder = TaskInfo.newBuilder();
        
        taskBuilder.setName(nodeTask.getTaskPrefix())
            .setTaskId(taskId)
            .setSlaveId(offer.getSlaveId())
            .addResources(
                Resource.newBuilder().setName("cpus")
                .setType(Value.Type.SCALAR)
                .setScalar(taskCpus)
                .build())
            .addResources(
                Resource.newBuilder().setName("mem")
                .setType(Value.Type.SCALAR)
                .setScalar(taskMemory)
                .build());
        
        if (useOffersPorts) {
          // set ports
          Value.Ranges.Builder valueRanger = Value.Ranges.newBuilder();
          for (Long port : ports) {
            valueRanger.addRange(Value.Range.newBuilder()
                    .setBegin(port)
                    .setEnd(port));
          }
          
          taskBuilder.addResources(Resource.newBuilder().setName("ports")
              .setType(Value.Type.RANGES)
              .setRanges(valueRanger.build()));
        }
        taskBuilder.setCommand(commandInfo);
        return taskBuilder.build();
      }
      
      @VisibleForTesting
      CommandInfo createCommandInfo(ServiceResourceProfile profile, String executorCmd) {
        MyriadExecutorConfiguration myriadExecutorConfiguration = cfg.getMyriadExecutorConfiguration();
        CommandInfo.Builder commandInfo = CommandInfo.newBuilder();
        String yarnHomeEnv = cfg.getYarnEnvironment().get("YARN_HOME");
        org.apache.mesos.Protos.Environment.Variable.Builder yarnEnvB = 
            org.apache.mesos.Protos.Environment.Variable.newBuilder();
        yarnEnvB.setName("YARN_HOME").setValue(yarnHomeEnv);
        org.apache.mesos.Protos.Environment.Builder yarnHomeB = 
            org.apache.mesos.Protos.Environment.newBuilder();
        yarnHomeB.addVariables(yarnEnvB.build());
        commandInfo.mergeEnvironment(yarnHomeB.build());

        if (myriadExecutorConfiguration.getNodeManagerUri().isPresent()) {
          //Both FrameworkUser and FrameworkSuperuser to get all of the directory permissions correct.
          if (!(cfg.getFrameworkUser().isPresent() && cfg.getFrameworkSuperUser().isPresent())) {
            throw new RuntimeException("Trying to use remote distribution, but frameworkUser" +
                "and/or frameworkSuperUser not set!");
          }

          LOGGER.info("Using remote distribution");
          String clGeneratedCommand = clGenerator.generateCommandLine(profile, null);
          
          String nmURIString = myriadExecutorConfiguration.getNodeManagerUri().get();

          //Concatenate all the subcommands
          String cmd = clGeneratedCommand + " " + executorCmd;

          //get the nodemanagerURI
          //We're going to extract ourselves, so setExtract is false
          LOGGER.info("Getting Hadoop distribution from:" + nmURIString);
          URI nmUri = URI.newBuilder().setValue(nmURIString).setExtract(false)
              .build();

          //get configs directly from resource manager
          String configUrlString = clGenerator.getConfigurationUrl();
          LOGGER.info("Getting config from:" + configUrlString);
          URI configUri = URI.newBuilder().setValue(configUrlString)
              .build();

          LOGGER.info("Slave will execute command:" + cmd);
          commandInfo.addUris(nmUri).addUris(configUri).setValue("echo \"" + cmd + "\";" + cmd);
          commandInfo.setUser(cfg.getFrameworkSuperUser().get());

        } else {
          commandInfo.setValue(executorCmd);
        }
        return commandInfo.build();
      }
      
      /**
       * Helper method to reserve ports
       * @param offer
       * @param requestedPorts
       * @return
       */
      private List<Long> getAvailablePorts(Offer offer, int requestedPorts) {
        if (requestedPorts == 0) {
          return null;
        }
        final List<Long> returnedPorts = new ArrayList<>();
        for (Resource resource : offer.getResourcesList()){
          if (resource.getName().equals("ports")){
            Iterator<Value.Range> itr = resource.getRanges().getRangeList().iterator();
            while (itr.hasNext()) {
              Value.Range range = itr.next();
              if (range.getBegin() <= range.getEnd()) {
                long i = range.getBegin();
                while (i <= range.getEnd() && returnedPorts.size() < requestedPorts) {
                  returnedPorts.add(i);
                  i++;
                }
                if (returnedPorts.size() >= requestedPorts) { 
                  return returnedPorts;
                }
              }
            }
          }
        }
        // this is actually an error condition - we did not have enough ports to use
        return returnedPorts;
      }

      @Override
      public ExecutorInfo getExecutorInfoForSlave(FrameworkID frameworkId, Offer offer,
          CommandInfo commandInfo) {
        // nothing to implement here, since we are using default slave executor
        return null;
      }
      @Override
      public TaskConstraints getConstraints() {
        return constraints;
      }
      
      
      /**
       * Implement NM Task Constraints
       *
       */
      static class JHSTaskConstraints implements TaskConstraints {

        private int portsCount;
        
        JHSTaskConstraints(MyriadConfiguration cfg) {
          this.portsCount = 0;
          Map<String, ServiceConfiguration> auxConfigs = cfg.getServiceConfigurations();
          if (auxConfigs == null) {
            return;
          }
          // it is overkill, but just in case name of the service is different from "jobhistory"
          for (Map.Entry<String, ServiceConfiguration> auxConfig : auxConfigs.entrySet()) {
            final ServiceConfiguration auxCfg = auxConfig.getValue();
            if (JHSTaskFactoryImpl.class.getName().equals(auxCfg.getTaskFactoryImplName())) {
              if (auxCfg.getPorts() != null) {
                this.portsCount = auxCfg.getPorts().size();
              }
              return;
            }
          }
        }
        
        @Override
        public int portsCount() {
          return portsCount;
        }
      }
    }