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

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.Value;
import org.apache.mesos.Protos.CommandInfo.URI;
import org.apache.mesos.Protos.Value.Scalar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.myriad.configuration.AuxTaskConfiguration;
import com.ebay.myriad.configuration.MyriadConfiguration;
import com.ebay.myriad.configuration.MyriadExecutorConfiguration;
import com.ebay.myriad.state.NodeTask;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

  /**
   * Task creation class for JobHistoryServer
   * TODO (yfeldman) if we add attributes on slaves that can run JHS
   * we can also restrict where JHS can be started
   */
    public class JHSTaskFactoryImpl implements TaskFactory {
      private static final Logger LOGGER = LoggerFactory.getLogger(JHSTaskFactoryImpl.class);
      
      private static final String[] JHS_ADDRESSES = new String[] {
        "mapreduce.jobhistory.admin.address",
        "mapreduce.jobhistory.address",
        "mapreduce.jobhistory.webapp.address"
      };
      
      
      private static final int JHS_PORTS_COUNT = 3;

      private MyriadConfiguration cfg;
      @SuppressWarnings("unused")
      private TaskUtils taskUtils;
      private TaskConstraints constraints;

      @Inject
      public JHSTaskFactoryImpl(MyriadConfiguration cfg, TaskUtils taskUtils) {
          this.cfg = cfg;
          this.taskUtils = taskUtils;
          this.constraints = new JHSTaskConstraints(cfg);
      }
      @Override
      public TaskInfo createTask(Offer offer, TaskID taskId, NodeTask nodeTask) {
        Objects.requireNonNull(offer, "Offer should be non-null");
        Objects.requireNonNull(nodeTask, "NodeTask should be non-null");

        AuxTaskConfiguration serviceConfig = 
            cfg.getAuxTaskConfiguration(nodeTask.getTaskPrefix());
        
        if (serviceConfig == null) {
          return null;
        }
        
        //TODO (yfeldman) domain for DNS
        final String jhsHostName = nodeTask.getTaskPrefix() + "." + cfg.getFrameworkName() + ".mesos";
        final String jsEnv = serviceConfig.getEnvSettings();
        final String rmHostName = System.getProperty(YARN_RESOURCEMANAGER_HOSTNAME);
        
        final StringBuilder strB = new StringBuilder("env && export HADOOP_JOB_HISTORYSERVER_OPTS=");
        strB.append("\"");
        if (rmHostName != null && !rmHostName.isEmpty()) {
          strB.append("-D" + YARN_RESOURCEMANAGER_HOSTNAME + "=" + rmHostName + " ");
        }
        final List<Long> ports = serviceConfig.getPorts();
        
        final List<Long> returnedPorts = getAvailablePorts(offer, ports);
        
        if (ports != null && ports.size() >= JHS_PORTS_COUNT) {
          int i = 0;
          for (Long port : returnedPorts) {
            strB.append("-D" + JHS_ADDRESSES[i++] + "=" + jhsHostName + ":" + port + " ");
          }
        } else {
          LOGGER.info("Number of specified ports is not enough. Using JobHistoryServer default ports");
        }
        
        strB.append(jsEnv);
        strB.append("\"");
        strB.append(" && $YARN_HOME/bin/mapred historyserver");
        
        CommandInfo commandInfo = createCommandInfo(strB.toString());

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
        
        // set ports
        if (returnedPorts != null && !returnedPorts.isEmpty()) {
          Value.Ranges.Builder valueRanger = Value.Ranges.newBuilder();
          for (Long port : returnedPorts) {
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
      
      private CommandInfo createCommandInfo(String executorCmd) {
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
                  /*
                   TODO(darinj): Overall this is messier than I'd like. We can't let mesos untar the distribution, since
                   it will change the permissions.  Instead we simply download the tarball and execute tar -xvpf. We also
                   pull the config from the resource manager and put them in the conf dir.  This is also why we need
                   frameworkSuperUser. This will be refactored after Mesos-1790 is resolved.
                  */

          //Both FrameworkUser and FrameworkSuperuser to get all of the directory permissions correct.
          if (!(cfg.getFrameworkUser().isPresent() && cfg.getFrameworkSuperUser().isPresent())) {
            throw new RuntimeException("Trying to use remote distribution, but frameworkUser" +
                "and/or frameworkSuperUser not set!");
          }

          LOGGER.info("Using remote distribution");

          String nmURIString = myriadExecutorConfiguration.getNodeManagerUri().get();

          //TODO(DarinJ) support other compression, as this is a temp fix for Mesos 1760 may not get to it.
          //Extract tarball keeping permissions, necessary to keep HADOOP_HOME/bin/container-executor suidbit set.
          String tarCmd = "sudo tar -zxpf " + getFileName(nmURIString);

          //We need the current directory to be writable by frameworkUser for capsuleExecutor to create directories.
          //Best to simply give ownership to the user running the executor but we don't want to use -R as this
          //will silently remove the suid bit on container executor.
          String chownCmd = "sudo chown " + cfg.getFrameworkUser().get() + " .";

          //Place the hadoop config where in the HADOOP_CONF_DIR where it will be read by the JHS
          //The url for the resource manager config is: http(s)://hostname:port/conf so fetcher.cpp downloads the
          //config file to conf, It's an xml file with the parameters of yarn-site.xml, core-site.xml, mapred-site.xml and hdfs.xml.
          String configCopyCmd = "cp conf " + cfg.getYarnEnvironment().get("YARN_HOME") +
              "/etc/hadoop/yarn-site.xml";

          //Command to run the executor
          String executorPathString = myriadExecutorConfiguration.getPath();
          String sudoExecutorCmd = "sudo -E -u " + cfg.getFrameworkUser().get() + " -H " +
              executorCmd;

          //Concatenate all the subcommands
          String cmd = tarCmd + "&&" + chownCmd + "&&" + configCopyCmd + "&&" + sudoExecutorCmd;

          //get the nodemanagerURI
          //We're going to extract ourselves, so setExtract is false
          LOGGER.info("Getting Hadoop distribution from:" + nmURIString);
          URI nmUri = URI.newBuilder().setValue(nmURIString).setExtract(false)
              .build();

          //get configs directly from resource manager
          String configUrlString = getConfigurationUrl();
          LOGGER.info("Getting config from:" + configUrlString);
          URI configUri = URI.newBuilder().setValue(configUrlString)
              .build();

          //get the executor URI
          LOGGER.info("Getting executor from:" + executorPathString);
          URI executorUri = URI.newBuilder().setValue(executorPathString).setExecutable(true)
              .build();

          LOGGER.info("Slave will execute command:" + cmd);
          commandInfo.addUris(nmUri).addUris(configUri).addUris(executorUri).setValue("echo \"" + cmd + "\";" + cmd);

          commandInfo.setUser(cfg.getFrameworkSuperUser().get());

        } else {
          commandInfo.setValue(executorCmd);
        }
        return commandInfo.build();
      }
      
      private static String getFileName(String uri) {
        int lastSlash = uri.lastIndexOf('/');
        if (lastSlash == -1) {
          return uri;
        } else {
          String fileName = uri.substring(lastSlash + 1);
          Preconditions.checkArgument(!Strings.isNullOrEmpty(fileName),
              "URI should not have a slash at the end");
          return fileName;
        }
      }

      private String getConfigurationUrl() {
        YarnConfiguration conf = new YarnConfiguration();
        String httpPolicy = conf.get(YARN_HTTP_POLICY);
        if (httpPolicy != null && httpPolicy.equals(YARN_HTTP_POLICY_HTTPS_ONLY)) {
          String address = conf.get(YARN_RESOURCEMANAGER_WEBAPP_HTTPS_ADDRESS);
          if (address == null || address.isEmpty()) {
            address = conf.get(YARN_RESOURCEMANAGER_HOSTNAME) + ":8090";
          }
          return "https://" + address + "/conf";
        } else {
          String address = conf.get(YARN_RESOURCEMANAGER_WEBAPP_ADDRESS);
          if (address == null || address.isEmpty()) {
            address = conf.get(YARN_RESOURCEMANAGER_HOSTNAME) + ":8088";
          }
          return "http://" + address + "/conf";
        }
      }

      /**
       * Helper method to reserve ports
       * @param offer
       * @param requestedPorts
       * @return
       */
      private List<Long> getAvailablePorts(Offer offer, List<Long> requestedPorts) {
        if (requestedPorts == null || requestedPorts.isEmpty()) {
          return null;
        }
        final List<Long> returnedPorts = new ArrayList<>();
        boolean [] retValue = new boolean[requestedPorts.size()];
        for (Resource resource : offer.getResourcesList()){
          if (resource.getName().equals("ports")){
            Iterator<Value.Range> itr = resource.getRanges().getRangeList().iterator();
            int l = 0;
            while (itr.hasNext()) {
              Value.Range range = itr.next();
              if (range.getBegin() <= range.getEnd()) {
                for (int k = 0; k < requestedPorts.size(); k++) {
                  final Long port = requestedPorts.get(k);
                  if (port <= range.getEnd() && port >= range.getBegin()) {
                    retValue[k] = true;
                    l++;
                  }
                  if (l >= requestedPorts.size()) {
                    return requestedPorts;
                  }
                }
                // fill up some ports in case we won't find ones we need
                long i = range.getBegin();
                while (i <= range.getEnd() && returnedPorts.size() < requestedPorts.size()) {
                  returnedPorts.add(i);
                  i++;
                }
              }
            }
          }
        }
        // in case we managed to get some required ports
        // set to random only ones we could not set to requested
        for (int j = 0; j < retValue.length; j++) {
          if (retValue[j]) {
            returnedPorts.set(j, requestedPorts.get(j));
          } else {
            // TODO (yfeldman) need to make this more visible
            LOGGER.info("Following port: {} is not available will use {} port instead", 
                requestedPorts.get(j), returnedPorts.get(j));
          }
        }
        return returnedPorts;
      }
      @Override
      public ExecutorInfo getExecutorInfoForSlave(SlaveID slave) {
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
          Map<String, AuxTaskConfiguration> auxConfigs = cfg.getAuxTaskConfigurations();
          if (auxConfigs == null) {
            return;
          }
          // it is overkill, but just in case name of the service is different from "jobhistory"
          for (Map.Entry<String, AuxTaskConfiguration> auxConfig : auxConfigs.entrySet()) {
            final AuxTaskConfiguration auxCfg = auxConfig.getValue();
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