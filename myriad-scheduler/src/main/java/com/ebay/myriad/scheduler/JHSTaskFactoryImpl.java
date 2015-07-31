package com.ebay.myriad.scheduler;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.inject.Inject;

import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.Value;
import org.apache.mesos.Protos.Value.Scalar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.myriad.configuration.AuxTaskConfiguration;
import com.ebay.myriad.configuration.MyriadConfiguration;
import com.ebay.myriad.state.NodeTask;

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
        
        CommandInfo.Builder commandInfo = CommandInfo.newBuilder();
        
        String yarnHomeEnv = cfg.getYarnEnvironment().get("YARN_HOME");
        org.apache.mesos.Protos.Environment.Variable.Builder yarnEnvB = 
            org.apache.mesos.Protos.Environment.Variable.newBuilder();
        yarnEnvB.setName("YARN_HOME").setValue(yarnHomeEnv);
        org.apache.mesos.Protos.Environment.Builder yarnHomeB = 
            org.apache.mesos.Protos.Environment.newBuilder();
        yarnHomeB.addVariables(yarnEnvB.build());
        commandInfo.mergeEnvironment(yarnHomeB.build());

        commandInfo.setValue(strB.toString()).build();

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