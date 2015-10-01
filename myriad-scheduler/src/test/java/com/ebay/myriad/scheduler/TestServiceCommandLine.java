package com.ebay.myriad.scheduler;

import static org.junit.Assert.*;

import org.apache.mesos.Protos.CommandInfo;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ebay.myriad.configuration.MyriadConfiguration;
import com.ebay.myriad.scheduler.TaskFactory.NMTaskFactoryImpl;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * Class to test CommandLine generation
 *
 */
public class TestServiceCommandLine {

  static MyriadConfiguration cfg;
  
  static String toJHSCompare = "echo \"sudo tar -zxpf hadoop-2.5.0.tar.gz && sudo chown hduser . &&" +
      " cp conf /usr/local/hadoop/etc/hadoop/yarn-site.xml; sudo -E -u hduser -H $YARN_HOME/bin/mapred historyserver\";" +
      "sudo tar -zxpf hadoop-2.5.0.tar.gz && sudo chown hduser . && cp conf /usr/local/hadoop/etc/hadoop/yarn-site.xml; sudo -E -u hduser -H $YARN_HOME/bin/mapred historyserver";
  
  static String toNMCompare = "echo \"sudo tar -zxpf hadoop-2.5.0.tar.gz && sudo chown hduser . &&" +
      " cp conf /usr/local/hadoop/etc/hadoop/yarn-site.xml; export YARN_HOME=/usr/local/hadoop; sudo -E -u hduser -H " +
      "env YARN_HOME=\"/usr/local/hadoop\" YARN_NODEMANAGER_OPTS=\"-Dyarn.nodemanager.container-executor.class=org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor " +
      "-Dnodemanager.resource.cpu-vcores=10 -Dnodemanager.resource.memory-mb=15 -Dmyriad.yarn.nodemanager.address=0.0.0.0:1 -Dmyriad.yarn.nodemanager.localizer.address=0.0.0.0:2 " +
      "-Dmyriad.yarn.nodemanager.webapp.address=0.0.0.0:3 -Dmyriad.mapreduce.shuffle.port=0.0.0.0:4\"  $YARN_HOME/bin/yarn nodemanager\";" +
      "sudo tar -zxpf hadoop-2.5.0.tar.gz && sudo chown hduser . && cp conf /usr/local/hadoop/etc/hadoop/yarn-site.xml; export YARN_HOME=/usr/local/hadoop; sudo -E -u hduser -H " +
      "env YARN_HOME=\"/usr/local/hadoop\" YARN_NODEMANAGER_OPTS=\"-Dyarn.nodemanager.container-executor.class=org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor " +
      "-Dnodemanager.resource.cpu-vcores=10 -Dnodemanager.resource.memory-mb=15 -Dmyriad.yarn.nodemanager.address=0.0.0.0:1 -Dmyriad.yarn.nodemanager.localizer.address=0.0.0.0:2 " +
      "-Dmyriad.yarn.nodemanager.webapp.address=0.0.0.0:3 -Dmyriad.mapreduce.shuffle.port=0.0.0.0:4\"  $YARN_HOME/bin/yarn nodemanager";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    cfg = mapper.readValue(
            Thread.currentThread().getContextClassLoader().getResource("myriad-config-test-default.yml"),
            MyriadConfiguration.class);

  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Test
  public void testJHSCommandLineGeneration() throws Exception {
    JHSTaskFactoryImpl jhs = new JHSTaskFactoryImpl(cfg, null);
    String executorCmd = "$YARN_HOME/bin/mapred historyserver";
    ServiceResourceProfile profile = new ServiceResourceProfile("jobhistory", 10.0, 15.0);
    
    CommandInfo cInfo = jhs.createCommandInfo(profile, executorCmd);
     
    assertTrue(toJHSCompare.equalsIgnoreCase(cInfo.getValue()));
  }

  @Test
  public void testNMCommandLineGeneration() throws Exception {
    Long [] ports = new Long [] {1L, 2L, 3L, 4L};
    NMPorts nmPorts = new NMPorts(ports);
    
    ServiceResourceProfile profile = new ExtendedResourceProfile(new NMProfile("nm", 10.0, 15.0), 3.0, 5.0);
    
    ExecutorCommandLineGenerator clGenerator = new DownloadNMExecutorCLGenImpl(cfg, "hdfs://namenode:port/dist/hadoop-2.5.0.tar.gz");
    NMTaskFactoryImpl nms = new NMTaskFactoryImpl(cfg, null, clGenerator);
    
    CommandInfo cInfo = nms.getCommandInfo(profile, nmPorts);
    
    assertTrue(toNMCompare.equalsIgnoreCase(cInfo.getValue()));

  }
}
