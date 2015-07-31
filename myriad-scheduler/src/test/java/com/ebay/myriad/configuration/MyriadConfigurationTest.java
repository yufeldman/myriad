package com.ebay.myriad.configuration;

import static org.junit.Assert.*;

import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * AuxServices/tasks test
 *
 */
public class MyriadConfigurationTest {

  static MyriadConfiguration cfg;
  
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
  public void additionalPropertiestest() throws Exception {
    
    Map<String, AuxTaskConfiguration> auxConfigs = cfg.getAuxTaskConfigurations();
    
    assertNotNull(auxConfigs);
    assertEquals(auxConfigs.size(), 2);
    
    for (Map.Entry<String, AuxTaskConfiguration> entry : auxConfigs.entrySet()) {
      String taskName = entry.getKey();
      AuxTaskConfiguration config = entry.getValue();
      String outTaskname = config.getTaskName();
      assertEquals(taskName, outTaskname);
    }
  }

}
