package com.ebay.myriad;

import static org.junit.Assert.*;

import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ebay.myriad.scheduler.TaskFactory;
import com.google.inject.Guice;
import com.google.inject.Injector;

/**
 * Test for Multibindings
 *
 */
public class MultiBindingsTest {

  private static Injector injector;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MyriadTestModule myriadModule = new MyriadTestModule();
    injector = Guice.createInjector(
            myriadModule);

  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Test
  public void multiBindingsTest() {
    
    MultiBindingsUsage myinstance = injector.getInstance(MultiBindingsUsage.class);
    
    Map<String, TaskFactory> taskMap = myinstance.getMap();
    assertNotNull(taskMap);
    assertEquals(3, taskMap.size());
    
    taskMap = myinstance.getMap();
    for (Map.Entry<String, TaskFactory> entry : taskMap.entrySet()) {
      String keyName = entry.getKey();
      TaskFactory taskFactory = entry.getValue();
    }
    
    
  }

}
