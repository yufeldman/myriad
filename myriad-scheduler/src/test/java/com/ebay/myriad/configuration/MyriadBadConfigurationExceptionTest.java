package com.ebay.myriad.configuration;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Class to test MyriadBadConfigurationException
 *
 */
public class MyriadBadConfigurationExceptionTest {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Test
  public void myriadExceptionTest() {
    final String testStr = "{\"detailMessage\":\"Bad configuration exception\",\"stackTrace\":[],\"suppressedExceptions\":[]}";
    MyriadBadConfigurationException exp = new MyriadBadConfigurationException("Bad configuration exception");
    
    assertEquals(testStr, exp.toString());
  }

}
