package com.ebay.myriad.api;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ebay.myriad.api.model.FlexUpClusterRequest;
import com.google.common.base.Preconditions;

/**
 * Test class for flexup/down APIs
 *
 */
public class TestClusterResource {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Test
  public void testFlexUpPreconditions() {
    FlexUpClusterRequest flexupRequest = new FlexUpClusterRequest(null, null);
    
    try {
    Preconditions.checkNotNull(flexupRequest.getInstances(),
        "instances are null");
    
    fail("Should not reach here");
    
    Preconditions.checkNotNull(flexupRequest.getProfile(),
        "profile is null");
    } catch (NullPointerException npe) {
      assertTrue(npe.getMessage().equalsIgnoreCase("instances are null"));
    }
    
    flexupRequest = new FlexUpClusterRequest(5, null);
    
    try {
    Preconditions.checkNotNull(flexupRequest.getInstances(),
        "instances are null");
    
    Preconditions.checkNotNull(flexupRequest.getProfile(),
        "profile is null");
    fail("Should not reach here");    
    } catch (NullPointerException npe) {
      assertTrue(npe.getMessage().equalsIgnoreCase("profile is null"));
    }

    flexupRequest = new FlexUpClusterRequest(5, "abc");
    
    try {
    Preconditions.checkNotNull(flexupRequest.getInstances(),
        "instances are null");
    
    Preconditions.checkNotNull(flexupRequest.getProfile(),
        "profile is null");
    
    } catch (NullPointerException npe) {
      fail("Should not reach here");
    }
  }

}
