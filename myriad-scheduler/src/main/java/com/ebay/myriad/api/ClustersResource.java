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
package com.ebay.myriad.api;

import com.codahale.metrics.annotation.Timed;
import com.ebay.myriad.api.model.FlexDownClusterRequest;
import com.ebay.myriad.api.model.FlexDownServiceRequest;
import com.ebay.myriad.api.model.FlexUpClusterRequest;
import com.ebay.myriad.api.model.FlexUpServiceRequest;
import com.ebay.myriad.configuration.MyriadBadConfigurationException;
import com.ebay.myriad.configuration.MyriadConfiguration;
import com.ebay.myriad.configuration.NodeManagerConfiguration;
import com.ebay.myriad.scheduler.MyriadOperations;
import com.ebay.myriad.scheduler.ServiceProfileManager;
import com.ebay.myriad.state.SchedulerState;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * RESTful API to resource manager
 */
@Path("/cluster")
public class ClustersResource {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClustersResource.class);

    private MyriadConfiguration cfg;
    private SchedulerState schedulerState;
    private ServiceProfileManager profileManager;
    private MyriadOperations myriadOperations;

    @Inject
    public ClustersResource(MyriadConfiguration cfg,
                            SchedulerState state,
                            ServiceProfileManager profileManager,
                            MyriadOperations myriadOperations) {
        this.cfg = cfg;
        this.schedulerState = state;
        this.profileManager = profileManager;
        this.myriadOperations = myriadOperations;
    }

    @Timed
    @PUT
    @Path("/flexup")
    @Produces(MediaType.TEXT_PLAIN)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response flexUp(FlexUpClusterRequest request) {
        Preconditions.checkNotNull(request,
                "request object cannot be null or empty");

        LOGGER.info("Received Flexup Cluster Request");

        Integer instances = request.getInstances();
        String profile = request.getProfile();

        LOGGER.info("Instances: {}", instances);
        LOGGER.info("Profile: {}", profile);

        // Validate profile request
        Response.ResponseBuilder response = Response.status(Response.Status.ACCEPTED);
        if (!this.profileManager.exists(profile)) {
            response.status(Response.Status.BAD_REQUEST)
                    .entity("Profile does not exist: " + profile);
            LOGGER.error("Provided profile does not exist " + profile);
        } else if (!this.isValidInstanceSize(instances)) {
            response.status(Response.Status.BAD_REQUEST)
                    .entity("Invalid instance size: " + instances);
            LOGGER.error("Invalid instance size request " + instances);
        }

        Response returnResponse = response.build();
        if (returnResponse.getStatus() == Response.Status.ACCEPTED.getStatusCode()) {
            this.myriadOperations.flexUpCluster(instances, profile);
        }

        return returnResponse;
    }

    @Timed
    @PUT
    @Path("/flexupservice")
    @Produces(MediaType.TEXT_PLAIN)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response flexUpservice(FlexUpServiceRequest request) {
      Preconditions.checkNotNull(request,
              "request object cannot be null or empty");

      LOGGER.info("Received Flexup a Service Request");

      Integer instances = request.getInstances();
      String profile = request.getServiceName();

      LOGGER.info("Instances: {}", instances);
      LOGGER.info("Profile: {}", profile);
      
      // Validate profile request
      Response.ResponseBuilder response = Response.status(Response.Status.ACCEPTED);
      
      if (cfg.getServiceConfiguration(profile) != null) {
        response.status(Response.Status.BAD_REQUEST)
                .entity("Sevrice does not exist: " + profile);
        LOGGER.error("Provided service does not exist " + profile);
        return response.build();
      }
      
      if (!this.isValidInstanceSize(instances)) {
        response.status(Response.Status.BAD_REQUEST)
                .entity("Invalid instance size: " + instances);
        LOGGER.error("Invalid instance size request " + instances);
        return response.build();
      }

      try {
        this.myriadOperations.flexUpAService(instances, profile);
      } catch (MyriadBadConfigurationException e) {
        return response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
      }
      return response.build();
    }

    @Timed
    @PUT
    @Path("/flexdown")
    @Produces(MediaType.TEXT_PLAIN)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response flexDown(FlexDownClusterRequest request) {
        Preconditions.checkNotNull(request,
                "request object cannot be null or empty");

        Integer instances = request.getInstances();

        LOGGER.info("Received flexdown request");
        LOGGER.info("Instances: " + instances);

        Response.ResponseBuilder response = Response.status(Response.Status.ACCEPTED);

        if (!this.isValidInstanceSize(instances)) {
            response.status(Response.Status.BAD_REQUEST)
                    .entity("Invalid instance size: " + instances);
            LOGGER.error("Invalid instance size request " + instances);
        }

        // warn that number of requested instances isn't available
        // but instances will still be flexed down
        Integer flexibleInstances = this.myriadOperations.getFlexibleInstances(NodeManagerConfiguration.NM_TASK_PREFIX);
        if (flexibleInstances < instances)  {
            response.entity("Number of requested instances is greater than available.");
            // just doing a simple check here. pass the requested number of instances
            // to myriadOperations and let it sort out how many actually get flexxed down.
            LOGGER.warn("Requested number of instances greater than available: {} < {}", flexibleInstances, instances);
        }

        Response returnResponse = response.build();
        if (returnResponse.getStatus() == Response.Status.ACCEPTED.getStatusCode()) {
            this.myriadOperations.flexDownCluster(instances);
        }
        return returnResponse;
    }

    private boolean isValidInstanceSize(Integer instances) {
        return (instances > 0);
    }

    @Timed
    @PUT
    @Path("/flexdownservice")
    @Produces(MediaType.TEXT_PLAIN)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response flexDownservice(FlexDownServiceRequest request) {
      Preconditions.checkNotNull(request,
              "request object cannot be null or empty");
      
      Integer instances = request.getInstances();
      String serviceName = request.getServiceName();
      
      LOGGER.info("Received flexdown request for service {}", serviceName);
      LOGGER.info("Instances: " + instances);

      Response.ResponseBuilder response = Response.status(Response.Status.ACCEPTED);

      if (!this.isValidInstanceSize(instances)) {
          response.status(Response.Status.BAD_REQUEST)
                  .entity("Invalid instance size: " + instances);
          LOGGER.error("Invalid instance size request " + instances);
          return response.build();
      }

      // warn that number of requested instances isn't available
      // but instances will still be flexed down
      Integer flexibleInstances = this.myriadOperations.getFlexibleInstances(serviceName);
      if (flexibleInstances < instances)  {
          response.entity("Number of requested instances is greater than available.");
          // just doing a simple check here. pass the requested number of instances
          // to myriadOperations and let it sort out how many actually get flexxed down.
          LOGGER.warn("Requested number of instances greater than available: {} < {}", flexibleInstances, instances);
      }

      try {
        this.myriadOperations.flexDownAService(instances, serviceName);
      } catch (MyriadBadConfigurationException e) {
        return response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
      }
      return response.build();
    }
}
