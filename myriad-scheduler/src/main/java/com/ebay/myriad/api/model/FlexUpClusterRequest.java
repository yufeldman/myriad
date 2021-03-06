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
package com.ebay.myriad.api.model;

import com.google.gson.Gson;
import java.util.List;
import org.hibernate.validator.constraints.NotEmpty;

/**
 * Flex up request parameters
 */
public class FlexUpClusterRequest {
    @NotEmpty
    public Integer instances;

    @NotEmpty
    public String profile;

    public List<String> constraints;

    public FlexUpClusterRequest() {
    }
    
    public FlexUpClusterRequest(Integer instances, String profile, List<String> constraints) {
        this.instances = instances;
        this.profile = profile;
        this.constraints = constraints;
    }

    public Integer getInstances() {
        return instances;
    }

    public void setInstances(Integer instances) {
        this.instances = instances;
    }

    public String getProfile() {
        return profile;
    }

    public void setProfile(String profile) {
        this.profile = profile;
    }

    public List<String> getConstraints() {
      return constraints;
    }

    public void setConstraints(List<String> constraints) {
      this.constraints = constraints;
    }

    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}
