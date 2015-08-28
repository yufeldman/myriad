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

package com.ebay.myriad.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;

/**
 * Myriad specific exception
 *
 */
@SuppressWarnings("serial")
public class MyriadBadConfigurationException extends Exception {

  @JsonProperty
  private String message;
  
  public MyriadBadConfigurationException(String message) {
    super(message);
  }

  @Override
  public String toString() {
      Gson gson = new Gson();
      return gson.toJson(this);
  }

  public String getMessage() {
    return super.getMessage();
  }

  public void setMessage(String message) {
    this.message = message;
  }
}
