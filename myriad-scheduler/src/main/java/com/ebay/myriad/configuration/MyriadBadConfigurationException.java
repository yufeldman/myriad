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
