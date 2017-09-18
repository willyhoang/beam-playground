package org.apache.beam.examples.common;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

/**
 * Class to hold information about a User
 */
@DefaultCoder(AvroCoder.class)
public class User {
  private Integer userId;
  private Integer zipCode;

  public User() {
  }

  public User(Integer userId, Integer zipCode) {
    this.userId = userId;
    this.zipCode = zipCode;
  }

  public Integer getUserId() {
    return userId;
  }

  public void setUserId(Integer userId) {
    this.userId = userId;
  }

  public Integer getZipCode() {
    return zipCode;
  }

  public void setZipCode(Integer zipCode) {
    this.zipCode = zipCode;
  }
}
