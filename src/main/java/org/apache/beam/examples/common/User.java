package org.apache.beam.examples.common;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

/**
 * Class to hold information about a User
 */
@DefaultCoder(AvroCoder.class)
public class User {
  private Integer userId;
  private String zipCode;

  public User() {
  }

  public User(Integer userId, String zipCode) {
    this.userId = userId;
    this.zipCode = zipCode;
  }

  public Integer getUserId() {
    return userId;
  }

  public void setUserId(Integer userId) {
    this.userId = userId;
  }

  public String getZipCode() {
    return zipCode;
  }

  public void setZipCode(String zipCode) {
    this.zipCode = zipCode;
  }
}
