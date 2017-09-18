package org.apache.beam.examples.events;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.sql.Timestamp;

/**
 * Class to hold information a User activation or deactivation.
 */
@DefaultCoder(AvroCoder.class)
public class UserActivationEvent {
  private Integer userId;
  private boolean active;
  private Timestamp occuredAt;

  public UserActivationEvent() {
  }

  public UserActivationEvent(Integer userId, boolean active, Timestamp occuredAt) {
    this.userId = userId;
    this.active = active;
    this.occuredAt = occuredAt;
  }

  public Integer getUserId() {
    return userId;
  }

  public void setUserId(Integer userId) {
    this.userId = userId;
  }

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
  }

  public Timestamp getOccuredAt() {
    return occuredAt;
  }

  public void setOccuredAt(Timestamp occuredAt) {
    this.occuredAt = occuredAt;
  }
}
