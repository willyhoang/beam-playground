package org.apache.beam.examples.events;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.joda.time.Instant;

/**
 * Class to hold information a User activation or deactivation.
 */
@DefaultCoder(AvroCoder.class)
public class UserActivationEvent {
  private Integer userId;
  private boolean active;
  private Instant occuredAt;

  public UserActivationEvent() {
  }

  public UserActivationEvent(Integer userId, boolean active, Instant occuredAt) {
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

  public Instant getOccuredAt() {
    return occuredAt;
  }

  public void setOccuredAt(Instant occuredAt) {
    this.occuredAt = occuredAt;
  }

  @Override
  public String toString() {
    return "UserActivationEvent{" +
        "userId=" + userId +
        ", active=" + active +
        ", occuredAt=" + occuredAt +
        '}';
  }
}
