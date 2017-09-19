package org.apache.beam.examples.common;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

/**
 * Class to hold information about a User's stats.
 */
@DefaultCoder(AvroCoder.class)
public class UserStats {
  private boolean isActive;
  private long numOrdersShipped;

  public UserStats() {
  }

  public UserStats(boolean isActive, long numOrdersShipped) {
    this.isActive = isActive;
    this.numOrdersShipped = numOrdersShipped;
  }

  public boolean isActive() {
    return isActive;
  }

  public void setActive(boolean active) {
    isActive = active;
  }

  public long getNumOrdersShipped() {
    return numOrdersShipped;
  }

  public void setNumOrdersShipped(long numOrdersShipped) {
    this.numOrdersShipped = numOrdersShipped;
  }

  @Override
  public String toString() {
    return "UserStats{" +
        "isActive=" + isActive +
        ", numOrdersShipped=" + numOrdersShipped +
        '}';
  }
}
