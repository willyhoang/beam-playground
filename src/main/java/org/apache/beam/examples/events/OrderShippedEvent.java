package org.apache.beam.examples.events;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.joda.time.Instant;

/**
 * Class to hold information about an order shipped event.
 */
@DefaultCoder(AvroCoder.class)
public class OrderShippedEvent {
  private Integer orderId;
  private Integer userId;
  private Instant occuredAt;

  public OrderShippedEvent() {
  }

  public OrderShippedEvent(Integer orderId, Integer userId, Instant occuredAt) {
    this.orderId = orderId;
    this.userId = userId;
    this.occuredAt = occuredAt;
  }

  public Integer getOrderId() {
    return orderId;
  }

  public void setOrderId(Integer orderId) {
    this.orderId = orderId;
  }

  public Integer getUserId() {
    return userId;
  }

  public void setUserId(Integer userId) {
    this.userId = userId;
  }

  public Instant getOccuredAt() {
    return occuredAt;
  }

  public void setOccuredAt(Instant occuredAt) {
    this.occuredAt = occuredAt;
  }

  @Override
  public String toString() {
    return "OrderShippedEvent{" +
        "orderId=" + orderId +
        ", userId=" + userId +
        ", occuredAt=" + occuredAt +
        '}';
  }
}
