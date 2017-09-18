package org.apache.beam.examples.events;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.sql.Timestamp;

/**
 * Class to hold information about an order shipped event.
 */
@DefaultCoder(AvroCoder.class)
public class OrderShippedEvent {
  private Integer orderId;
  private Integer userId;
  private Timestamp occuredAt;

  public OrderShippedEvent() {
  }

  public OrderShippedEvent(Integer orderId, Integer userId, Timestamp occuredAt) {
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

  public Timestamp getOccuredAt() {
    return occuredAt;
  }

  public void setOccuredAt(Timestamp occuredAt) {
    this.occuredAt = occuredAt;
  }
}
