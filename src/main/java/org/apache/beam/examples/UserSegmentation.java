/*
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
package org.apache.beam.examples;

import org.apache.beam.examples.common.UserSegment;
import org.apache.beam.examples.common.UserStats;
import org.apache.beam.examples.events.OrderShippedEvent;
import org.apache.beam.examples.events.UserActivationEvent;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.logging.Logger;

public class UserSegmentation {

  private static Logger logger = Logger.getLogger(UserSegmentation.class.getName());

  static class ParseUserActivationEvent extends DoFn<String, UserActivationEvent> {
    private final Counter numParseErrors = Metrics.counter("main", "ParseErrors");

    @ProcessElement
    public void processElement(ProcessContext c) {
      String[] fields = c.element().split(",");
        int userId = Integer.parseInt(fields[0]);
        boolean active = (fields[1].equals("true"));
        Instant occuredAt = Instant.parse(fields[2]);
        UserActivationEvent userActivationEvent = new UserActivationEvent(userId, active, occuredAt);
        c.outputWithTimestamp(userActivationEvent, occuredAt);
    }
  }

  static class ParseOrderShippedEvent extends DoFn<String, OrderShippedEvent> {
    private final Counter numParseErrors = Metrics.counter("main", "ParseErrors");

    @ProcessElement
    public void processElement(ProcessContext c) {
      String[] fields = c.element().split(",");
        int orderId = Integer.parseInt(fields[0]);
        int userId = Integer.parseInt(fields[1]);
        Instant occuredAt = Instant.parse(fields[2]);

        OrderShippedEvent orderShippedEvent = new OrderShippedEvent(orderId, userId, occuredAt);
        c.outputWithTimestamp(orderShippedEvent, occuredAt);
    }
  }

  static class AssignUserSegment extends SimpleFunction<KV<Integer, UserStats>, KV<Integer, UserSegment>> {
    @Override
    public KV<Integer, UserSegment> apply(KV<Integer, UserStats> input) {
      boolean isActive = input.getValue().isActive();
      long numOrders = input.getValue().getNumOrdersShipped();
      UserSegment assignment = null;
      if (!isActive) {
        assignment = UserSegment.DEACTIVATED_USER;
      } else {
        if (numOrders == 0) {
          assignment = UserSegment.NEW_USER;
        } else if (numOrders == 1) {
          assignment = UserSegment.NEW_USER_WITH_FIRST_ORDER;
        } else if (numOrders == 2) {
          assignment = UserSegment.NEW_USER_WITH_SECOND_ORDER;
        } else if (numOrders >= 3) {
          assignment = UserSegment.ANCIENT_USER;
        }
      }
      return KV.of(input.getKey(), assignment);
    }
  }

  public static class FormatAsTextFn<T> extends SimpleFunction<KV<Integer, T>, String> {
    @Override
    public String apply(KV<Integer, T> input) {
      try {
        return String.format("user_id: %s -> %s", input.getKey(), input.getValue().toString());
      } catch (NullPointerException e) {
        logger.info("Got a null value for: " + input);
        return "";
      }
    }
  }

  public interface WordCountOptions extends PipelineOptions {

    @Description("Path of the file to read from")
    String getUserActivationsFile();
    void setUserActivationsFile(String value);

    @Description("Path of order events file to read from")
    String getOrdersShippedFile();
    void setOrdersShippedFile(String value);
  }

  public static void main(String[] args) {
    WordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
      .as(WordCountOptions.class);
    Pipeline p = Pipeline.create(options);
    logger.info("Users activation events file:" +  options.getUserActivationsFile());

    PCollection<KV<Integer, UserActivationEvent>> userActivationEvents =
        p.apply("ReadLines", TextIO.read().from(options.getUserActivationsFile()))
         .apply(ParDo.of(new ParseUserActivationEvent()))
         .apply(WithTimestamps.of((UserActivationEvent event) -> event.getOccuredAt()))
         .apply(Window.<UserActivationEvent>into(new GlobalWindows())
             .withAllowedLateness(Duration.standardHours(0))
             .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(1))))
             .accumulatingFiredPanes()
         )
         .apply(WithKeys.of(new SerializableFunction<UserActivationEvent, Integer>() {
              @Override
              public Integer apply(UserActivationEvent input) {
                return input.getUserId();
              }
            }));

    PCollection<KV<Integer, Boolean>> usersIsActive = userActivationEvents
        .apply(Latest.perKey())
        .apply(ParDo.of(new DoFn<KV<Integer, UserActivationEvent>, KV<Integer, Boolean>>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            c.output(KV.of(c.element().getKey(), c.element().getValue().isActive()));
          }
        }));

    usersIsActive.apply(MapElements.via(new FormatAsTextFn<Boolean>()))
        .apply("WriteCounts", TextIO.write().to("counts"));

    PCollection<KV<Integer, OrderShippedEvent>> orderShippedEvents =
        p.apply("ReadLines", TextIO.read().from(options.getOrdersShippedFile()))
            .apply(ParDo.of(new ParseOrderShippedEvent()))
            .apply(WithTimestamps.of((OrderShippedEvent event) -> event.getOccuredAt()))
            .apply(Window.<OrderShippedEvent>into(new GlobalWindows())
                .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(1))))
                .withAllowedLateness(Duration.standardMinutes(2))
                .accumulatingFiredPanes()
            )
            .apply(WithKeys.of(new SerializableFunction<OrderShippedEvent, Integer>() {
              @Override
              public Integer apply(OrderShippedEvent input) {
                return input.getUserId();
              }
            }));

    PCollection<KV<Integer, Long>> usersNumShippedOrders = orderShippedEvents
        .apply(Count.perKey());

    usersNumShippedOrders.apply(MapElements.via(new FormatAsTextFn<Long>()))
        .apply("WriteCounts", TextIO.write().to("counts2"));


    TupleTag<Boolean> activeFlagTag = new TupleTag<Boolean>();
    TupleTag<Long> orderCountTag = new TupleTag<Long>();

    PCollection<KV<Integer, UserStats>> usersState = KeyedPCollectionTuple
        .of(orderCountTag, usersNumShippedOrders)
        .and(activeFlagTag, usersIsActive)
        .apply(CoGroupByKey.<Integer>create())
        .apply(MapElements.via(new SimpleFunction<KV<Integer, CoGbkResult>, KV<Integer, UserStats>>() {
          @Override
          public KV<Integer, UserStats> apply(KV<Integer, CoGbkResult> input) {
            boolean isActive = input.getValue().getOnly(activeFlagTag, false);
            long orderCount = input.getValue().getOnly(orderCountTag, -1L);
            UserStats userStats = new UserStats(isActive, orderCount);
            return KV.of(input.getKey(), userStats);
          }
        }));

    usersState.apply(MapElements.via(new SimpleFunction<KV<Integer, UserStats>, String>() {
       @Override
       public String apply(KV<Integer, UserStats> input) {
         boolean isActive = input.getValue().isActive();
         long orderCount = input.getValue().getNumOrdersShipped();
         return String.format("User id: %d, Active: %b, Orders: %d", input.getKey(), isActive, orderCount);
       }
     }
    ))
    .apply("WriteMerged", TextIO.write().to("countsMerged"));

    usersState.apply(MapElements.via(new AssignUserSegment()))
        .apply(MapElements.via(new FormatAsTextFn<UserSegment>()))
        .apply("WriteCounts", TextIO.write().to("countsUserSegments"));

    p.run().waitUntilFinish();
  }
}
