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

import com.google.cloud.Tuple;
import org.apache.beam.examples.common.User;
import org.apache.beam.examples.events.OrderShippedEvent;
import org.apache.beam.examples.events.UserActivationEvent;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;

import java.util.logging.Logger;

/**
 * An example that counts words in Shakespeare and includes Beam best practices.
 *
 * <p>This class, {@link UserSegmentation}, is the second in a series of four successively more detailed
 * 'word count' examples. You may first want to take a look at {@link MinimalWordCount}.
 * After you've looked at this example, then see the {@link DebuggingWordCount}
 * pipeline, for introduction of additional concepts.
 *
 * <p>For a detailed walkthrough of this example, see
 *   <a href="https://beam.apache.org/get-started/wordcount-example/">
 *   https://beam.apache.org/get-started/wordcount-example/
 *   </a>
 *
 * <p>Basic concepts, also in the MinimalWordCount example:
 * Reading text files; counting a PCollection; writing to text files
 *
 * <p>New Concepts:
 * <pre>
 *   1. Executing a Pipeline both locally and using the selected runner
 *   2. Using ParDo with static DoFns defined out-of-line
 *   3. Building a composite transform
 *   4. Defining your own pipeline options
 * </pre>
 *
 * <p>Concept #1: you can execute this pipeline either locally or using by selecting another runner.
 * These are now command-line options and not hard-coded as they were in the MinimalWordCount
 * example.
 *
 * <p>To change the runner, specify:
 * <pre>{@code
 *   --runner=YOUR_SELECTED_RUNNER
 * }
 * </pre>
 *
 * <p>To execute this pipeline, specify a local output file (if using the
 * {@code DirectRunner}) or output prefix on a supported distributed file system.
 * <pre>{@code
 *   --output=[YOUR_LOCAL_FILE | YOUR_OUTPUT_PREFIX]
 * }</pre>
 *
 * <p>The input file defaults to a public data set containing the text of of King Lear,
 * by William Shakespeare. You can override it and choose your own input with {@code --inputFile}.
 */
public class UserSegmentation {

  private static Logger logger = Logger.getLogger(UserSegmentation.class.getName());

  static class ParseUserInfo extends DoFn<String, User> {
    private final Counter numParseErrors = Metrics.counter("main", "ParseErrors");

    @ProcessElement
    public void processElement(ProcessContext c) {
      String[] fields = c.element().split(",");
      try {
        int userId = Integer.parseInt(fields[0]);
        String zipCode = fields[40];
        if (zipCode.length() < 5) {
          logger.info("Zipcode invalid for " + c.element());
          return;
        }
        User parsedUser = new User(userId, zipCode);
        c.output(parsedUser);
      } catch (NumberFormatException e) {
        numParseErrors.inc();
        logger.info("Could not parse fields correctly on " + c.element() + "," + e.getMessage());
      }
    }
  }

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


  static class keyByZipCluster extends DoFn<User, KV<String, User>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      User user = c.element();
      String zipCluster = getZipCluster(user.getZipCode());
      c.output(KV.<String, User>of(zipCluster, user));
    }

    private String getZipCluster(String zipCode) {
      return zipCode.substring(0, 1);
    }

  }

  /** A SimpleFunction that converts a Word and Count into a printable string. */
  public static class FormatAsTextFn<T> extends SimpleFunction<KV<Integer, T>, String> {
    @Override
    public String apply(KV<Integer, T> input) {
      return String.format("user_id: %s -> %s", input.getKey(), input.getValue().toString());
    }
  }


  /**
   * Options supported by {@link UserSegmentation}.
   *
   * <p>Concept #4: Defining your own configuration options. Here, you can add your own arguments
   * to be processed by the command-line parser, and specify default values for them. You can then
   * access the options values in your pipeline code.
   *
   * <p>Inherits standard configuration options.
   */
  public interface WordCountOptions extends PipelineOptions {

    /**
     * By default, this example reads from a public dataset containing the text of
     * King Lear. Set this option to choose a different input file or glob.
     */
    @Description("Path of the file to read from")
    @Default.String("gs://machinelearning.data.blueapron.com/willy/user_segmentation_attributes_1000")
    String getInputFile();
    void setInputFile(String value);

    @Description("Path of order events file to read from")
    String getOrdersFile();
    void setOrdersFile(String value);

    /**
     * Set this required option to specify where to write the output.
     */
    @Description("Path of the file to write to")
    @Required
    String getOutput();
    void setOutput(String value);

    /**
     * Set this required option to specify where to write the output.
     */
    @Description("Path of the file to write to")
    @Required
    String getOutput2();
    void setOutput2(String value);

  }

  public static void main(String[] args) {
    WordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
      .as(WordCountOptions.class);
    Pipeline p = Pipeline.create(options);
    logger.info("Input file: " +  options.getInputFile());

    // Concepts #2 and #3: Our pipeline applies the composite CountWords transform, and passes the
    // static FormatAsTextFn() to the ParDo transform.
    PCollection<KV<Integer, UserActivationEvent>> userActivationEvents =
        p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
         .apply(ParDo.of(new ParseUserActivationEvent()))
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
        .apply("WriteCounts", TextIO.write().to(options.getOutput()));

    PCollection<KV<Integer, OrderShippedEvent>> orderShippedEvents =
        p.apply("ReadLines", TextIO.read().from(options.getOrdersFile()))
            .apply(ParDo.of(new ParseOrderShippedEvent()))
            .apply(WithKeys.of(new SerializableFunction<OrderShippedEvent, Integer>() {
              @Override
              public Integer apply(OrderShippedEvent input) {
                return input.getUserId();
              }
            }));

    PCollection<KV<Integer, Long>> usersNumShippedOrders = orderShippedEvents
        .apply(Count.perKey());

    usersNumShippedOrders.apply(MapElements.via(new FormatAsTextFn<Long>()))
        .apply("WriteCounts", TextIO.write().to(options.getOutput2()));


    TupleTag<Boolean> activeFlagTag = new TupleTag<Boolean>();
    TupleTag<Long> orderCountTag = new TupleTag<Long>();

    PCollection<KV<Integer, CoGbkResult>> usersState = KeyedPCollectionTuple
        .of(orderCountTag, usersNumShippedOrders)
        .and(activeFlagTag, usersIsActive)
        .apply(CoGroupByKey.<Integer>create());

    usersState.apply(MapElements.via(new SimpleFunction<KV<Integer, CoGbkResult>, String>() {
       @Override
       public String apply(KV<Integer, CoGbkResult> input) {
         boolean isActive = input.getValue().getOnly(activeFlagTag);
         long orderCount = input.getValue().getOnly(orderCountTag);
         return String.format("User id: %d, Active: %b, Orders: %d", input.getKey(), isActive, orderCount);
       }
     }
    ))
    .apply("WriteMerged", TextIO.write().to("countsMerged"));


    p.run().waitUntilFinish();
  }
}
