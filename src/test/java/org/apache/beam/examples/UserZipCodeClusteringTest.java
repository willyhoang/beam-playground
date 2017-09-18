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

import org.apache.beam.examples.DebuggingWordCount.WordCountOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.InputStream;
import java.net.URL;

/**
 * Tests for {@link UserZipcodeClustering}.
 */
@RunWith(JUnit4.class)
public class UserZipCodeClusteringTest {
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testDebuggingWordCount() throws Exception {
    URL fileUrl = this.getClass().getResource("/results-20170915-113730.csv");
    String filePath = fileUrl.getPath();
    File testFile = new File(filePath);
    File outputFile = tmpFolder.newFile();

    WordCountOptions options =
        TestPipeline.testingPipelineOptions().as(WordCountOptions.class);
    options.setInputFile(testFile.getAbsolutePath());
    options.setOutput(outputFile.getAbsolutePath());
    UserZipcodeClustering.main(TestPipeline.convertToArgs(options));
  }

  @Rule
  public TestPipeline p = TestPipeline.create();

  @Test
  public void testUser() throws Exception {
    URL fileUrl = this.getClass().getResource("/results-20170915-113730.csv");
    String filePath = fileUrl.getPath();

    PCollection<String> lines = p.apply(TextIO.read().from(filePath))
        .apply(Filter.by((String line) -> !line.contains("user_id,meal_plan")));


    p.run().waitUntilFinish();
  }
}
