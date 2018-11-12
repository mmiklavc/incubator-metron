/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.metron.statistics.approximation;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.adrianwalker.multilinestring.Multiline;
import org.apache.metron.statistics.approximation.FrequencySketch.ItemEstimate;
import org.apache.metron.stellar.common.utils.StellarProcessorUtils;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

public class FrequencySketchIntegrationTest {

  private Map<String, Object> values = new HashMap<String, Object>() {{
    put("val1", "mike");
    put("val2", "mike");
    put("val3", "metron");
    put("val4", "batman");
    put("nullArg", null);
  }};

  /**
   *  FREQUENCY_SKETCH_UNPACK(
   *  FREQUENCY_SKETCH_ESTIMATE(
   *    FREQUENCY_SKETCH_UPDATE(
   *      FREQUENCY_SKETCH_UPDATE(
   *        FREQUENCY_SKETCH_INIT(2),
   *        val1
   *      ),
   *      val2
   *    ),
   *    'mike'
   *  ),
   *  0,
   *  "estimate"
   *)
   */
  @Multiline
  private static String sketchEstimate;

  @Test
  public void provides_count_estimate() {
    Long estimate = (Long) StellarProcessorUtils.run(sketchEstimate, values);
    assertThat("Incorrect estimate returned", estimate, equalTo(2L));
  }

  /**
   *FREQUENCY_SKETCH_UNPACK(
   *  FREQUENCY_SKETCH_ESTIMATE(
   *    FREQUENCY_SKETCH_UPDATE(
   *      FREQUENCY_SKETCH_UPDATE(
   *        FREQUENCY_SKETCH_UPDATE(
   *          FREQUENCY_SKETCH_INIT(4),
   *          val1
   *        ),
   *        val2
   *      ),
   *      val3
   *    )
   *  ),
   *  1,
   *  "estimate"
   *)
   */
  @Multiline
  private static String sketchSecondItem;

  @Test
  public void provides_count_estimate_on_second_item() {
    Long estimate = (Long) StellarProcessorUtils.run(sketchSecondItem, values);
    assertThat("Incorrect estimate returned", estimate, equalTo(1L));
  }

}
