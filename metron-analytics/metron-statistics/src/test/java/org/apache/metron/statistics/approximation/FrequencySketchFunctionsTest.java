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
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.metron.statistics.approximation.FrequencySketch.ItemEstimate;
import org.apache.metron.statistics.approximation.FrequencySketchFunctions.Estimate;
import org.apache.metron.statistics.approximation.FrequencySketchFunctions.Init;
import org.apache.metron.statistics.approximation.FrequencySketchFunctions.Merge;
import org.apache.metron.statistics.approximation.FrequencySketchFunctions.Update;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class FrequencySketchFunctionsTest {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void initializes_function() {
    FrequencySketch fi = (FrequencySketch) new Init().apply(ImmutableList.of(4));
    assertThat(fi, notNullValue());
  }

  @Test
  public void init_throws_exception_on_missing_params() {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Must provide max map size");
    new Init().apply(ImmutableList.of());
  }

  @Test
  public void init_throws_exception_on_bad_params() {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(
        "The value of the parameter \"maxMapSize\" must be a positive integer-power of 2 and greater than 0");
    new Init().apply(ImmutableList.of("jello"));
  }

  @Test
  public void returns_top_results() {
    FrequencySketch fi = (FrequencySketch) new Init().apply(ImmutableList.of(2048));
    int count = 0;
    for (int i=0; i<200; i++) {
      for (int j=200-i; j>0; j--) {
        count++;
        fi = (FrequencySketch) new Update().apply(ImmutableList.of(fi, j));
      }
    }
    for (int i=0; i<197; i++) {
      fi = (FrequencySketch) new Update().apply(ImmutableList.of(fi, 200));
    }

    List<Object> topItems = (List) new Estimate().apply(ImmutableList.of(fi));
    System.out.println("count: " + count);
    System.out.println(Arrays.toString(topItems.toArray()));
    assertThat(topItems.size(), equalTo(4));
  }

  @Test
  public void calculates_exact_frequencies_for_specific_object() {
    FrequencySketch fi = (FrequencySketch) new Init().apply(ImmutableList.of(8));
    fi = (FrequencySketch) new Update().apply(ImmutableList.of(fi, "a"));
    fi = (FrequencySketch) new Update().apply(ImmutableList.of(fi, "a"));
    fi = (FrequencySketch) new Update().apply(ImmutableList.of(fi, ImmutableList.of("a", "a")));
    fi = (FrequencySketch) new Update().apply(ImmutableList.of(fi, "b"));
    fi = (FrequencySketch) new Update().apply(ImmutableList.of(fi, "b"));
    fi = (FrequencySketch) new Update().apply(ImmutableList.of(fi, ImmutableList.of("b", "b")));
    {
      List<ItemEstimate> itemEstimates = (List) new Estimate()
          .apply(ImmutableList.of(fi, "a"));
      ItemEstimate itemEstimate = itemEstimates.get(0);
      assertThat((String) itemEstimate.getItem(), equalTo("a"));
      assertThat(itemEstimate.getEstimate(), equalTo(4L));
    }
    {
      List<ItemEstimate> itemEstimates = (List) new Estimate()
          .apply(ImmutableList.of(fi, "b"));
      ItemEstimate itemEstimate = itemEstimates.get(0);
      assertThat((String) itemEstimate.getItem(), equalTo("b"));
      assertThat(itemEstimate.getEstimate(), equalTo(4L));
    }
  }

  @Test
  public void update_throws_exception_on_missing_sketch_param() {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Must provide FrequencySketch object and item to update sketch with.");
    new Update().apply(ImmutableList.of());
  }

  @Test
  public void update_throws_exception_on_bad_sketch_param() {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Must provide FrequencySketch object and item to update sketch with.");
    new Update().apply(ImmutableList.of("imnotasketch"));
  }

  @Test
  public void estimate_throws_exception_on_missing_sketch_param() {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Must provide FrequencySketch as first arg.");
    new Estimate().apply(ImmutableList.of());
  }

  @Test
  public void estimate_throws_exception_on_bad_sketch_param() {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Must provide FrequencySketch as first arg.");
    List listWithNull = new ArrayList();
    listWithNull.add(null);
    new Estimate().apply(listWithNull);
  }

  @Test
  public void calculates_exact_frequencies_of_many_objects() {
    FrequencySketch fi = (FrequencySketch) new Init().apply(ImmutableList.of(16));
    fi = (FrequencySketch) new Update().apply(ImmutableList.of(fi, "a"));
    fi = (FrequencySketch) new Update().apply(ImmutableList.of(fi, "b"));
    fi = (FrequencySketch) new Update().apply(ImmutableList.of(fi, "c"));
    fi = (FrequencySketch) new Update().apply(ImmutableList.of(fi, 1));
    fi = (FrequencySketch) new Update().apply(ImmutableList.of(fi, 2));
    fi = (FrequencySketch) new Update().apply(ImmutableList.of(fi, 3));
    fi = (FrequencySketch) new Update().apply(ImmutableList.of(fi, 1L));
    fi = (FrequencySketch) new Update().apply(ImmutableList.of(fi, 2L));
    fi = (FrequencySketch) new Update().apply(ImmutableList.of(fi, 3L));
    List<ItemEstimate> itemEstimates = (List) new Estimate().apply(ImmutableList.of(fi));
    assertThat(itemEstimates.size(), equalTo(9));
    for (ItemEstimate itemEstimate : itemEstimates) {
      assertThat(itemEstimate.getEstimate(), equalTo(1L));
    }
  }

  @Test
  public void merges_frequency_checks() {
    FrequencySketch fi1 = (FrequencySketch) new Init().apply(ImmutableList.of(16));
    fi1 = (FrequencySketch) new Update().apply(ImmutableList.of(fi1, "a"));
    fi1 = (FrequencySketch) new Update().apply(ImmutableList.of(fi1, "a"));
    fi1 = (FrequencySketch) new Update().apply(ImmutableList.of(fi1, "a"));

    FrequencySketch fi2 = (FrequencySketch) new Init().apply(ImmutableList.of(16));
    fi2 = (FrequencySketch) new Update().apply(ImmutableList.of(fi2, "a"));
    fi2 = (FrequencySketch) new Update().apply(ImmutableList.of(fi2, "a"));
    fi2 = (FrequencySketch) new Update().apply(ImmutableList.of(fi2, "a"));

    FrequencySketch mergedFi = (FrequencySketch) new Merge()
        .apply(ImmutableList.of(ImmutableList.of(fi1, fi2), 16));
    List<ItemEstimate> itemEstimates = (List) new Estimate()
        .apply(ImmutableList.of(mergedFi, "a"));
    assertThat(itemEstimates.get(0).getEstimate(), equalTo(6L));
  }

  @Test
  public void merge_with_self_is_identity_function() {
    FrequencySketch fi = (FrequencySketch) new Init().apply(ImmutableList.of(16));
    fi = (FrequencySketch) new Update().apply(ImmutableList.of(fi, "a"));
    FrequencySketch mergedFi = (FrequencySketch) new Merge()
        .apply(ImmutableList.of(ImmutableList.of(fi), 16));
    List<ItemEstimate> itemEstimates = (List) new Estimate().apply(ImmutableList.of(mergedFi, "a"));
    assertThat(itemEstimates.get(0).getEstimate(), equalTo(1L));
    itemEstimates = (List) new Estimate().apply(ImmutableList.of(mergedFi, "b"));
    assertThat(itemEstimates.get(0).getEstimate(), equalTo(0L));
  }

  @Test
  public void merge_throws_exception_on_missing_sketch_param() {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Must pass list of FrequencySketches and maxMapSize.");
    new Merge().apply(ImmutableList.of());
  }

  @Test
  public void merge_throws_exception_on_bad_sketch_param() {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(
        "Must provide single FrequencySketch or List of FrequencySketches as first arg.");
    List listWithNull = new ArrayList();
    listWithNull.add(null);
    listWithNull.add(24L);
    new Merge().apply(listWithNull);
  }

  @Test
  public void merge_throws_exception_on_missing_maxMapSize_param() {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Must pass list of FrequencySketches and maxMapSize.");
    Object fi = new Init().apply(ImmutableList.of(16));
    new Merge().apply(ImmutableList.of(fi));
  }

  @Test
  public void merge_throws_exception_on_bad_maxMapSize_param() {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Unable to set maxMapSize parameter from 'blah'.");
    Object fi = new Init().apply(ImmutableList.of(16));
    new Merge().apply(ImmutableList.of(fi, "blah"));
  }

}
