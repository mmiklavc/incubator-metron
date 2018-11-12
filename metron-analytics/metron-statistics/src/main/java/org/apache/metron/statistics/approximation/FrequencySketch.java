/**
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
package org.apache.metron.statistics.approximation;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.yahoo.sketches.frequencies.ErrorType;
import com.yahoo.sketches.frequencies.ItemsSketch;
import com.yahoo.sketches.frequencies.ItemsSketch.Row;
import java.util.ArrayList;
import java.util.List;

/**
 * Wraps an implementation of the Yahoo Data Sketches frequent items sketch implementation for Java.
 * https://datasketches.github.io/docs/FrequentItems/FrequentItemsOverview.html
 */
public class FrequencySketch implements KryoSerializable {

  public static class ItemEstimate {

    private Object item;
    private long estimate;
    private long upperBound;
    private long lowerBound;

    public ItemEstimate() {
    }

    public ItemEstimate(Object item, long estimate) {
      this.item = item;
      this.estimate = estimate;
    }

    public ItemEstimate setItem(Object item) {
      this.item = item;
      return this;
    }

    public ItemEstimate setEstimate(long estimate) {
      this.estimate = estimate;
      return this;
    }

    public ItemEstimate setUpperBound(long upperBound) {
      this.upperBound = upperBound;
      return this;
    }

    public ItemEstimate setLowerBound(long lowerBound) {
      this.lowerBound = lowerBound;
      return this;
    }

    public Object getItem() {
      return item;
    }

    public long getEstimate() {
      return estimate;
    }

    public long getUpperBound() {
      return upperBound;
    }

    public long getLowerBound() {
      return lowerBound;
    }
  }

  private final ItemsSketch<Object> sketch;

  public FrequencySketch(int maxMapSize) {
    this.sketch = new ItemsSketch<>(maxMapSize);
  }

  public void update(Object item) {
    sketch.update(item);
  }

  public void updateAll(List<Object> items) {
    for (Object item : items) {
      sketch.update(item);
    }
  }

  public long getEstimate(Object item) {
    return sketch.getEstimate(item);
  }

  public List<ItemEstimate> getEstimates() {
    Row<Object>[] frequentItems = sketch.getFrequentItems(ErrorType.NO_FALSE_NEGATIVES);
    List<ItemEstimate> itemEstimates = new ArrayList<>();
    for (Row<Object> row : frequentItems) {
      ItemEstimate itemEstimate = new ItemEstimate().setItem(row.getItem())
          .setEstimate(row.getEstimate()).setUpperBound(row.getUpperBound())
          .setLowerBound(row.getLowerBound());
      itemEstimates.add(itemEstimate);
    }
    return itemEstimates;
  }

  public static FrequencySketch merge(List<FrequencySketch> sketches, int maxMapSize) {
    FrequencySketch newSketch = new FrequencySketch(maxMapSize);
    for (FrequencySketch fi : sketches) {
      if (null != fi.sketch) {
        newSketch.sketch.merge(fi.sketch);
      }
    }
    return newSketch;
  }

  @Override
  public void write(Kryo kryo, Output output) {

  }

  @Override
  public void read(Kryo kryo, Input input) {

  }

}
