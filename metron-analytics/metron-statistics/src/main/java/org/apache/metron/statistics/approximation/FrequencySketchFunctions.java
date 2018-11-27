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

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.List;
import org.apache.metron.statistics.approximation.FrequencySketch.ItemEstimate;
import org.apache.metron.stellar.common.utils.ConversionUtils;
import org.apache.metron.stellar.dsl.BaseStellarFunction;
import org.apache.metron.stellar.dsl.Stellar;

public class FrequencySketchFunctions {

  /**
   * Initializes the summary statistics.
   *
   * Initialization can occur from either FREQUENCY_SKETCH_INIT and FREQUENCY_SKETCH_UPDATE.
   */
  private static FrequencySketch sketchInit(List<Object> args) {
    int maxMapSize = 0;
    if (args.size() > 0 && args.get(0) instanceof Number) {
      maxMapSize = ConversionUtils.convert(args.get(0), Integer.class);
    }
    if (maxMapSize > 0) {
      return new FrequencySketch(maxMapSize);
    } else {
      throw new IllegalArgumentException("The value of the parameter \"maxMapSize\" must be a positive integer-power of 2 and greater than 0");
    }
  }

  @Stellar(namespace = "FREQUENCY_SKETCH"
      , name = "INIT"
      , description = "Initializes the frequency estimator sketch. maxMapSize must be > 0 and a power of 2."
      , params = {
      "maxMapSize - Determines the physical size of the internal hash map managed by this sketch and must be a power of 2. The maximum capacity of this internal hash map is 0.75 times * maxMapSize. Both the ultimate accuracy and size of this sketch are a function of maxMapSize."
  }
      , returns = "A new frequency estimator sketch"
  )
  public static class Init extends BaseStellarFunction {

    @Override
    public Object apply(List<Object> args) {
      if (args.size() != 1) {
        throw new IllegalArgumentException("Must provide max map size");
      }
      return sketchInit(args);
    }
  }

  @Stellar(namespace = "FREQUENCY_SKETCH"
      , name = "UPDATE"
      , description = "Add a value to the frequency estimator sketch."
      , params = {
      "frequencySketch - the frequency estimator sketch to add a value to. If null, then a new one is initialized."
      , "value+ - value to add to the sketch. Takes a single item or a list"
  }
      , returns = "A new frequency estimator sketch."
  )
  public static class Update extends BaseStellarFunction {

    @Override
    public Object apply(List<Object> args) {
      if (args.size() != 2) {
        throw new IllegalArgumentException(
            "Must provide FrequencySketch object and item to update sketch with.");
      }
      FrequencySketch fi = ConversionUtils.convert(args.get(0), FrequencySketch.class);
      if (null == fi) {
        fi = sketchInit(args);
      }
      Object secondArg = args.get(1);
      if (secondArg instanceof List) {
        fi.updateAll((List) secondArg);
      } else {
        fi.update(secondArg);
      }
      return fi;
    }
  }

  @Stellar(namespace = "FREQUENCY_SKETCH"
      , name = "ESTIMATE"
      , description = "Calculates the estimated occurrence of this item(s) in the frequency estimator sketch."
      , params = {
      "frequencySketch - the frequency estimator sketch to use for calculating the estimate"
      , "fields - Optional list of GeoIP fields to grab. Options are: item, estimate, upperbound, lowerbound"
  }
      , returns = "If a single field is requested a string of the field. If multiple fields a map of string of the fields, and null otherwise."
  )
  public static class Estimate extends BaseStellarFunction {

    @Override
    public Object apply(List<Object> args) {
      if (args.size() < 1) {
        throw new IllegalArgumentException("Must provide FrequencySketch as first arg.");
      }
      FrequencySketch fi = ConversionUtils.convert(args.get(0), FrequencySketch.class);
      if (null == fi) {
        throw new IllegalArgumentException("Must provide FrequencySketch as first arg.");
      }
      List<Object> frequencies = new ArrayList<>();
      if (args.size() == 2) {
        Object item = args.get(1);
        frequencies.add(new ItemEstimate(item, fi.getEstimate(item)));
      } else {
        return fi.getEstimates();
      }
      return frequencies;
    }
  }

  @Stellar(namespace = "FREQUENCY_SKETCH"
      , name = "MERGE"
      , description = "Merge frequency estimator sketches together. An empty list returns null."
      , params = {
      "frequencySketches - List of frequency estimator sketches to merge. Takes a single sketch or a list."
      ,
      "maxMapSize - Determines the physical size of the internal hash map managed by this sketch and must be a power of 2. The maximum capacity of this internal hash map is 0.75 times * maxMapSize. Both the ultimate accuracy and size of this sketch are a function of maxMapSize."
  }
      , returns = "Merged frequency estimator sketch."
  )
  public static class Merge extends BaseStellarFunction {

    @Override
    public Object apply(List<Object> args) {
      if (args.size() != 2) {
        throw new IllegalArgumentException(
            "Must pass list of FrequencySketches and maxMapSize.");
      }
      List<Object> sketches = new ArrayList<>();
      if (args.get(0) instanceof List) {
        sketches = (List) args.get(0); // list of sketches
      } else {
        FrequencySketch fi = ConversionUtils.convert(args.get(0), FrequencySketch.class);
        if (null == fi) {
          throw new IllegalArgumentException(
              "Must provide single FrequencySketch or List of FrequencySketches as first arg.");
        }
        sketches.add(fi); // single sketch
      }
      if (sketches.size() == 0) {
        return null;
      }
      Integer maxMapSize = ConversionUtils.convert(args.get(1), Integer.class);
      if (maxMapSize == null) {
        throw new IllegalArgumentException(
            format("Unable to set maxMapSize parameter from '%s'.", args.get(1)));
      }

      List<FrequencySketch> convertedSketches = ConversionUtils
          .convertList(sketches, FrequencySketch.class);
      return FrequencySketch.merge(convertedSketches, maxMapSize);
    }
  }

  @Stellar(namespace = "FREQUENCY_SKETCH"
      , name = "UNPACK"
      , description = "Unpacks results."
      , params = {
      "frequencySketches - List of frequency estimator sketches to merge. Takes a single sketch or a list."
      , "index - index of result"
      , "feature - item, estimate, upper, lower"
  }
      , returns = "Value of exracted item."
  )
  public static class Unpacker extends BaseStellarFunction {

    @Override
    public Object apply(List<Object> args) {
      if (args.size() != 3) {
        throw new IllegalArgumentException(
            "Must pass FrequencySketch result, index, and feature.");
      }
      List<ItemEstimate> estimates;
      if (args.get(0) instanceof List) {
        estimates = ConversionUtils.convertList((List) args.get(0), ItemEstimate.class);
        if (null == estimates) {
          throw new IllegalArgumentException(
              "Must provide single FrequencySketch or List of FrequencySketches as first arg.");
        }
      } else {
        throw new IllegalArgumentException(
            "Must provide single FrequencySketch or List of FrequencySketches as first arg.");
      }
      Integer index = ConversionUtils.convert(args.get(1), Integer.class);
      if (index == null) {
        throw new IllegalArgumentException(
            format("Unable to set index parameter from '%s'.", args.get(1)));
      }
      String feature = ConversionUtils.convert(args.get(2), String.class);
      if (feature == null) {
        throw new IllegalArgumentException(
            format("Unable to set feature parameter from '%s'.", args.get(2)));
      }
      // Stellar validation will cause problems if we don't check size here
      if (estimates.size() > 0) {
        ItemEstimate ie = estimates.get(index);
        switch (feature) {
          case "item":
            return ie.getItem();
          case "estimate":
            return ie.getEstimate();
          case "upper":
            return ie.getUpperBound();
          case "lower":
            return ie.getLowerBound();
          default:
            throw new IllegalArgumentException(format("Unknown option '%s'", feature));
        }
      } else {
        return null;
      }
    }
  }
}
