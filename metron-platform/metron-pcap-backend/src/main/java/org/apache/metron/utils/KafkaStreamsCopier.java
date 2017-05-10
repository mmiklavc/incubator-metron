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

package org.apache.metron.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.metron.common.utils.JSONUtils;

public class KafkaStreamsCopier {

  public static void main(String[] args) throws Exception {
    Map<String, Object> streamsConfiguration = new HashMap<>();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "copy_test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
        "y134.l42scl.hortonworks.com:6667,y135.l42scl.hortonworks.com:6667,y136.l42scl.hortonworks.com:6667");
    streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG,
        "y113.l42scl.hortonworks.com:2181,y114.l42scl.hortonworks.com:2181,y115.l42scl.hortonworks.com:2181");
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    streamsConfiguration.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
    streamsConfiguration.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        "org.apache.kafka.streams.processor.WallclockTimestampExtractor");

    if (args.length > 2) {
      Map<String, Object> additionalConfig = JSONUtils.INSTANCE.load(new File(args[2]),
          new TypeReference<Map<String, Object>>() {
          });
      streamsConfiguration.putAll(additionalConfig);
    }

    KStreamBuilder builder = new KStreamBuilder();
    KStream<Bytes, Bytes> textLines = builder.stream(args[0]);
    textLines.to(args[1]);
    StreamsConfig c = new StreamsConfig(streamsConfiguration);
    KafkaStreams streams = new KafkaStreams(builder, c);
    streams.setUncaughtExceptionHandler((t, e) -> e.printStackTrace());

    streams.start();

    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }
}
