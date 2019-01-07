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

package org.apache.metron.parsers

import java.util
import java.util.Properties

import org.apache.metron.common.configuration.{ConfigurationsUtils, ParserConfigurations, SensorParserConfig}
import org.apache.metron.common.utils.JSONUtils
import org.apache.metron.integration.{BaseIntegrationTest, ComponentRunner}
import org.apache.metron.integration.components.{KafkaComponent, ZKServerComponent}

object TestParserApplication extends BaseIntegrationTest {
  var sensorType = "csv"
  var sensorTopic = "input"
  var outputTopic = "output"
  var zkComponentName = "zk"
  var kafkaComponentName = "kafka"
  var parserConfigurations: ParserConfigurations = new ParserConfigurations()

  var parserConfigJSON: String =
    s"""
       | {
       |   "parserClassName": "org.apache.metron.parsers.csv.CSVParser",
       |   "sensorTopic": "$sensorTopic",
       |   "outputTopic": "$outputTopic",
       |   "errorTopic": "error",
       |   "parserConfig": {
       |     "columns" : {
       |       "col1": 0,
       |       "col2": 1,
       |       "col3": 2
       |     }
       |   }
       | }
    """.stripMargin

  var parserConfigJSONUpdate: String =
    s"""
       | {
       |   "parserClassName": "org.apache.metron.parsers.csv.CSVParser",
       |   "sensorTopic": "$sensorTopic",
       |   "outputTopic": "$outputTopic",
       |   "errorTopic": "error",
       |   "parserConfig": {
       |     "columns" : {
       |       "col1": 0,
       |       "col2": 1,
       |       "col3": 2,
       |       "col4": 3
       |     }
       |   }
       | }
    """.stripMargin

  def main(args: Array[String]) {
    // Setup the local components
    val topologyProperties: Properties = new Properties
    val zkServerComponent: ZKServerComponent = BaseIntegrationTest.getZKServerComponent(topologyProperties)
    val kafkaComponent: KafkaComponent = BaseIntegrationTest.getKafkaComponent(topologyProperties, new util.ArrayList[KafkaComponent.Topic]() {})
    topologyProperties.setProperty("kafka.broker", kafkaComponent.getBrokerList)

    val runner: ComponentRunner = new ComponentRunner.Builder()
      .withComponent(zkComponentName, zkServerComponent)
      .withComponent(kafkaComponentName, kafkaComponent)
      .withMillisecondsBetweenAttempts(5000)
      .withCustomShutdownOrder(Array[String](kafkaComponentName, zkComponentName)).withNumRetries(10).build
    runner.start()

    // Load messages
    kafkaComponent.writeMessages(sensorTopic, "1_c1, 1_c2, 1_c3")
    kafkaComponent.writeMessages(sensorTopic, "2_c1, 2_c2, 2_c3")
    kafkaComponent.writeMessages(sensorTopic, "brokenMessage")

    // Set up ZK configs
    println("Setting up ZK and configs from driver")
    val connectionStr = zkServerComponent.getConnectionString
    val brokerList = kafkaComponent.getBrokerList
    val parserConfig: SensorParserConfig = JSONUtils.INSTANCE.load(parserConfigJSON, classOf[SensorParserConfig])
    ConfigurationsUtils.writeSensorParserConfigToZookeeper(sensorType, parserConfig, connectionStr)
    ConfigurationsUtils.writeGlobalConfigToZookeeper(new util.HashMap[String, AnyRef](), connectionStr)

    ParserApplication.main(Array(
      zkServerComponent.getConnectionString,
      brokerList,
      sensorType,
      "test"
    ))

    runner.stop()
  }
}
