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

package org.apache.metron.enrichment

import java.io.File
import java.util
import java.util.Properties

import scala.collection.JavaConverters._
import org.apache.metron.common.configuration.enrichment.SensorEnrichmentConfig
import org.apache.metron.common.configuration.{ConfigurationsUtils, EnrichmentConfigurations, ParserConfigurations, SensorParserConfig}
import org.apache.metron.common.utils.JSONUtils
import org.apache.metron.enrichment.adapters.geo.GeoLiteDatabase
import org.apache.metron.integration.components.{KafkaComponent, ZKServerComponent}
import org.apache.metron.integration.{BaseIntegrationTest, ComponentRunner}

object TestEnrichmentApplication extends BaseIntegrationTest {
  var sensorType = "csv"
  var inputTopic = "input"
  var outputTopic = "output"
  var zkComponentName = "zk"
  var kafkaComponentName = "kafka"
  var enrichmentConfigurations: EnrichmentConfigurations = new EnrichmentConfigurations

  val enrichmentConfigJSON: String =
    """
      |{
      |  "enrichment" : {
      |    "fieldMap": {
      |      "stellar": {
      |         "config" : {
      |           "col1" : "TO_UPPER(col1)",
      |           "col2" : "TO_UPPER(col2)",
      |           "col3" : "TO_UPPER(col3)"
      |         }
      |      }
      |    }
      |  }
      |}
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
    kafkaComponent.writeMessages(inputTopic, """{"original_string":"2_c1, 2_c2, 2_c3","guid":"effb067a-8443-4547-848b-32218e54170f","col2":"2_c2","col3":"2_c3","col1":"2_c1","timestamp":1548138818239,"source.type":"csv"}""")

    // Set up ZK configs
    println("Setting up ZK and configs from driver")
    val connectionStr = zkServerComponent.getConnectionString
    val brokerList = kafkaComponent.getBrokerList
    val enrichmentConfig: SensorEnrichmentConfig = JSONUtils.INSTANCE.load(enrichmentConfigJSON, classOf[SensorEnrichmentConfig])
    ConfigurationsUtils.writeSensorEnrichmentConfigToZookeeper(sensorType, enrichmentConfig, connectionStr)
//    GeoLiteDatabase.GEO_HDFS_FILE -> new File("metron-platform/metron-enrichment/metron-enrichment-common/src/test/resources/GeoLite/GeoIP2-City-Test.mmdb.gz").getAbsolutePath
    val globalConfig: Map[String, AnyRef] = Map(
      GeoLiteDatabase.GEO_HDFS_FILE -> new File("../metron-enrichment-common/src/test/resources/GeoLite/GeoIP2-City-Test.mmdb.gz").getAbsolutePath
    )
    ConfigurationsUtils.writeGlobalConfigToZookeeper(globalConfig.asJava, connectionStr)

    EnrichmentApplication.main(Array(
      zkServerComponent.getConnectionString,
      brokerList,
      inputTopic,
      outputTopic,
      "test"
    ))

    runner.stop()
  }
}
