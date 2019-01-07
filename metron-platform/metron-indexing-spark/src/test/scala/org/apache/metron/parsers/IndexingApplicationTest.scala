package org.apache.metron.parsers

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

import java.nio.charset.StandardCharsets
import java.util
import java.util.function.Supplier
import java.util.{Optional, Properties, function}

import org.apache.curator.RetryPolicy
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.metron.common.configuration.{ConfigurationsUtils, IndexingConfigurations}
import org.apache.metron.common.message.metadata.RawMessage
import org.apache.metron.common.utils.JSONUtils
import org.apache.metron.indexing.dao.AccessConfig
import org.apache.metron.indexing.dao.update.Document
import org.apache.metron.indexing.spark.IndexingApplication
import org.apache.metron.integration.components.{KafkaComponent, ZKServerComponent}
import org.apache.metron.integration.{BaseIntegrationTest, ComponentRunner}
import org.apache.metron.solr.SolrConstants
import org.apache.metron.solr.SolrConstants.SOLR_ZOOKEEPER
import org.apache.metron.solr.dao.SolrDao
import org.apache.metron.solr.integration.components.SolrComponent
import org.apache.metron.stellar.dsl.Context.{Capabilities, Capability}
import org.apache.metron.stellar.dsl.{Context, StellarFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.{JavaConversions, mutable}


object IndexingApplicationTest extends BaseIntegrationTest {
  var sensorType = "csv"
  var sensorTopic = "input"
  var outputTopic = "output"
  var zkComponentName = "zk"
  var kafkaComponentName = "kafka"
  var solrComponentName = "solr"

  var indexingConfigJSON: String =
    s"""
      | {
      |   "hdfs": {
      |     "index": "$sensorTopic",
      |     "batchSize": 5,
      |     "enabled": true
      |   },
      |   "solr": {
      |     "index": "$sensorTopic",
      |     "batchSize": 5,
      |     "enabled": true
      |   }
      | }
    """.stripMargin

  def main(args: Array[String]) {
    // TODO fix this. For now hardcode the values
    //      if (args.length < 3) {
    //        System.err.println(
    //          s"""
    //             |Usage: DirectKafkaWordCount <brokers> <topics>
    //             |  <brokers> is a list of one or more Kafka brokers
    //             |  <groupId> is a consumer group name to consume from topics
    //             |  <topics> is a list of one or more kafka topics to consume from
    //             |
    //          """.stripMargin)
    //        System.exit(1)
    //      }

    // Setup the local components
    val topologyProperties: Properties = new Properties
    val zkServerComponent: ZKServerComponent = BaseIntegrationTest.getZKServerComponent(topologyProperties)
    val kafkaComponent: KafkaComponent = BaseIntegrationTest.getKafkaComponent(topologyProperties, new util.ArrayList[KafkaComponent.Topic]() {})
    topologyProperties.setProperty("kafka.broker", kafkaComponent.getBrokerList)
    val solrComponent: SolrComponent = new SolrComponent.Builder().addInitialCollection("bro", "metron-platform/metron-solr/src/main/config/schema/bro").build()

    val runner: ComponentRunner = new ComponentRunner.Builder()
      .withComponent(zkComponentName, zkServerComponent)
      .withComponent(kafkaComponentName, kafkaComponent)
      .withComponent(solrComponentName, solrComponent)
      //        .withComponent("config", configUploadComponent)
      .withMillisecondsBetweenAttempts(5000)
      .withCustomShutdownOrder(Array[String](kafkaComponentName, zkComponentName, solrComponentName)).withNumRetries(10).build
    runner.start()

    // Load messages
    kafkaComponent.writeMessages(sensorTopic, "{\"guid\": \"1\", \"bro_timestamp\":\"1402307733.473\",\"status_code\":200,\"method\":\"GET\",\"source.type\":\"bro\"}")
    kafkaComponent.writeMessages(sensorTopic, "{\"guid\": \"2\", \"bro_timestamp\":\"1402400000.000\",\"status_code\":400,\"method\":\"POST\",\"source.type\":\"bro\"}")
    //kafkaComponent.writeMessages(sensorTopic, "brokenMessage")

    // Ensure we can read messages
//    println("messages in Kafka:")
//    kafkaComponent.readMessages(sensorTopic).toList.foreach { raw =>
//      val str = new String(raw)
//      println(str)
//    }

    // Set up ZK configs
    val connectionStr = zkServerComponent.getConnectionString
    val brokerList = kafkaComponent.getBrokerList // TODO make sure this is properly formatted
    val solrZookeeper = solrComponent.getZookeeperUrl
    println(connectionStr)
    println(brokerList)
    println(solrZookeeper)
    val indexingConfig = JSONUtils.INSTANCE.load(indexingConfigJSON, classOf[java.util.Map[String, Object]])
    ConfigurationsUtils.writeSensorIndexingConfigToZookeeper(sensorType, indexingConfig, connectionStr)
    val globalConfig = new util.HashMap[String, AnyRef]()
    globalConfig.put(SOLR_ZOOKEEPER, solrZookeeper)
    ConfigurationsUtils.writeGlobalConfigToZookeeper(globalConfig, connectionStr)

    val args = Array(connectionStr, brokerList, sensorTopic)

    IndexingApplication.main(args)

    println("messages in Solr:")
    solrComponent.getAllIndexedDocs("bro").toList.foreach { raw =>
      println(raw)
    }

    System.exit(0)
  }
}

