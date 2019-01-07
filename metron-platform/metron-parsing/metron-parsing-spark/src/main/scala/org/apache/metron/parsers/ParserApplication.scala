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

import java.nio.charset.StandardCharsets
import java.util.Collections
import java.util.function.Supplier

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.metron.common.configuration.{ConfigurationsUtils, ParserConfigurations}
import org.apache.metron.common.message.metadata.RawMessage
import org.apache.metron.common.zookeeper.{ConfigurationsCache, ZKConfigurationsCache}
import org.apache.metron.stellar.dsl.Context.{Capabilities, Capability}
import org.apache.metron.stellar.dsl.{Context, StellarFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._

import scala.collection.JavaConversions
import scala.collection.JavaConversions._


object ParserApplication {
  def main(args: Array[String]) {
    if (args.length < 3) {
      printHelp()
    }

    val zookeeperUrl = args(0)
    val brokers = args(1)
    val sensorType = args(2)
    val groupId = sensorType + "_parser"
    val test = args.length > 3 && "test".equals(args(3))

    // Create context with 1 second batch interval
    val sparkConf = new SparkConf().setAppName("Metron_" + sensorType)
    if (test) {
      sparkConf.setMaster("local")
    }
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // Pull back the topic
    val retryPolicy = new ExponentialBackoffRetry(1000, 3)
    val client = CuratorFrameworkFactory.newClient(zookeeperUrl, retryPolicy)
    client.start()
    // Right now this is only a single topic
    val topicsSet = Set(ConfigurationsUtils.readSensorParserConfigFromZookeeper(sensorType, client).getSensorTopic)

    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest")
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    messages.foreachRDD { rdd =>
      rdd.foreachPartition { partition =>
        // Setup the ZK Client used for handling updates to the config in flight.
        val retryPolicy = new ExponentialBackoffRetry(1000, 3)
        val client = CuratorFrameworkFactory.newClient(zookeeperUrl, retryPolicy)
        client.start()
        //        val cache = prepCache(client, zookeeperUrl)
        val configurationsCache: ConfigurationsCache = new ZKConfigurationsCache(client, ZKConfigurationsCache.ConfiguredTypes.PARSER)
        configurationsCache.start()


        // Get Global Configs
        val globalConfigs = configurationsCache.get[ParserConfigurations](classOf[ParserConfigurations]).getGlobalConfig

        // Set up the capabilities
        val builder = new Context.Builder()
        // This syntax is confusing, but seemingly necessary to
        // a) use a reserved keyword, 'with', as a method name (i.e. use backticks)
        // b) Choose the overloaded method correctly (i.e. type the arguments)
        // c) Pass the Java lambda to Scala correctly. Theoretically it would work in 2.12 (i.e. new Capability explicitly)
        // Taken from ParserBolt.initializeStellar, with some of the indirection used to hide ZK updates and caching and such removed.
        builder.`with`(Context.Capabilities.ZOOKEEPER_CLIENT: Enum[Capabilities], new Capability {
          override def get(): AnyRef = client
        })
        builder.`with`(Context.Capabilities.GLOBAL_CONFIG: Enum[Capabilities], new Capability {
          override def get(): AnyRef = globalConfigs
        })
        builder.`with`(Context.Capabilities.STELLAR_CONFIG: Enum[Capabilities], new Capability {
          override def get(): AnyRef = globalConfigs
        })

        // Setup Stellar
        val stellarContext: Context = builder.build
        StellarFunctions.initialize(stellarContext)

        val parserRunner = new ParserRunnerImpl(JavaConversions.setAsJavaSet(Set(sensorType)))
        // Do similar to above with explicit new Supplier
        val parserSupplier = new Supplier[ParserConfigurations] {
          override def get(): ParserConfigurations = configurationsCache.get[ParserConfigurations](classOf[ParserConfigurations])
        }
        parserRunner.init(parserSupplier, stellarContext)

        val producer = new KafkaProducer[String, String](kafkaParams)

        // Actually do the work on a per message basis
        partition.foreach {
          record =>
            // TODO don't just ignore metadata
            val message = new RawMessage(record.value().getBytes(StandardCharsets.UTF_8), Collections.emptyMap())
            val result = parserRunner.execute(sensorType, message, parserSupplier.get())
            println("Messages: " + result.getMessages)
            result.getMessages.foreach {
              resultMessage =>
                val outputTopic = parserSupplier.get().getSensorParserConfig(sensorType).getOutputTopic
                val outputMessage = new ProducerRecord[String, String](outputTopic, null, resultMessage.toJSONString)
                producer.send(outputMessage)
            }
            println("Errors: : " + result.getErrors)
          // Just discard errors
        }
        configurationsCache.close()
        client.close()
      }
    }

    ssc.start()
    if (test) {
      // Hackery for a test mode
      ssc.awaitTerminationOrTimeout(2000)
      println("Terminating")
      ssc.stop()
    }
  }

  private def printHelp(): Unit = {
    System.err.println(
      s"""
         |Usage: ParserApplication <zookeeper_url> <brokers> <sensor_type>
         |  <zookeeper_url> is a comma-separated list of ZooKeeper URLs in the form <node>:<port>
         |  <brokers> is a list of one or more Kafka brokers
         |  <sensor_type> is what sensor will be run.
        """.stripMargin)
    System.exit(1)
  }
}
