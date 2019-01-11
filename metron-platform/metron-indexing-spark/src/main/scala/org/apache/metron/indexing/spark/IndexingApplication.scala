package org.apache.metron.indexing.spark

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

import java.util
import java.util.function.Supplier
import java.util.{Optional, Properties, function}

import org.apache.curator.RetryPolicy
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.metron.common.Constants
import org.apache.metron.common.configuration.{ConfigurationsUtils, IndexingConfigurations}
import org.apache.metron.common.utils.{JSONUtils, MessageUtils}
import org.apache.metron.common.zookeeper.{ConfigurationsCache, ZKConfigurationsCache}
import org.apache.metron.indexing.dao.AccessConfig
import org.apache.metron.indexing.dao.update.Document
import org.apache.metron.solr.SolrConstants
import org.apache.metron.solr.dao.SolrDao
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.json.simple.JSONObject
import org.json.simple.parser.JSONParser


object IndexingApplication {

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: IndexingApplication zookeeperUrl brokerList topics")
      System.exit(1)
    }

    val Array(zookeeperUrl, brokers, topics) = args



    // Create context with 1 second batch interval
    // TODO Need to set this based on sensor
    val sparkConf = new SparkConf().setAppName("IndexingApplication").setMaster("local")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    println("topicSet = " + topicsSet)
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> "spark-indexing-application",
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
        val retryPolicy: RetryPolicy = new ExponentialBackoffRetry(1000, 3)
        var client = CuratorFrameworkFactory.newClient(zookeeperUrl, retryPolicy)
        client.start()

        val configurationsCache: ConfigurationsCache = new ZKConfigurationsCache(client, ZKConfigurationsCache.ConfiguredTypes.INDEXING)
        configurationsCache.start()

        val solrDao: SolrDao = new SolrDao()
        val globalConfigSupplier: Supplier[util.Map[String, AnyRef]] = new Supplier[util.Map[String, AnyRef]] {
          override def get(): util.Map[String, AnyRef] = {
            configurationsCache.get[IndexingConfigurations](classOf[IndexingConfigurations]).getGlobalConfig
          }
        }

        val indexSupplier: util.function.Function[String, String] = new function.Function[String, String] {
          override def apply(t: String): String = {
            "bro"
          }
        }
        val accessConfig: AccessConfig = new AccessConfig()
        accessConfig.setGlobalConfigSupplier(globalConfigSupplier)
        accessConfig.setIndexSupplier(indexSupplier)
        solrDao.init(accessConfig)

        partition.foreach {
          record =>
            println(record.value())

            val message: java.util.Map[String, Object] = JSONUtils.INSTANCE.load(record.value(), classOf[java.util.Map[String, Object]])
            val sensorType: String = message.get(Constants.SENSOR_TYPE).asInstanceOf[String]
            val indexingConfiguration: IndexingConfigurations = configurationsCache.get[IndexingConfigurations](classOf[IndexingConfigurations])

            val document: Document = Document.fromJSON(message)
            solrDao.update(document, Optional.of(indexingConfiguration.getIndex(sensorType, SolrConstants.SOLR_WRITER_NAME)))
        }
        client.close()
      }
    }

    ssc.start()
//    ssc.awaitTerminationOrTimeout(6000)
//    ssc.stop()
    ssc.awaitTermination()
  }
}

