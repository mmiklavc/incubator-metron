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

import java.nio.charset.StandardCharsets
import java.util.function.Supplier

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.metron.common.Constants
import org.apache.metron.common.configuration.EnrichmentConfigurations
import org.apache.metron.common.configuration.enrichment.SensorEnrichmentConfig
import org.apache.metron.common.utils.MessageUtils
import org.apache.metron.common.zookeeper.{ConfigurationsCache, ZKConfigurationsCache}
import org.apache.metron.enrichment.adapters.host.HostFromJSONListAdapter
import org.apache.metron.enrichment.adapters.stellar.StellarAdapter
import org.apache.metron.enrichment.cache.CacheKey
import org.apache.metron.enrichment.interfaces.EnrichmentAdapter
import org.apache.metron.enrichment.parallel._
import org.apache.metron.stellar.dsl.Context.{Capabilities, Capability}
import org.apache.metron.stellar.dsl.{Context, StellarFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.json.simple.JSONObject
import org.json.simple.parser.JSONParser

import scala.collection.JavaConversions._


object EnrichmentApplication {

  // constants
  final val StellarContextConf = "stellarContext"
  final val ThreadpoolNumThreadsTopologyConf = "metron.threadpool.size"
  final val ThreadpoolTypeTopologyConf = "metron.threadpool.type"
  final val CaptureCacheStats = true


  def main(args: Array[String]) {
    if (args.length < 3) {
      printHelp()
    }

    val zookeeperUrl = args(0)
    val brokers = args(1)
    val inputTopic = args(2)
    var outputTopic: String = if (args.length > 3) args(3) else Constants.INDEXING_TOPIC
    val groupId = "enrichments"
    val test = args.length > 4 && "test".equals(args(4))

    val threadPoolSize = 1
    val threadPoolType = WorkerPoolStrategies.valueOf("FIXED")
    val batchDuration = Seconds(1)
    val maxCacheSize = 100000
    val maxTimeRetain = 10

    // Create context with 1 second batch interval
    val sparkConf = new SparkConf().setAppName("Metron_enrichment")
    if (test) {
      sparkConf.setMaster("local")
    }
    val ssc = new StreamingContext(sparkConf, batchDuration)

    // Pull back the topic
    /**
      * what is this here for?
      * val retryPolicy = new ExponentialBackoffRetry(1000, 3)
      * val client = CuratorFrameworkFactory.newClient(zookeeperUrl, retryPolicy)
      *client.start()
      */

    // Create direct kafka stream with brokers and topics
    var kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer]
    )
    if (test) kafkaParams += (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest")
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set(inputTopic), kafkaParams))

    println("Iterating RDDs")

    messages.foreachRDD { rdd =>
      rdd.foreachPartition { partition =>
        // Scala 2.11 does not handle Suppliers for java 8 lambdas
        val jsonParserSupplier = new Supplier[JSONParser] {
          override def get(): JSONParser = new JSONParser
        }
        // JSONParser is not thread safe
        val jsonParser = ThreadLocal.withInitial[JSONParser](jsonParserSupplier)

        // Setup the ZK Client used for handling updates to the config in flight.
        val retryPolicy = new ExponentialBackoffRetry(1000, 3)
        val client = CuratorFrameworkFactory.newClient(zookeeperUrl, retryPolicy)
        client.start()
        //        val cache = prepCache(client, zookeeperUrl)
        val configurationsCache: ConfigurationsCache = new ZKConfigurationsCache(client, ZKConfigurationsCache.ConfiguredTypes.ENRICHMENT)
        configurationsCache.start()

        // Get Global Configs
        val globalConfig = configurationsCache.get[EnrichmentConfigurations](classOf[EnrichmentConfigurations]).getGlobalConfig

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
          override def get(): AnyRef = globalConfig
        })
        builder.`with`(Context.Capabilities.STELLAR_CONFIG: Enum[Capabilities], new Capability {
          override def get(): AnyRef = globalConfig
        })

        // Setup Stellar
        val stellarContext: Context = builder.build
        StellarFunctions.initialize(stellarContext)

        // HBase info
        /*
        val hbaseConfig = new SimpleHBaseConfig()
          .withProviderImpl("org.apache.metron.hbase.HTableProvider")
          .withHBaseTable("enrichment")
          .withHBaseCF("t")
        */

        val enrichmentsByType: Map[String, EnrichmentAdapter[CacheKey]] = Map(
          //          ("geo", new GeoAdapter),
          ("host", new HostFromJSONListAdapter(
            """[
              |{"ip":"10.1.128.236", "local":"YES", "type":"webserver", "asset_value" : "important"},
              |{"ip":"10.1.128.237", "local":"UNKNOWN", "type":"unknown", "asset_value" : "important"},
              |{"ip":"10.60.10.254", "local":"YES", "type":"printer", "asset_value" : "important"}
              |]""".stripMargin)),
          //          ("hbaseEnrichment", new SimpleHBaseAdapter(hbaseConfig)),
          ("stellar", new StellarAdapter().ofType("ENRICHMENT"))
        )

        for ((typeName, adapter) <- enrichmentsByType) {
          val success = adapter.initializeAdapter(globalConfig)
          if (!success) {
            throw new IllegalStateException("Could not initialize adapter: " + typeName);
          }
        }

        val workerPoolStrategy = WorkerPoolStrategies.FIXED;
        val numThreads: Int = getNumThreads(threadPoolSize);
        val strategy: EnrichmentStrategies = EnrichmentStrategies.ENRICHMENT;
        val nullLog = null // deal with logging later
        ConcurrencyContext.get(strategy).initialize(numThreads, maxCacheSize, maxTimeRetain, workerPoolStrategy, nullLog, CaptureCacheStats);
        val enricher = new ParallelEnricher(enrichmentsByType, ConcurrencyContext.get(strategy), CaptureCacheStats);
        // deal with this later
        // GeoLiteDatabase.INSTANCE.update((String)getConfigurations().getGlobalConfig().get(GeoLiteDatabase.GEO_HDFS_FILE));

        val enrichmentContext = new EnrichmentContext(StellarFunctions.FUNCTION_RESOLVER(), stellarContext);

        val enrichmentConfigSupplier = new Supplier[EnrichmentConfigurations] {
          override def get(): EnrichmentConfigurations = configurationsCache.get[EnrichmentConfigurations](classOf[EnrichmentConfigurations])
        }

        val producer = new KafkaProducer[String, String](kafkaParams)

        println("Iterating records")

        partition.foreach {
          record =>
            println("Received record")
            val raw = new String(record.value().getBytes(StandardCharsets.UTF_8))
            val parsedMessage: JSONObject = jsonParser.get().parse(raw).asInstanceOf[JSONObject] // need cast
            println(parsedMessage.toJSONString)
            try {
              val sourceType = MessageUtils.getSensorType(parsedMessage)
              var sensorEnrichConfig = enrichmentConfigSupplier.get().getSensorEnrichmentConfig(sourceType)
              if (sensorEnrichConfig == null) {
                println("Unable to find SensorEnrichmentConfig for sourceType: " + sourceType)
                sensorEnrichConfig = new SensorEnrichmentConfig
              }
              //This is an existing kludge for the stellar adapter to pass information along.
              //We should figure out if this can be rearchitected a bit.  This smells.
              sensorEnrichConfig.getConfiguration.putIfAbsent(StellarContextConf, stellarContext)
              //              String guid = getGUID(input, message);
              val guid = parsedMessage.get(Constants.GUID).asInstanceOf[String]

              // enrich the message
              val result = enricher.apply(parsedMessage, strategy, sensorEnrichConfig, null)
              var enriched: JSONObject = result.getResult
              enriched = strategy.postProcess(enriched, sensorEnrichConfig, enrichmentContext)
              println("Message: " + enriched.toJSONString)
              val outputMessage = new ProducerRecord[String, String](outputTopic, null, enriched.toJSONString)
              producer.send(outputMessage)
              if (!result.getEnrichmentErrors.isEmpty) {
                println("Errors: : " + result.getEnrichmentErrors)
              }
            }
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
      ssc.stop(stopSparkContext = true, stopGracefully = true)
    } else {
      ssc.awaitTermination()
    }
  }

  private def printHelp(): Unit = {
    System.err.println(
      s"""
         |Usage: EnrichmentApplication <zookeeper_url> <brokers> <input_topic> <output_topic>
         |  <zookeeper_url> is a comma-separated list of ZooKeeper URLs in the form <node>:<port>
         |  <brokers> is a list of one or more Kafka brokers
         |  <input_topic> is the kafka input topic to listen to
         |  <output_topic> is the kafka output topic to wrtie to
        """.stripMargin)
    System.exit(1)
  }

  private def getNumThreads(numThreads: Any): Int = numThreads match {
    case num: Number => num.intValue()
    case str: String =>
      val numThreadsStr = str.trim.toUpperCase
      if (numThreadsStr.endsWith("C")) {
        val factor = Integer.parseInt(numThreadsStr.replace("C", ""))
        factor * Runtime.getRuntime.availableProcessors
      } else {
        Integer.parseInt(numThreadsStr)
      }
    case _ => 2 * Runtime.getRuntime.availableProcessors
  }

}
