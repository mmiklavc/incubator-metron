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
import java.util.{Collections, Properties}

import org.apache.curator.framework.recipes.cache.TreeCacheEvent
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.metron.common.Constants
import org.apache.metron.common.configuration.writer.{ConfigurationStrategy, ConfigurationsStrategies}
import org.apache.metron.common.configuration.{ConfigurationType, ConfigurationsUtils, ParserConfigurations, SensorParserConfig}
import org.apache.metron.common.message.metadata.RawMessage
import org.apache.metron.common.utils.JSONUtils
import org.apache.metron.common.zookeeper.configurations.{ConfigurationsUpdater, Reloadable}
import org.apache.metron.integration.components.{KafkaComponent, ZKServerComponent}
import org.apache.metron.integration.{BaseIntegrationTest, ComponentRunner}
import org.apache.metron.stellar.dsl.Context.{Capabilities, Capability}
import org.apache.metron.stellar.dsl.{Context, StellarFunctions}
import org.apache.metron.zookeeper.SimpleEventListener.Callback
import org.apache.metron.zookeeper.{SimpleEventListener, ZKCache}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._

import scala.collection.JavaConversions._
import scala.collection.{JavaConversions, mutable}


object ParserApplication extends BaseIntegrationTest with Reloadable {
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

    val runner: ComponentRunner = new ComponentRunner.Builder()
      .withComponent(zkComponentName, zkServerComponent)
      .withComponent(kafkaComponentName, kafkaComponent)
      //        .withComponent("config", configUploadComponent)
      .withMillisecondsBetweenAttempts(5000)
      //        .withCustomShutdownOrder(Array[String]("config", kafkaComponentName, zkComponent)).withNumRetries(10).build
      .withCustomShutdownOrder(Array[String](kafkaComponentName, zkComponentName)).withNumRetries(10).build
    runner.start()

    // Load messages
    kafkaComponent.writeMessages(sensorTopic, "1_c1, 1_c2, 1_c3")
    kafkaComponent.writeMessages(sensorTopic, "2_c1, 2_c2, 2_c3")
    kafkaComponent.writeMessages(sensorTopic, "brokenMessage")

    // Set up ZK configs
    println("Setting up ZK and configs from driver")
    val connectionStr = zkServerComponent.getConnectionString
    val brokerList = kafkaComponent.getBrokerList // TODO make sure this is properly formatted
    val parserConfig: SensorParserConfig = JSONUtils.INSTANCE.load(parserConfigJSON, classOf[SensorParserConfig])
    ConfigurationsUtils.writeSensorParserConfigToZookeeper(sensorType, parserConfig, connectionStr)
    ConfigurationsUtils.writeGlobalConfigToZookeeper(new util.HashMap[String, AnyRef](), connectionStr)


    val args = Array(connectionStr, brokerList, "csv_parser", sensorTopic)
    val Array(zookeeperUrl, brokers, groupId, topics) = args



    // Create context with 1 second batch interval
    // TODO Need to set this based on sensor
    val sparkConf = new SparkConf().setAppName("ParserTest").setMaster("local")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    println("topicSet = " + topicsSet)
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
        //  FunctionResolver functionResolver;
        //  Map<String, Object> variables;
        //  Context context = null;
        //  StellarStatefulExecutor executor;
        //
        //  @Before
        //  public void setup() {
        //    variables = new HashMap<>();
        //    functionResolver = new SimpleFunctionResolver()
        //            .withClass(ParserFunctions.ParseFunction.class)
        //            .withClass(ParserFunctions.InitializeFunction.class)
        //            .withClass(ParserFunctions.ConfigFunction.class);
        //    context = new Context.Builder().build();
        //    executor = new DefaultStellarStatefulExecutor(functionResolver, context);

        val retryPolicy = new ExponentialBackoffRetry(1000, 3)
        val client = CuratorFrameworkFactory.newClient(zookeeperUrl, retryPolicy)
        client.start()
        val cache = prepCache(client, zookeeperUrl)

        // Get Global Configs
        val globalConfigs = ConfigurationsUtils.readGlobalConfigFromZookeeper(client)

        // Get Parser Configs
        val configs = new ParserConfigurations()
        val parserConfigs = mutable.HashMap[String, SensorParserConfig]()
        ConfigurationsUtils.updateParserConfigsFromZookeeper(configs, client)
        List(sensorType).foreach { sensorType =>
          val parserConfig = configs.getSensorParserConfig(sensorType)
          if (parserConfig == null) throw new IllegalStateException("Cannot find the parser configuration in zookeeper for " + sensorType + "." +
            "  Please check that it exists in zookeeper by using the 'zk_load_configs.sh -m DUMP' command.")
          parserConfigs.put(sensorType, parserConfig);
        }

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

        var parserRunner = new ParserRunnerImpl(JavaConversions.setAsJavaSet(Set(sensorType)))
        // Do similar to above with explicit new Supplier
        var parserSupplier = new Supplier[ParserConfigurations] {
          override def get(): ParserConfigurations = configs
        }
        parserRunner.init(parserSupplier, stellarContext)

        val producer = new KafkaProducer[String, String](kafkaParams)

        // Actually do the work on a per message basis
        partition.foreach {
          record =>
            // TODO don't just ignore metadata
            var message = new RawMessage(record.value().getBytes(StandardCharsets.UTF_8), Collections.emptyMap())
            var result = parserRunner.execute(sensorType, message, configs)
            println("Messages: " + result.getMessages)
            println("Errors: : " + result.getErrors)
            result.getMessages.foreach {
              resultMessage =>
                val outputMessage = new ProducerRecord[String, String](outputTopic, null, resultMessage.toJSONString)
                producer.send(outputMessage)
            }
        }
        client.close()
        cache.close()
      }
    }

    ssc.start()
    ssc.awaitTerminationOrTimeout(6000)
    ssc.stop()

    println("messages in Kafka:")
    kafkaComponent.readMessages(outputTopic).toList.foreach { raw =>
      val str = new String(raw)
      println(str)
    }

    System.exit(0)
  }

  protected def prepCache(client: CuratorFramework, zookeeperUrl: String): ZKCache = {
    try {
      //this is temporary to ensure that any validation passes.
      //The individual bolt will reinitialize stellar to dynamically pull from
      //zookeeper.
      ConfigurationsUtils.setupStellarStatically(client)
      val updater = createUpdater
      val listener = new SimpleEventListener.Builder().`with`(
        new Callback {
          override def apply(client: CuratorFramework, path: String, data: Array[Byte]): Unit = updater.update(client, path, data)
        },
        TreeCacheEvent.Type.NODE_ADDED, TreeCacheEvent.Type.NODE_UPDATED).`with`(
        new Callback {
          override def apply(client: CuratorFramework, path: String, data: Array[Byte]): Unit = updater.delete(client, path, data)
        },
        TreeCacheEvent.Type.NODE_REMOVED).build
      val cache = new ZKCache.Builder().withClient(client).withListener(listener).withRoot(Constants.ZOOKEEPER_TOPOLOGY_ROOT).build
      updater.forceUpdate(client)
      cache.start()
      cache
    } catch {
      case e: Exception =>
        throw new RuntimeException(e)
    }
  }

  protected def getConfigurationStrategy: ConfigurationStrategy[ParserConfigurations] = {
    ConfigurationsStrategies.PARSERS.asInstanceOf[ConfigurationStrategy[ParserConfigurations]]
  }

  protected def createUpdater: ConfigurationsUpdater[ParserConfigurations] = {
    getConfigurationStrategy.createUpdater(this, new Supplier[ParserConfigurations]() {
      override def get(): ParserConfigurations = getConfigurations
    })
  }

  def getConfigurations: ParserConfigurations = parserConfigurations

  override def reloadCallback(name: String, `type`: ConfigurationType): Unit = {}
}

