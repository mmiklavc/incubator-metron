package org.apache.metron.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Joiner;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.metron.common.utils.JSONUtils;
import org.apache.metron.common.utils.KafkaUtils;

import javax.annotation.Nullable;
import java.io.File;
import java.util.*;
import java.util.function.Function;

public class KafkaCopy {
  private static abstract class OptionHandler implements Function<String, Option> {}
  private enum CopyOptions {
    HELP("h", new OptionHandler() {

      @Nullable
      @Override
      public Option apply(@Nullable String s) {
        return new Option(s, "help", false, "Generate Help screen");
      }
    })
    ,CONSUMER_GROUP("cg", new OptionHandler() {

      @Nullable
      @Override
      public Option apply(@Nullable String s) {
        Option o = new Option(s, "group", true, "consumer group");
        o.setArgName("quorum");
        o.setRequired(true);
        return o;
      }
    })
    ,ZOOKEEPER("z", new OptionHandler() {

      @Nullable
      @Override
      public Option apply(@Nullable String s) {
        Option o = new Option(s, "zkquorum", true, "zookeeper quorum");
        o.setArgName("quorum");
        o.setRequired(true);
        return o;
      }
    })
    ,INPUT("i", new OptionHandler() {

      @Nullable
      @Override
      public Option apply(@Nullable String s) {
        Option o = new Option(s, "input", true, "Input topic");
        o.setArgName("TOPIC");
        o.setRequired(true);
        return o;
      }
    })
    ,OUTPUT("o", new OptionHandler() {

      @Nullable
      @Override
      public Option apply(@Nullable String s) {
        Option o = new Option(s, "output", true, "output topic");
        o.setArgName("TOPIC");
        o.setRequired(true);
        return o;
      }
    })
    ,CONSUMER_CONFIGS("cc", new OptionHandler() {

      @Nullable
      @Override
      public Option apply(@Nullable String s) {
        Option o = new Option(s, "consumer_configs", true, "consumer configs in a json map");
        o.setArgName("CONFIG_FILE");
        o.setRequired(false);
        return o;
      }
    })
    ,PRODUCER_CONFIGS("pc", new OptionHandler() {

      @Nullable
      @Override
      public Option apply(@Nullable String s) {
        Option o = new Option(s, "producer_configs", true, "producer configs in a json map");
        o.setArgName("CONFIG_FILE");
        o.setRequired(false);
        return o;
      }
    })
    ;
    Option option;
    String shortCode;

    CopyOptions(String shortCode, OptionHandler optionHandler) {
      this.shortCode = shortCode;
      this.option = optionHandler.apply(shortCode);
    }

    public boolean has(CommandLine cli) {
      return cli.hasOption(shortCode);
    }

    public String get(CommandLine cli) {
      return cli.getOptionValue(shortCode);
    }

    public static CommandLine parse(CommandLineParser parser, String[] args) {
      try {
        CommandLine cli = parser.parse(getOptions(), args);
        if (CopyOptions.HELP.has(cli)) {
          printHelp();
          System.exit(0);
        }
        return cli;
      } catch (ParseException e) {
        System.err.println("Unable to parse args: " + Joiner.on(' ').join(args));
        e.printStackTrace(System.err);
        printHelp();
        System.exit(-1);
        return null;
      }
    }

    public static void printHelp() {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("KafkaCopy", getOptions());
    }

    public static Options getOptions() {
      Options ret = new Options();
      for (CopyOptions o : CopyOptions.values()) {
        ret.addOption(o.option);
      }
      return ret;
    }
  }

  public static class Progress {
    private int count = 0;
    private String anim= "|/-\\";
    private long startTime = System.currentTimeMillis();
    double eps = 0;

    public synchronized void update() {
      int currentCount = count++;
      int secondsElapsed = (int) ((System.currentTimeMillis() - startTime)/1000);
      if(currentCount % 1000 == 0) {
        eps = (1.0 * currentCount) / secondsElapsed;
      }
      System.out.print("\rProcessed " + currentCount + " ( " + String.format("%.2f", eps) + " e/s ) - " + anim.charAt(currentCount % anim.length()));
    }
  }

  public static void main(String... argv) throws Exception {
    CommandLine cli = CopyOptions.parse(new PosixParser(), argv);
    Map<String, Object> producerConfig = new HashMap<>();
    List<String> brokers = KafkaUtils.INSTANCE.getBrokersFromZookeeper(CopyOptions.ZOOKEEPER.get(cli));
    producerConfig.put("bootstrap.servers", Joiner.on(",").join(brokers));
    producerConfig.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerConfig.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerConfig.put("request.required.acks", "-1");
    producerConfig.put("fetch.message.max.bytes", "" + 1024 * 1024 * 10);
    producerConfig.put("replica.fetch.max.bytes", "" + 1024 * 1024 * 10);
    producerConfig.put("message.max.bytes", "" + 1024 * 1024 * 10);
    producerConfig.put("message.send.max.retries", "10");
    if (CopyOptions.PRODUCER_CONFIGS.has(cli)) {
      Map<String, Object> additionalConfigs = JSONUtils.INSTANCE.load(new File(CopyOptions.PRODUCER_CONFIGS.get(cli)), new TypeReference<Map<String, Object>>() {
      });
      producerConfig.putAll(additionalConfigs);
    }
    Map<String, Object> consumerConfig = new HashMap<>();
    consumerConfig.put("bootstrap.servers", Joiner.on(",").join(brokers));
    consumerConfig.put("group.id", CopyOptions.CONSUMER_GROUP.get(cli));
    consumerConfig.put("enable.auto.commit", "true");
    consumerConfig.put("auto.commit.interval.ms", "1000");
    consumerConfig.put("session.timeout.ms", "30000");
    consumerConfig.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    consumerConfig.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

    if (CopyOptions.CONSUMER_CONFIGS.has(cli)) {
      Map<String, Object> additionalConfigs = JSONUtils.INSTANCE.load(new File(CopyOptions.CONSUMER_CONFIGS.get(cli)), new TypeReference<Map<String, Object>>() {
      });
      consumerConfig.putAll(additionalConfigs);
    }

    try(Producer<byte[], byte[]> producer = new KafkaProducer<>(producerConfig)) {
      try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerConfig)) {
        Progress progress = new Progress();
        consumer.subscribe(Arrays.asList(CopyOptions.INPUT.get(cli)));
        for (ConsumerRecord<byte[], byte[]> record : consumer.poll(5000)) {
          producer.send(new ProducerRecord<>(CopyOptions.OUTPUT.get(cli), record.key(), record.value()));
          progress.update();
        }
      }
    }
  }
}
