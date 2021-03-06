/**
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package flink.benchmark.state;

import flink.benchmark.BenchmarkConfig;
import flink.benchmark.generator.HighKeyCardinalityGeneratorSource;
import flink.benchmark.utils.ThroughputLogger;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
//import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.UUID;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketAddress;

/**
 * To Run:  flink run -c flink.benchmark.state.AdvertisingTopologyFlinkStateHighKeyCard target/flink-benchmarks-0.1.0.jar "../conf/benchmarkConf.yaml"
 * <p>
 * <p>
 * Implementation where all state is kept in Flink (not in redis).  Designed for large #'s of campaigns
 */
public class AdvertisingTopologyFlinkStateHighKeyCardFPGA {


  public static void main(final String[] args) throws Exception {

    BenchmarkConfig config = BenchmarkConfig.fromArgs(args);

    // queryable state registration
    //ZooKeeperConfiguration zooKeeperConfiguration = new ZooKeeperConfiguration(config.akkaZookeeperPath, config.akkaZookeeperQuorum);
    //RegistrationService registrationService = new ZooKeeperRegistrationService(zooKeeperConfiguration);
    RegistrationService registrationService = null;

    // flink environment
    StreamExecutionEnvironment env = setupFlinkEnvironment(config);
    final TypeInformation<Tuple3<String, Long, Long>> queryWindowResultType = TypeInfoParser.parse("Tuple3<String, Long, Long>");

    //DataStream<String> rawMessageStream = streamSource(config, env);
    DataStream<UUID> rawMessageStream = streamSource(config, env);

    // log performance
    rawMessageStream.flatMap(new ThroughputLogger<UUID>(240, 1_000_000));

    rawMessageStream
      .keyBy(identity())
      .transform("Query Window",
        queryWindowResultType,
        new QueryableWindowOperatorEvicting(config.windowSize, registrationService, true));
      //.print();

    env.execute("UDP Receiver");
  }

  /**
   * Do some Flink Configuration
   */
  private static StreamExecutionEnvironment setupFlinkEnvironment(BenchmarkConfig config) throws IOException {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setGlobalJobParameters(config.getParameters());
    env.getConfig().enableObjectReuse();

    // enable checkpointing for fault tolerance
    if (config.checkpointsEnabled) {
      env.enableCheckpointing(config.checkpointInterval);
      if (config.checkpointToUri) {
        env.setStateBackend(new FsStateBackend(config.checkpointUri));
      }
    }

    // use event time
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    return env;
  }

  /**
   * The identity selector
   */
  private static KeySelector<UUID, UUID> identity() {
    return new KeySelector<UUID, UUID>() {
      @Override
      public UUID getKey(UUID s) {
        return s;
      }
    };
  }

  /**
   * Choose data source, either Kafka or data generator
   */
  //private static DataStream<String> streamSource(BenchmarkConfig config, StreamExecutionEnvironment env) {
  private static DataStream<UUID> streamSource(BenchmarkConfig config, StreamExecutionEnvironment env) {
    //RichParallelSourceFunction<String> source;
    RichSourceFunction<UUID> source;
    String sourceName;
    //if (config.useLocalEventGenerator) {
      source = new RichSourceFunction<UUID>() {
        private boolean running;
        DatagramSocket recSocket;

        @Override
        public void open(Configuration config) throws Exception {
          recSocket = new DatagramSocket(5431);
        }

        @Override
        public void run(SourceContext<UUID> sourceContext) throws Exception {
          running = true;
          while (running) {
            byte[] buf = new byte[512];
            DatagramPacket packet = new DatagramPacket(buf, buf.length);
            recSocket.receive(packet);
            int len = packet.getLength();
            String eventStr = new String(buf, 0, len);
            sourceContext.collect(UUID.fromString(eventStr));
          }
          recSocket.close();
        }

        @Override
        public void cancel() {
          running = false;
        }
      };
      sourceName = "UDP_Receiver";
    //} else {
      //source = kafkaSource(config);
      //sourceName = "Kafka";
    //}

    return env.addSource(source, sourceName);
  }

  /**
   * Setup kafka source
   */
  //private static FlinkKafkaConsumer08<String> kafkaSource(BenchmarkConfig config) {
  //  return new FlinkKafkaConsumer08<>(
  //    config.kafkaTopic,
  //    new SimpleStringSchema(),
  //    config.getParameters().getProperties());
  //}

  // --------------------------------------------------------------------------
  //   user functions
  // --------------------------------------------------------------------------

  /**
   * Parse JSON
   */
  //public static class Deserializer extends
  //  RichFlatMapFunction<Ids, Tuple7<String, String, String, String, String, String, String>> {

  //  private transient JSONParser parser = null;

  //  @Override
  //  public void open(Configuration parameters) throws Exception {
  //    parser = new JSONParser();
  //  }

  //  @Override
  //  public void flatMap(Ids input, Collector<Tuple7<String, String, String, String, String, String, String>> out)
  //    throws Exception {
  //    //JSONObject obj = (JSONObject) parser.parse(input);

  //    Tuple7<String, String, String, String, String, String, String> tuple =
  //      new Tuple7<>(
  //        //obj.getAsString("user_id"),
  //        //obj.getAsString("page_id"),
  //        //obj.getAsString("campaign_id"),
  //        //obj.getAsString("ad_type"),
  //        //obj.getAsString("event_type"),
  //        //obj.getAsString("event_time"),
  //        //obj.getAsString("ip_address"));
  //        input.getUserId(),
  //        input.getPageId(),
  //        input.getCampaignId(),
  //        input.getAdType(),
  //        input.getEventType(),
  //        input.getEventTime(),
  //        input.getIpAddress());
  //    out.collect(tuple);
  //  }
  //}

  /**
   * Filter out everything except "view" events
   */
  //public static class EventFilter implements
  //  FilterFunction<Tuple7<String, String, String, String, String, String, String>> {
  //  @Override
  //  public boolean filter(Tuple7<String, String, String, String, String, String, String> tuple) {
  //    //System.out.println(tuple.f0 + " " + tuple.f1 + " " + tuple.f2 + " " + tuple.f3 + " " + tuple.f4 + " " + tuple.f5 + " " + tuple.f6);
  //    return tuple.f4.equals("view");
  //  }
  //}

  /**
   * Project to campaign id
   */
  //public static class Projector implements MapFunction<Tuple7<String, String, String, String, String, String, String>, UUID> {

  //  @Override
  //  public UUID map(Tuple7<String, String, String, String, String, String, String> tuple) {
  //    //System.out.println(tuple.f0 + " " + tuple.f1 + " " + tuple.f2 + " " + tuple.f3 + " " + tuple.f4 + " " + tuple.f5 + " " + tuple.f6);
  //    return UUID.fromString(tuple.f2);
  //  }
  //}

  /**
   * Generate timestamp and watermarks
   */
  //public static class AdTimestampExtractor extends AscendingTimestampExtractor<UUID> {
  //  @Override
  //  public long extractAscendingTimestamp(kUID element) {
  //    return Long.parseLong(element.f5);
  //  }
  //}
}
