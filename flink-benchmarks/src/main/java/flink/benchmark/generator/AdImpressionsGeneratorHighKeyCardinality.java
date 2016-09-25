package flink.benchmark.generator;

import flink.benchmark.BenchmarkConfig;
import flink.benchmark.utils.ThroughputLogger;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;
import org.apache.flink.streaming.connectors.kafka.partitioner.FixedPartitioner;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple7;
//import com.ericfukuda.flink.IdsProtos.Ids;

/**
 * Distributed Data Generator for AdImpression Events.  Designed to generate
 * large numbers of campaigns
 */
public class AdImpressionsGeneratorHighKeyCardinality {

	public static void main(String[] args) throws Exception {
		final BenchmarkConfig benchmarkConfig = BenchmarkConfig.fromArgs(args);
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		SourceFunction<Tuple7<String, String, String, String, String, String, String>> source = new HighKeyCardinalityGeneratorSource(benchmarkConfig);
		//SourceFunction<String> source = new HighKeyCardinalityGeneratorSource(benchmarkConfig);
		DataStream<Tuple7<String, String, String, String, String, String, String>> adImpressions = env.addSource(source);
		//DataStream<String> adImpressions = env.addSource(source);

		adImpressions.flatMap(new ThroughputLogger<Tuple7<String, String, String, String, String, String, String>>(240, 1_000_000));
		//adImpressions.flatMap(new ThroughputLogger<String>(240, 1_000_000));

    //adImpressions.map(new MapFunction<Tuple7<String, String, String, String, String, String, String>, String>() {
    //  @Override
    //  public String map(Tuple7<String, String, String, String, String, String, String> event) throws Exception {
    //    return event.toString();
    //  }
    //})
		//adImpressions.addSink(new FlinkKafkaProducer08<>(
		//		benchmarkConfig.kafkaTopic,
		//		new SimpleStringSchema(), 
		//		benchmarkConfig.getParameters().getProperties(),
		//		new FixedPartitioner<String>()));

		env.execute("Ad Impressions data generator " + benchmarkConfig.getParameters().toMap().toString());
	}
}
