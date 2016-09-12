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
import com.ericfukuda.flink.IdsProtos.Ids;

/**
 * Distributed Data Generator for AdImpression Events.  Designed to generate
 * large numbers of campaigns
 */
public class AdImpressionsGeneratorHighKeyCardinality {

	public static void main(String[] args) throws Exception {
		final BenchmarkConfig benchmarkConfig = BenchmarkConfig.fromArgs(args);
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		SourceFunction<Ids> source = new HighKeyCardinalityGeneratorSource(benchmarkConfig);
		DataStream<Ids> adImpressions = env.addSource(source);

		adImpressions.flatMap(new ThroughputLogger<Ids>(240, 1_000_000));

		//adImpressions.addSink(new FlinkKafkaProducer08<>(
		//		benchmarkConfig.kafkaTopic,
		//		new SimpleStringSchema(), 
		//		benchmarkConfig.getParameters().getProperties(),
		//		new FixedPartitioner<String>()));

		env.execute("Ad Impressions data generator " + benchmarkConfig.getParameters().toMap().toString());
	}
}
