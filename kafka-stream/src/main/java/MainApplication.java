import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import java.util.Properties;

public class MainApplication {
	public static void main(final String[] args) {
		Properties config = new Properties();
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "store-example");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> inputStream = builder.stream("input-topic");
		KTable<String, Long> wordCounts = inputStream
				.mapValues(value -> value.toLowerCase())
				.flatMapValues(value -> Arrays.asList(value.split(" ")))
				.groupBy((key, value) -> value)
				.count(Materialized.as("word-counts-store"));

		wordCounts.toStream().to("output-topic", Produced.with(Serdes.String(), Serdes.Long()));

		KafkaStreams streams = new KafkaStreams(builder.build(), config);
		streams.start();
	}
}