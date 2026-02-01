import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;


void main() {

    // 1- create stream builder
    StreamsBuilder builder = new StreamsBuilder();

    // 2- source topic
    KStream<String, String> stream = builder.stream("input-topic",
            Consumed.with(Serdes.String(), Serdes.String())
    );


    // 3- stateless operations
    var stateLessStream = stream.peek((k, v) -> IO.println(k + " : " + v))
            .filter((k, v) -> !k.equals("random-key"));


    // 4- create topology from builder
    Topology topology = builder.build();

    // 5 - start execution
    var props = new Properties();
    props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-app");


    try (var streams = new KafkaStreams(topology, props)) {

        streams.setUncaughtExceptionHandler((Throwable throwable) -> {
            IO.println("error: " + throwable.getMessage());
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        streams.start();
    }

}
