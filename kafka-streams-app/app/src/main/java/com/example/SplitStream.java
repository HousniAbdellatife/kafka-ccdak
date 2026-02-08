import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;

import java.util.Properties;


static String INPUT_TOPIC = "INPUT";

void main() {
    StreamsBuilder builder = new StreamsBuilder();


    builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .split()
            .branch((k, v) -> k.startsWith("A"),
                    Branched.withConsumer(ks -> ks.to("A")))
            .branch((k, v) -> k.startsWith("B"),
                    Branched.withConsumer(ks -> ks.to("B")))
            .defaultBranch(Branched.withConsumer(ks -> ks.to("others")));


    Topology topology = builder.build();
    Properties props = new Properties();

    try (KafkaStreams kafkaStreams = new KafkaStreams(topology, props)) {
        kafkaStreams.start();
    }

}
