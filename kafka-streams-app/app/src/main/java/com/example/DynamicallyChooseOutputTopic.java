import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

void main() {
    StreamsBuilder builder = new StreamsBuilder();

    builder.stream("input", Consumed.with(Serdes.String(), Serdes.String()))
            .mapValues(v -> v.toUpperCase())
            .to(
                    (key, v, recordContext) -> {
                        if (key.startsWith("A")) return "A";
                        else if (key.startsWith("B")) return "B";
                        else return "C";
                    },
                    Produced.with(Serdes.String(), Serdes.String())
            );
}
