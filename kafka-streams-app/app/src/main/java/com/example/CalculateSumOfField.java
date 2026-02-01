import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;


static String INPUT_TOPIC = "input";
static String OUTPUT_TOPIC = "output";

void main() {
    StreamsBuilder builder = new StreamsBuilder();

    builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .mapValues(v -> Long.valueOf(v))
            .groupByKey()
            .reduce(Long::sum)
            .toStream()
            .mapValues(v -> "%s total of sales".formatted(v))
            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }