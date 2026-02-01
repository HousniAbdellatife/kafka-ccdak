import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;

void main() {
    StreamsBuilder builder = new StreamsBuilder();
    builder.stream("input", Consumed.with(Serdes.String(), Serdes.String()))
            .toTable(Materialized.as("input-table-store")); //store name
}