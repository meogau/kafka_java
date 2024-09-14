package thainv.research.kafka.kafkastreams.processor;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;
import thainv.research.kafka.kafkastreams.message.KSMessage;
import thainv.research.kafka.kafkastreams.processor.serialization.MessageDeserialization;
import thainv.research.kafka.kafkastreams.processor.serialization.MessageSerde;
import thainv.research.kafka.kafkastreams.processor.serialization.MessageSerialization;

import java.time.Duration;

@Component
@EnableKafkaStreams
public class KafkaStreamProcessor {

    public KafkaStreamProcessor(StreamsBuilder streamsBuilder) {
        // source
        String sourceTopic = "ks_source_topic";
        KStream<String, KSMessage> stream = streamsBuilder.stream(sourceTopic,
                Consumed.with(Serdes.String(),
                        new MessageSerde<>(new MessageDeserialization<>(KSMessage.class), new MessageSerialization<>())));

        // Group the stream by key
        KGroupedStream<String, KSMessage> groupedStream = stream
                .groupBy((_, value) -> value.getConsoleKey(), Grouped.with(Serdes.String(), new JsonSerde<>(KSMessage.class)));

        // Define a custom time window of 30 seconds
        TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(30));

        // Aggregate sum
        KTable<Windowed<String>, Long> aggregatedTable = groupedStream
                .windowedBy(timeWindows)
                .aggregate(
                        () -> 0L,
                        (_, value, aggValue) -> aggValue + value.getValue(),
                        Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("aggregate-store")
                                .withValueSerde(Serdes.Long())
                );
        // sink
        String sinkTopic = "ks_sink_topic";
        aggregatedTable
                .toStream()
                .map((windowedKey, sumValue) -> new KeyValue<>(windowedKey.key(), new KSMessage(windowedKey.key(), sumValue)))
                .to(sinkTopic, Produced.with(Serdes.String(), new JsonSerde<>(KSMessage.class)));
    }
}
