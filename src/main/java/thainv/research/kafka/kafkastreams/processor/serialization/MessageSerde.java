package thainv.research.kafka.kafkastreams.processor.serialization;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

@RequiredArgsConstructor
public class MessageSerde<T> implements Serde<T> {
    private final MessageDeserialization<T> messageDeserialization;
    private final MessageSerialization<T> serializer;


    @Override
    public Serializer<T> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return messageDeserialization;
    }
}
