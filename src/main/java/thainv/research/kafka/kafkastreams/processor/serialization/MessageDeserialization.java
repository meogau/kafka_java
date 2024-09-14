package thainv.research.kafka.kafkastreams.processor.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class MessageDeserialization<T> implements Deserializer<T> {
    private final Class<T> clazz;
    public MessageDeserialization(Class<T> clazz) {
        this.clazz = clazz;
    }
    ObjectMapper mapper = new ObjectMapper();
    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            return mapper.readValue(data, clazz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
