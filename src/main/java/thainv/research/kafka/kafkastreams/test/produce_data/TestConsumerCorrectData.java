package thainv.research.kafka.kafkastreams.test.produce_data;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import thainv.research.kafka.kafkastreams.message.KSMessage;

import java.util.HashMap;
import java.util.Map;

@Component
@RequiredArgsConstructor
public class TestConsumerCorrectData {
    private final Map<String, Long> map = new HashMap<>();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "ks_sink_topic", groupId = "ks_correct_group")
    public void listen(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        try {
            KSMessage message = objectMapper.convertValue(record.value(), KSMessage.class);
            if (map.containsKey(message.getConsoleKey()))
                map.put(message.getConsoleKey(), map.get(message.getConsoleKey()) + message.getValue());
            else map.put(message.getConsoleKey(), message.getValue());
            printData();
        } finally {
            acknowledgment.acknowledge();
        }
    }

    void printData(){
        var total = 0L;
        for(var entry : map.entrySet()){
            System.out.println(entry.getKey() + ": " + entry.getValue());
            total += entry.getValue();
        }
        System.out.println("total: " + total);
    }
}
