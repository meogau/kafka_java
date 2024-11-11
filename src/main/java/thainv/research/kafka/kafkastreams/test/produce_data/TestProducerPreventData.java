package thainv.research.kafka.kafkastreams.test.produce_data;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import thainv.research.kafka.kafkastreams.message.KSMessage;

import java.util.Random;

@Component
@RequiredArgsConstructor
public class TestProducerPreventData {
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Scheduled(fixedDelayString = "30000")
    public void initData() throws InterruptedException {
        String sourceTopic = "source_test";
        var random = new Random();
        var value = 10000L;
        while (value>0){
            long sendValue = value<10? value:random.nextLong(10);
            value -= sendValue;
            var key = "ks_key"+sendValue;
            var message = new KSMessage(key,sendValue);
            kafkaTemplate.send(sourceTopic,key,message);
        }
    }
}
