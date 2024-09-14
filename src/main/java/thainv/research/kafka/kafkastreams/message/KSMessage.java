package thainv.research.kafka.kafkastreams.message;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@AllArgsConstructor
@NoArgsConstructor
public class KSMessage {
    private String consoleKey;
    private Long value;
}
