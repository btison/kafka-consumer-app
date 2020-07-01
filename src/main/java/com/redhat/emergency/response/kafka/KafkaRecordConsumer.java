package com.redhat.emergency.response.kafka;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.enterprise.context.ApplicationScoped;

import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class KafkaRecordConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaRecordConsumer.class);

    @Incoming("channel")
    public CompletionStage<Void> process(IncomingKafkaRecord<String, String> message) {
        return CompletableFuture.runAsync(() -> {
            Optional<IncomingKafkaRecordMetadata> metadata = message.getMetadata(IncomingKafkaRecordMetadata.class);
            StringBuilder sb = new StringBuilder();
            metadata.ifPresent(m -> {
                Headers headers = m.getHeaders();
                Arrays.stream(headers.toArray()).forEach(header -> {
                    sb.append(header.key()).append(": ").append(new String(header.value())).append(", ");
                });
            });
            String hs = sb.toString();
            if (hs.length() > 2) {
                hs = hs.substring(0, hs.length()-2);
            }
            log.info("Consumed message from topic '" + message.getTopic() + "', partition '" 
                + message.getPartition() + "', offset '" + message.getOffset() + "'");
            log.info("    Headers: " + hs);
            log.info("    Message key: " + message.getKey());
            log.info("    Message value: " + message.getPayload());
        });
    }

}
