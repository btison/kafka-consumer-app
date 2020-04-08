package org.acme.quarkus.kafka;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.enterprise.context.ApplicationScoped;

import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class KafkaRecordConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaRecordConsumer.class);

    @Incoming("channel")
    public CompletionStage<Void> process(IncomingKafkaRecord<String, String> message) {
        return CompletableFuture.runAsync(() -> {
            log.info("Consumed message from topic '" + message.getTopic() + "', partition '" 
                + message.getPartition() + "', offset '" + message.getOffset() + "'");
            log.info("    Message key: " + message.getKey());
            log.info("    Message value: " + message.getPayload());
        });
    }

}
