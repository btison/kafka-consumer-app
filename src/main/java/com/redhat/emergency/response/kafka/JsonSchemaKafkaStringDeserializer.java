package com.redhat.emergency.response.kafka;

import java.io.ByteArrayInputStream;

import com.fasterxml.jackson.core.JsonParser;
import com.worldturner.medeia.api.StreamInputSource;
import com.worldturner.medeia.schema.validation.SchemaValidator;
import io.apicurio.registry.utils.serde.JsonSchemaKafkaDeserializer;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonSchemaKafkaStringDeserializer extends JsonSchemaKafkaDeserializer<String> {

    private static final Logger log = LoggerFactory.getLogger(JsonSchemaKafkaStringDeserializer.class);

    @Override
    public String deserialize(String topic, byte[] data) {
        //ignore the message if there are no headers
        log.warn("Incoming message has no schema id header");
        return null;
    }

    @Override
    public String deserialize(String topic, Headers headers, byte[] data) {

        if (data == null) {
            return null;
        }

        try {
            if (isValidationEnabled()) {
                Long globalId = getGlobalId(headers);

                SchemaValidator schema = getSchemaCache().getSchema(globalId);
                JsonParser parser = api.createJsonParser(schema, new StreamInputSource(new ByteArrayInputStream(data)));
                api.parseAll(parser);
                log.info("Message validated. Schema id: " + globalId);
            }
            return new String(data);
        } catch (RuntimeException e) {
            log.error("Exception while validating incoming message", e);
            //ignore the message in case of validation exceptions
            return null;
        }
    }
}
