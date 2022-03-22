package com.som.model.output;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.SneakyThrows;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class EventDetailSerializationSchema implements SerializationSchema<EventDetail> {

    static ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @SneakyThrows
    @Override
    public byte[] serialize(final EventDetail eventDetail) {
        return objectMapper.writeValueAsBytes(eventDetail);
    }
}
