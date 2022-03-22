package com.som.model.input;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class UserActivityDeserializationSchema implements DeserializationSchema<UserActivity> {

    static ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @Override
    public UserActivity deserialize(byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes, UserActivity.class);
    }

    @Override
    public boolean isEndOfStream(UserActivity inputMessage) {
        return false;
    }

    @Override
    public TypeInformation<UserActivity> getProducedType() {
        return TypeInformation.of(UserActivity.class);
    }

}
