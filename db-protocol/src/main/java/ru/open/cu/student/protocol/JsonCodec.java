package ru.open.cu.student.protocol;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public final class JsonCodec {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private JsonCodec() {
    }

    public static byte[] toJsonBytes(Object o) {
        try {
            return MAPPER.writeValueAsBytes(o);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize JSON", e);
        }
    }

    public static <T> T fromJsonBytes(byte[] bytes, Class<T> type) {
        try {
            return MAPPER.readValue(bytes, type);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse JSON", e);
        }
    }
}


