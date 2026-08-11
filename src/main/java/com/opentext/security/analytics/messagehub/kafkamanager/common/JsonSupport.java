package com.opentext.security.analytics.messagehub.kafkamanager.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;

public final class JsonSupport {

    private JsonSupport() {}

    public static String toJson(ObjectMapper objectMapper, Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Failed to serialize JSON", exception);
        }
    }

    public static List<String> toStringList(ObjectMapper objectMapper, String json) {
        try {
            if (json == null || json.isBlank()) {
                return List.of();
            }
            return objectMapper.readValue(json, new TypeReference<List<String>>() {});
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Failed to parse JSON list", exception);
        }
    }

    public static Map<String, String> toStringMap(ObjectMapper objectMapper, String json) {
        try {
            if (json == null || json.isBlank()) {
                return Map.of();
            }
            return objectMapper.readValue(json, new TypeReference<Map<String, String>>() {});
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Failed to parse JSON map", exception);
        }
    }
}
