package com.opentext.security.analytics.messagehub.kafkamanager.kafkaadmin;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public final class KafkaEndpointSupport {

    private KafkaEndpointSupport() {}

    public static String normalizeEndpointList(String endpoints) {
        if (endpoints == null) {
            return null;
        }

        String trimmed = endpoints.trim();
        if (trimmed.isEmpty()) {
            return "";
        }

        List<String> normalized = new ArrayList<>();
        for (String entry : trimmed.split(",", -1)) {
            normalized.add(normalizeEndpoint(entry));
        }
        return String.join(",", normalized);
    }

    public static String normalizeSecurityProtocol(String securityProtocol) {
        if (securityProtocol == null) {
            return null;
        }
        return securityProtocol.trim().toUpperCase(Locale.ROOT);
    }

    public static void validateEndpointList(String endpoints) {
        if (endpoints == null || endpoints.isBlank()) {
            return;
        }
        normalizeEndpointList(endpoints);
    }

    private static String normalizeEndpoint(String endpoint) {
        String trimmed = endpoint == null ? "" : endpoint.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("endpoint list contains an empty entry");
        }

        int schemeSeparator = trimmed.indexOf("://");
        if (schemeSeparator < 0) {
            return trimmed;
        }

        String listener = trimmed.substring(0, schemeSeparator).trim();
        String address = trimmed.substring(schemeSeparator + 3).trim();
        if (listener.isEmpty()) {
            throw new IllegalArgumentException("listener name is missing before '://'");
        }
        if (address.isEmpty()) {
            throw new IllegalArgumentException("listener '" + listener + "' is missing a host:port address");
        }
        return address;
    }
}
