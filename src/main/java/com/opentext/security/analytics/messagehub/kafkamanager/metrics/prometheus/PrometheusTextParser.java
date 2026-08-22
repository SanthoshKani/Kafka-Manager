package com.opentext.security.analytics.messagehub.kafkamanager.metrics.prometheus;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Minimal Prometheus text exposition format parser constrained to an allowlist of metric names.
 * Independent from HTTP and enforces maximum input size.
 */
public final class PrometheusTextParser {

    public static final class ParsedMetric {
        public final String name;
        public final Map<String, String> labels;
        public final double value;
        public final Instant timestamp; // may be null

        public ParsedMetric(String name, Map<String, String> labels, double value, Instant timestamp) {
            this.name = Objects.requireNonNull(name);
            this.labels = Map.copyOf(labels);
            this.value = value;
            this.timestamp = timestamp;
        }
    }

    private final Set<String> allowlist;
    private final int maxInputBytes;

    public PrometheusTextParser(Set<String> allowlist, int maxInputBytes) {
        this.allowlist = Objects.requireNonNull(allowlist);
        if (maxInputBytes <= 0) throw new IllegalArgumentException("maxInputBytes must be > 0");
        this.maxInputBytes = maxInputBytes;
    }

    public List<ParsedMetric> parse(byte[] input) throws IOException {
        Objects.requireNonNull(input);
        if (input.length > maxInputBytes) {
            throw new IOException("input exceeds maximum allowed size");
        }
        String content = new String(input, StandardCharsets.UTF_8);
        List<ParsedMetric> out = new ArrayList<>();
        String[] lines = content.split("\n");
        for (String raw : lines) {
            String line = raw.trim();
            if (line.isEmpty()) continue;
            if (line.startsWith("#")) continue; // skip HELP, TYPE, comments
            // parse metric line: name{labels} value [timestamp]
            int brace = line.indexOf('{');
            int space = line.indexOf(' ');
            String name;
            String labelsPart = null;
            String rest;
            if (brace > 0) {
                name = line.substring(0, brace).trim();
                int endBrace = line.indexOf('}', brace);
                if (endBrace < 0) continue; // malformed -> ignore
                labelsPart = line.substring(brace + 1, endBrace);
                rest = line.substring(endBrace + 1).trim();
            } else {
                // no labels
                int idx = line.indexOf(' ');
                if (idx < 0) continue;
                name = line.substring(0, idx).trim();
                rest = line.substring(idx).trim();
            }
            if (!allowlist.contains(name)) continue;
            Map<String, String> labels = labelsPart == null ? Map.of() : parseLabels(labelsPart);
            // rest contains value and optional timestamp
            String[] parts = rest.split("\\s+");
            if (parts.length == 0) continue;
            String valStr = parts[0];
            double val;
            try {
                val = Double.parseDouble(valStr);
            } catch (NumberFormatException e) {
                continue; // reject invalid numeric values
            }
            if (!Double.isFinite(val)) continue;
            Instant ts = null;
            if (parts.length > 1) {
                try {
                    long millis = Long.parseLong(parts[1]);
                    // Prometheus timestamps are in milliseconds or seconds? JMX exporter uses milliseconds; try millis
                    ts = Instant.ofEpochMilli(millis);
                } catch (NumberFormatException e) {
                    // ignore timestamp parse and leave null
                }
            }
            out.add(new ParsedMetric(name, labels, val, ts));
        }
        return out;
    }

    private Map<String, String> parseLabels(String s) {
        Map<String, String> labels = new HashMap<>();
        int len = s.length();
        int i = 0;
        while (i < len) {
            // parse key
            int eq = s.indexOf('=', i);
            if (eq < 0) break;
            String key = s.substring(i, eq).trim();
            i = eq + 1;
            // value must start with '"'
            if (i >= len || s.charAt(i) != '"') break;
            i++;
            StringBuilder sb = new StringBuilder();
            boolean esc = false;
            while (i < len) {
                char c = s.charAt(i++);
                if (esc) {
                    // support basic escapes \ and \" and \n and \t
                    switch (c) {
                        case 'n':
                            sb.append('\n');
                            break;
                        case 't':
                            sb.append('\t');
                            break;
                        case 'r':
                            sb.append('\r');
                            break;
                        default:
                            sb.append(c);
                            break;
                    }
                    esc = false;
                } else if (c == '\\') {
                    esc = true;
                } else if (c == '"') {
                    break;
                } else {
                    sb.append(c);
                }
            }
            labels.put(normalizeLabelKey(key), sb.toString());
            // skip optional comma
            while (i < len && s.charAt(i) != ',') i++;
            if (i < len && s.charAt(i) == ',') i++;
        }
        return labels;
    }

    private String normalizeLabelKey(String key) {
        if (key == null) return null;
        return key.trim().toLowerCase();
    }
}
