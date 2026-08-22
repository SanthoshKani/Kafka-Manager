package com.opentext.security.analytics.messagehub.kafkamanager.metrics.prometheus;

import java.net.URI;
import java.util.Map;

/**
 * Resolve a broker metrics endpoint. Resolution uses either an explicit per-broker host:port map or
 * an endpoint template. No HTTP requests are performed here.
 */
public final class PrometheusEndpointResolver {

    public static final class ResolutionResult {
        public final boolean success;
        public final URI uri;
        public final Reason reason;

        public enum Reason {
            NONE,
            NO_ENDPOINT_CONFIGURED,
            INVALID_TEMPLATE
        }

        private ResolutionResult(boolean success, URI uri, Reason reason) {
            this.success = success;
            this.uri = uri;
            this.reason = reason;
        }

        public static ResolutionResult success(URI uri) {
            return new ResolutionResult(true, uri, Reason.NONE);
        }

        public static ResolutionResult noEndpoint() {
            return new ResolutionResult(false, null, Reason.NO_ENDPOINT_CONFIGURED);
        }

        public static ResolutionResult invalidTemplate() {
            return new ResolutionResult(false, null, Reason.INVALID_TEMPLATE);
        }
    }

    private final String endpointTemplate; // may contain {host},{port},{brokerId}
    private final Map<Integer, String> perBrokerHostPort; // brokerId -> host:port
    private final String scrapePath;

    public PrometheusEndpointResolver(
            String endpointTemplate, Map<Integer, String> perBrokerHostPort, String scrapePath) {
        this.endpointTemplate = endpointTemplate;
        this.perBrokerHostPort = perBrokerHostPort;
        this.scrapePath = scrapePath == null ? "/metrics" : scrapePath;
    }

    public ResolutionResult resolve(int brokerId) {
        // prefer per-broker host mapping
        if (perBrokerHostPort != null && perBrokerHostPort.containsKey(brokerId)) {
            String hostPort = perBrokerHostPort.get(brokerId);
            try {
                URI uri = new URI("http://" + hostPort + scrapePath);
                return ResolutionResult.success(uri);
            } catch (Exception e) {
                return ResolutionResult.invalidTemplate();
            }
        }
        if (endpointTemplate != null && endpointTemplate.contains("{brokerId}")) {
            String resolved = endpointTemplate
                    .replace("{brokerId}", Integer.toString(brokerId))
                    .replace("{path}", scrapePath);
            try {
                URI uri = URI.create(resolved);
                return ResolutionResult.success(uri);
            } catch (Exception e) {
                return ResolutionResult.invalidTemplate();
            }
        }
        return ResolutionResult.noEndpoint();
    }
}
