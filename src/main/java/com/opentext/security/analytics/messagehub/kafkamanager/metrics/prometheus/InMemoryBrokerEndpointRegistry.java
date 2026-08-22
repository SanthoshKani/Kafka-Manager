package com.opentext.security.analytics.messagehub.kafkamanager.metrics.prometheus;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Simple in-memory registry used by tests and simple configuration scenarios.
 */
public class InMemoryBrokerEndpointRegistry implements BrokerEndpointRegistry {

    private final Map<UUID, Map<Integer, URI>> map = new HashMap<>();

    public void putEndpoint(UUID clusterId, int brokerId, URI uri) {
        map.computeIfAbsent(clusterId, k -> new HashMap<>()).put(brokerId, uri);
    }

    @Override
    public Map<Integer, URI> endpointsForCluster(UUID clusterId) {
        return map.containsKey(clusterId) ? Collections.unmodifiableMap(map.get(clusterId)) : Collections.emptyMap();
    }

    @Override
    public void removeCluster(UUID clusterId) {
        map.remove(clusterId);
    }

    @Override
    public void removeBroker(UUID clusterId, int brokerId) {
        Map<Integer, URI> m = map.get(clusterId);
        if (m != null) m.remove(brokerId);
    }
}
