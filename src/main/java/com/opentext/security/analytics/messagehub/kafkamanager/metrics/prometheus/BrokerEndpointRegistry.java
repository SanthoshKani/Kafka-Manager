package com.opentext.security.analytics.messagehub.kafkamanager.metrics.prometheus;

import java.net.URI;
import java.util.Map;
import java.util.UUID;

/**
 * Registry providing configured scrape endpoints per cluster and broker.
 */
public interface BrokerEndpointRegistry {

    /** Return map of brokerId -> endpoint URI for the cluster */
    Map<Integer, URI> endpointsForCluster(UUID clusterId);

    /** Remove all endpoints for the cluster (used when cluster deleted) */
    void removeCluster(UUID clusterId);

    /** Remove single broker endpoint */
    void removeBroker(UUID clusterId, int brokerId);
}
