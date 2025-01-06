package net.snowflake.ingest.streaming;

import java.util.Map;
import java.util.Objects;

public class ObservabilityClusteringKey implements Comparable<ObservabilityClusteringKey> {
    private final String namespace;
    private final String service;
    private final String cluster;

    public ObservabilityClusteringKey(String cluster, String namespace, String service) {
        this.cluster = cluster;
        this.namespace = namespace;
        this.service = service;
    }

    public static ObservabilityClusteringKey createFromRow(Map<String, Object> row) {
        Object column = row.get("RESOURCE_ATTRIBUTES");
        if (!(column instanceof Map)) {
            throw new IllegalArgumentException("Expected a RESOURCE_ATTRIBUTES column");
        }

        @SuppressWarnings("unchecked") Map<String, Object> resource_attributes = (Map<String, Object>) column;
        String namespace = (String)resource_attributes.get("service.namespace");
        String service = (String)resource_attributes.get("service.name");
        String cluster = (String)resource_attributes.get("snowflake.o11y.aggregator.cluster");

        if (namespace == null || service == null || cluster == null) {
            throw new IllegalArgumentException("Missing expected RESOURCE_ATTRIBUTES value(s)");
        }

        return new ObservabilityClusteringKey(cluster, namespace, service);
    }

  @Override
    public int compareTo(ObservabilityClusteringKey o) {
        int res = namespace.compareTo(o.namespace);
        if (res != 0) return res;

        res = service.compareTo(o.service);
        if (res != 0) return res;

        return cluster.compareTo(o.cluster);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ObservabilityClusteringKey that = (ObservabilityClusteringKey) o;
        return Objects.equals(cluster, that.cluster) && Objects.equals(namespace, that.namespace) && Objects.equals(service, that.service);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cluster, namespace, service);
    }
}
