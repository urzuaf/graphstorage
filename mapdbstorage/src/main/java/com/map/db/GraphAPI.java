package com.map.db;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Consumer;

public class GraphAPI {
    private final GraphStore store;

    public GraphAPI(GraphStore store) { this.store = store; }

    // Ingesta
    public void ingestNodes(Path nodes) throws IOException { store.ingestNodes(nodes); }
    public void ingestEdges(Path edges) throws IOException { store.ingestEdges(edges); }

    // Consultas
    public void forEachEdgeIdByLabel(String label, Consumer<String> edgeIdConsumer) {
        store.forEachEdgeIdByLabel(label, edgeIdConsumer);
    }
    public void forEachSourceNodeByLabel(String label, Consumer<String> nodeIdConsumer) {
        store.forEachSourceNodeByLabel(label, nodeIdConsumer);
    }
    public void forEachDestinationNodeByLabel(String label, Consumer<String> nodeIdConsumer) {
        store.forEachDestinationNodeByLabel(label, nodeIdConsumer);
    }
    public GraphStore.NodeBlob getNode(String nodeId) { return store.getNode(nodeId); }
    public GraphStore.EdgeBlob getEdge(String edgeId) { return store.getEdge(edgeId); }
    public void forEachNodeByPropertyEquals(String propName, String propValue, Consumer<String> nodeIdConsumer) {
        store.forEachNodeByPropertyEquals(propName, propValue, nodeIdConsumer);
    }
}
