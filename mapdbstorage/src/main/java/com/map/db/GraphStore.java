package com.map.db;

import org.mapdb.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Consumer;


public class GraphStore implements AutoCloseable {

    private static final String CF_NODES = "cf_nodes";
    private static final String CF_EDGES = "cf_edges";
    private static final String CF_INDEX = "cf_index";

    private static final char SEP = '\0';

    private final DB db;
    private final BTreeMap<String, NodeBlob> nodes;
    private final BTreeMap<String, EdgeBlob> edges;
    private final NavigableSet<String> index; // TreeSet ordenado para prefix scan

    private GraphStore(DB db,
                       BTreeMap<String, NodeBlob> nodes,
                       BTreeMap<String, EdgeBlob> edges,
                       NavigableSet<String> index) {
        this.db = db;
        this.nodes = nodes;
        this.edges = edges;
        this.index = index;
    }

    public static GraphStore open(Path file) {
        file.toFile().getParentFile().mkdirs();
        DB db = DBMaker.fileDB(file.toFile())
                .fileMmapEnableIfSupported()
                .transactionEnable()            // transaccional; commit() explícito
                .concurrencyScale(16)
                .closeOnJvmShutdown()
                .make();

        BTreeMap<String, NodeBlob> nodes = db.treeMap(CF_NODES, Serializer.STRING, Serializer.JAVA).createOrOpen();
        BTreeMap<String, EdgeBlob> edges = db.treeMap(CF_EDGES, Serializer.STRING, Serializer.JAVA).createOrOpen();
        NavigableSet<String> index = db.treeSet(CF_INDEX, Serializer.STRING).createOrOpen();

        return new GraphStore(db, nodes, edges, index);
    }

    @Override
    public void close() {
        try { db.commit(); } catch (Exception ignore) {}
        try { db.close(); } catch (Exception ignore) {}
    }


    public static final class NodeBlob implements Serializable {
        @Serial private static final long serialVersionUID = 1L;
        public final String label;
        public final Map<String, String> props;
        public NodeBlob(String label, Map<String, String> props) {
            this.label = label;
            this.props = new LinkedHashMap<>(props);
        }
        @Override public String toString() { return "NodeBlob{label="+label+", props="+props+"}"; }
    }

    public static final class EdgeBlob implements Serializable {
        @Serial private static final long serialVersionUID = 1L;
        public final String label;
        public final String src;
        public final String dst;
        public EdgeBlob(String label, String src, String dst) {
            this.label = label; this.src = src; this.dst = dst;
        }
        @Override public String toString() { return "EdgeBlob{label="+label+", src="+src+", dst="+dst+"}"; }
    }


    private static String keyNode(String nodeId) { return "node:" + nodeId; }
    private static String keyEdge(String edgeId) { return "edge:" + edgeId; }

    private static String idxKey(String... parts) {
        StringBuilder sb = new StringBuilder("idx");
        for (String p : parts) {
            sb.append(SEP).append(p);
        }
        return sb.toString();
    }
    private static String idxPrefix(String... parts) {
        return idxKey(parts); 
    }
    private static String suffixAfterPrefix(String key, String prefix) {
        int start = prefix.length();
        if (start < key.length() && key.charAt(start) == SEP) start++;
        return key.substring(start);
    }
    private static String norm(String s) { return s.toLowerCase(Locale.ROOT); }

    public void ingestNodes(Path nodesPgdf) throws IOException {
        try (BufferedReader br = Files.newBufferedReader(nodesPgdf, StandardCharsets.UTF_8)) {
            String line; String[] header = null;
            long written = 0;
            while ((line = br.readLine()) != null) {
                if (line.isBlank()) continue;
                if (line.startsWith("@")) {
                    header = Arrays.stream(line.split("\\|")).map(String::trim).toArray(String[]::new);
                    continue;
                }
                if (header == null) continue;

                String[] cols = line.split("\\|", -1);
                Map<String,String> row = new LinkedHashMap<>();
                for (int i=0;i<header.length && i<cols.length;i++) row.put(header[i], cols[i]);

                String nodeId = row.getOrDefault("@id","").trim();
                String label  = row.getOrDefault("@label","").trim();
                if (nodeId.isEmpty() || label.isEmpty()) continue;

                Map<String,String> props = new LinkedHashMap<>();
                for (var e : row.entrySet()){
                    String k = e.getKey();
                    if ("@id".equals(k) || "@label".equals(k)) continue;
                    String v = e.getValue()==null? "" : e.getValue();
                    props.put(k, v);
                }

                // Guardar nodo
                nodes.put(keyNode(nodeId), new NodeBlob(label, props));

                // Índice por propiedad (igualdad exacta, case-insensitive)
                for (var e : props.entrySet()) {
                    String val = e.getValue();
                    if (val == null || val.isEmpty()) continue;
                    index.add(idxKey("prop", e.getKey(), norm(val), nodeId));
                }

                if ((++written % 10_000) == 0) db.commit(); // commits periódicos
            }
            db.commit();
        }
    }

    public void ingestEdges(Path edgesPgdf) throws IOException {
        try (BufferedReader br = Files.newBufferedReader(edgesPgdf, StandardCharsets.UTF_8)) {
            String line; String[] header = null;
            long written = 0;
            while ((line = br.readLine()) != null) {
                if (line.isBlank()) continue;
                if (line.startsWith("@")) {
                    header = Arrays.stream(line.split("\\|")).map(String::trim).toArray(String[]::new);
                    continue;
                }
                if (header == null) continue;

                String[] cols = line.split("\\|", -1);
                Map<String,String> row = new HashMap<>();
                for (int i=0;i<header.length && i<cols.length;i++) row.put(header[i], cols[i]);

                String edgeId = row.getOrDefault("@id","").trim();
                String label  = row.getOrDefault("@label","").trim();
                String dir    = row.getOrDefault("@dir","T").trim();
                String src    = row.getOrDefault("@out","").trim();
                String dst    = row.getOrDefault("@in","").trim();
                if (label.isEmpty() || src.isEmpty() || dst.isEmpty()) continue;
                if (!"T".equalsIgnoreCase(dir)) continue;

                if (edgeId.isEmpty()) edgeId = makeEdgeId(src, label, dst);

                edges.put(keyEdge(edgeId), new EdgeBlob(label, src, dst));

                index.add(idxKey("label", "edge",     label, edgeId));
                index.add(idxKey("label", "srcnodes", label, src));
                index.add(idxKey("label", "dstnodes", label, dst));

                if ((++written % 20_000) == 0) db.commit();
            }
            db.commit();
        }
    }

    private static String makeEdgeId(String src, String label, String dst) {
        String s = src + "|" + label + "|" + dst;
        long x = 1125899906842597L;
        for (int i=0;i<s.length();i++) x = (x * 1315423911L) ^ s.charAt(i);
        return Long.toUnsignedString(x);
    }

    public NodeBlob getNode(String nodeId) {
        return nodes.get(keyNode(nodeId));
    }
    public EdgeBlob getEdge(String edgeId) {
        return edges.get(keyEdge(edgeId));
    }

    public void forEachEdgeIdByLabel(String label, Consumer<String> edgeIdConsumer) {
        final String prefix = idxPrefix("label","edge",label);
        for (String k : index.tailSet(prefix, true)) {
            if (!k.startsWith(prefix)) break;
            String edgeId = suffixAfterPrefix(k, prefix);
            edgeIdConsumer.accept(edgeId);
        }
    }

    public void forEachSourceNodeByLabel(String label, Consumer<String> nodeIdConsumer) {
        final String prefix = idxPrefix("label","srcnodes",label);
        for (String k : index.tailSet(prefix, true)) {
            if (!k.startsWith(prefix)) break;
            String nodeId = suffixAfterPrefix(k, prefix);
            nodeIdConsumer.accept(nodeId);
        }
    }

    public void forEachDestinationNodeByLabel(String label, Consumer<String> nodeIdConsumer) {
        final String prefix = idxPrefix("label","dstnodes",label);
        for (String k : index.tailSet(prefix, true)) {
            if (!k.startsWith(prefix)) break;
            String nodeId = suffixAfterPrefix(k, prefix);
            nodeIdConsumer.accept(nodeId);
        }
    }

    public void forEachNodeByPropertyEquals(String propName, String propValue, Consumer<String> nodeIdConsumer) {
        final String prefix = idxPrefix("prop", propName, norm(propValue));
        for (String k : index.tailSet(prefix, true)) {
            if (!k.startsWith(prefix)) break;
            String nodeId = suffixAfterPrefix(k, prefix);
            nodeIdConsumer.accept(nodeId);
        }
    }
}
