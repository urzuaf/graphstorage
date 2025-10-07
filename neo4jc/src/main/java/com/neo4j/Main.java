package com.neo4j;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Query;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Record;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

import static org.neo4j.driver.Values.parameters;

public class Main {

    private static final int NODE_BATCH = 100;
    private static final int EDGE_BATCH = 100;

    public static void main(String[] args) throws Exception {
        if (args.length < 4) usage();

        String uri  = args[0];
        String user = args[1];
        String pass = args[2];

        boolean queryMode = args[3].startsWith("-");

        try (Driver driver = GraphDatabase.driver(uri, AuthTokens.basic(user, pass));
             Session session = driver.session()) {

            if (!queryMode) {
                // Ingesta: <uri> <user> <pass> <Nodes.pgdf> <Edges.pgdf>
                if (args.length < 5) usage();
                Path nodesFile = Paths.get(args[3]);
                Path edgesFile = Paths.get(args[4]);

                clearDatabase(session);
                ensureSchema(session);
                session.run("CALL db.awaitIndexes()");

                long t0 = System.nanoTime();
                ingestNodes(session, nodesFile);
                ingestEdges(session, edgesFile);
                long t1 = System.nanoTime();

                System.out.printf(Locale.ROOT,
                        "Ingesta completada : %.3f ms%n",(t1 - t0) / 1e6);
                return;
            }

            // Consultas
            if (args.length < 5) usage();
            String flag = args[3];
            switch (flag) {
                case "-g" -> {
                    String nodeId = args[4];
                    queryNodeWithAllProps(session, nodeId);
                }
                case "-gl" -> {
                    String label = args[4];
                    queryEdgeIdsByLabel(session, label);
                }
                case "-nv" -> {
                    String spec = args[4]; // key=valor
                    int p = spec.indexOf('=');
                    if (p <= 0 || p == spec.length() - 1) {
                        System.err.println("Formato inválido para -nv. Usa atributo=valor");
                        System.exit(2);
                    }
                    String key = spec.substring(0, p);
                    String val = spec.substring(p + 1);
                    queryNodesByPropEquals(session, key, val);
                }
                default -> usage();
            }
        }
 
    }

    private static void usage() {
        System.err.println("""
            Uso:

              Ingesta:
                mvn compile exec:java -Dexec.mainClass="com.neo4j.Main" -Dexec.args="<uri> <user> <pass> <Nodes.pgdf> <Edges.pgdf>"

              Consultas:
                1) Nodo + todas sus propiedades
                   mvn compile exec:java -Dexec.mainClass="com.neo4j.Main" -Dexec.args="<uri> <user> <pass> -g <node_id>"

                2) IDs de aristas por etiqueta
                   mvn compile exec:java -Dexec.mainClass="com.neo4j.Main" -Dexec.args="<uri> <user> <pass> -gl <label>"

                3) Nodos con propiedad=valor (case-insensitive exact)
                   mvn compile exec:java -Dexec.mainClass="com.neo4j.Main" -Dexec.args="<uri> <user> <pass> -nv <atributo=valor>"
            """);
        System.exit(2);
    }

    private static void ensureSchema(Session session) {
        // Índices en nodoss
        session.run("CREATE INDEX node_id IF NOT EXISTS FOR (n:Node) ON (n.id)");
        session.run("CREATE INDEX node_label IF NOT EXISTS FOR (n:Node) ON (n.label)");

    }

    //Ingesta de nodos
 private static void ingestNodes(Session session, Path nodesPgdf) throws IOException {
    try (BufferedReader br = Files.newBufferedReader(nodesPgdf, StandardCharsets.UTF_8)) {
        String line;
        String[] header = null;
        long nCount = 0;

        // agrupar nodos por label
        Map<String, List<Map<String, Object>>> byLabel = new HashMap<>();

        while ((line = br.readLine()) != null) {
            if (line.isBlank()) continue;

            if (line.startsWith("@")) {
                header = line.split("\\|", -1);
                continue;
            }
            if (header == null) continue;

            String[] cols = line.split("\\|", -1);
            Map<String, String> row = new LinkedHashMap<>();
            for (int i = 0; i < header.length && i < cols.length; i++) {
                row.put(header[i], cols[i]);
            }

            String id = nonNull(row.get("@id")).trim();
            String label = nonNull(row.get("@label")).trim();
            if (id.isEmpty() || label.isEmpty()) continue;

            Map<String, Object> props = new LinkedHashMap<>();
            for (var e : row.entrySet()) {
                String k = e.getKey();
                if ("@id".equals(k) || "@label".equals(k)) continue;
                String v = nonNull(e.getValue()).trim();
                if (!v.isEmpty()) props.put(k, v);
            }

            Map<String, Object> nodeData = new HashMap<>();
            nodeData.put("id", id);
            nodeData.put("props", props);
            byLabel.computeIfAbsent(label, k -> new ArrayList<>()).add(nodeData);
            nCount++;
        }

        // insertar cada grupo con su propio label
        for (var entry : byLabel.entrySet()) {
            String label = entry.getKey();
            List<Map<String, Object>> batch = entry.getValue();

            String cypher = String.format("""
                UNWIND $batch AS row
                CREATE (n:%s {id: row.id})
                SET n += row.props
                """, label.replaceAll("[^A-Za-z0-9_]", "_")); // sanitize label

            try (Transaction tx = session.beginTransaction()) {
                tx.run(new Query(cypher, parameters("batch", batch)));
                tx.commit();
            }
        }

        System.out.printf(Locale.ROOT, "  Nodes: %,d%n", nCount);
    }
}


    //Ingesta de aristas
// Ingesta de aristas (sin tx exterior; tx por lote)
private static void ingestEdges(Session session, Path edgesPgdf) throws IOException {
    try (BufferedReader br = Files.newBufferedReader(edgesPgdf, StandardCharsets.UTF_8)) {
        String line = br.readLine();
        if (line == null) {
            System.out.println("  Edges: 0");
            return;
        }

        String[] header = line.split("\\|", -1);
        Map<String, Integer> idx = new HashMap<>();
        for (int i = 0; i < header.length; i++) idx.put(header[i], i);

        long eCount = 0;
        int batchCount = 0;
        List<Map<String, Object>> batch = new ArrayList<>(EDGE_BATCH);

        while ((line = br.readLine()) != null) {
            if (line.isBlank() || line.startsWith("@")) continue;

            String[] cols = line.split("\\|", -1);
            String eid = safe(cols, idx.get("@id"));
            String lab = safe(cols, idx.get("@label"));
            String dir = safe(cols, idx.get("@dir"));
            String src = safe(cols, idx.get("@out"));
            String dst = safe(cols, idx.get("@in"));

            if (lab.isEmpty() || src.isEmpty() || dst.isEmpty()) continue;

            boolean directed = !"F".equalsIgnoreCase(dir.isEmpty() ? "T" : dir);
            if (eid.isEmpty()) eid = makeEdgeId(src, lab, dst);

            batch.add(Map.of(
                "eid", eid,
                "label", lab,
                "src", src,
                "dst", dst,
                "directed", directed
            ));

            batchCount++;
            eCount++;

            if (batchCount >= EDGE_BATCH) {
                try (Transaction tx = session.beginTransaction()) {
                    executeEdgeBatch(tx, batch); // una sola tx por lote
                    tx.commit();
                }
                batch.clear();
                batchCount = 0;
            }
        }

        if (!batch.isEmpty()) {
            try (Transaction tx = session.beginTransaction()) {
                executeEdgeBatch(tx, batch);
                tx.commit();
            }
            batch.clear();
        }

        System.out.printf(Locale.ROOT, "  Edges: %,d%n", eCount);
    }
}

// Ejecuta un lote (MATCH + CREATE para ahorrar memoria; MERGE solo en la relación)
private static void executeEdgeBatch(Transaction tx, List<Map<String, Object>> batch) {
    Map<String, List<Map<String, Object>>> grouped = new HashMap<>();
    for (Map<String, Object> edge : batch) {
        String label = (String) edge.get("label");
        grouped.computeIfAbsent(label, k -> new ArrayList<>()).add(edge);
    }

    for (Map.Entry<String, List<Map<String, Object>>> entry : grouped.entrySet()) {
        String relType = relType(entry.getKey());

        String cypher = """
            UNWIND $batch AS row
            MATCH (src:Node {id: row.src})
            MATCH (dst:Node {id: row.dst})
            MERGE (src)-[r:%s {id: row.eid}]->(dst)
            SET r.directed = row.directed
            """.formatted(relType);

        tx.run(new Query(cypher, parameters("batch", entry.getValue())));
    }
}


    // Consultas

    // -g <node_id>
    private static void queryNodeWithAllProps(Session session, String nodeId) {
        long t0 = System.nanoTime();
        String cypher = "MATCH (n:Node {id: $id}) RETURN n";
        Result result = session.run(cypher, parameters("id", nodeId));

        if (!result.hasNext()) {
            System.out.println("Node not found: " + nodeId);
            return;
        }
        Record rec = result.next();
        var node = rec.get("n").asNode();
        Map<String,Object> props = node.asMap();

        System.out.println("Node ID: " + nodeId);
        Object label = props.getOrDefault("label", "");
        System.out.println("Label:   " + label);

        props.entrySet().stream()
                .filter(e -> !e.getKey().equals("id") && !e.getKey().equals("label"))
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> System.out.println("  " + e.getKey() + " = " + e.getValue()));
        long t1 = System.nanoTime();
        System.out.printf(Locale.ROOT, "Query time: %.3f ms%n", (t1 - t0) / 1e6);
    }

    // -gl <label>  
    private static void queryEdgeIdsByLabel(Session session, String label) {
        long t0 = System.nanoTime();
        String relType = relType(label);
        String cypher = "MATCH ()-[r:" + relType + "]->() RETURN r.id AS id";
        Result result = session.run(cypher);

        long count = 0;
        final long LIMIT_PRINT = 10;
        while (result.hasNext()) {
            Record rec = result.next();
            count++;
            if (count <= LIMIT_PRINT) {
                System.out.println(rec.get("id").asString());
            }
        }
        if (count > LIMIT_PRINT) {
            System.out.println("… (mostrando solo los primeros " + LIMIT_PRINT + " resultados)");
        }
        System.err.printf(Locale.ROOT, "Total edgeIds: %,d%n", count);
        long t1 = System.nanoTime();
        System.out.printf(Locale.ROOT, "Query time: %.3f ms%n", (t1 - t0) / 1e6);
    }

    // -nv <atributo=valor> 
    private static void queryNodesByPropEquals(Session session, String key, String value) {
        long t0 = System.nanoTime();
        String cypher = """
        MATCH (n:Node)
        WHERE properties(n)[$key] = $val
        RETURN n.id AS id
        """;
        Result result = session.run(cypher, parameters("key", key, "val", value));

        long count = 0;
        final long LIMIT_PRINT = 10;
        while (result.hasNext()) {
            Record rec = result.next();
            count++;
            if (count <= LIMIT_PRINT) {
                System.out.println(rec.get("id").asString());
            }
        }
        if (count > LIMIT_PRINT) {
            System.out.println("… (mostrando solo los primeros " + LIMIT_PRINT + " resultados)");
        }
        System.err.printf(Locale.ROOT, "Total nodes: %,d%n", count);
        long t1 = System.nanoTime();
        System.out.printf(Locale.ROOT, "Query time: %.3f ms%n", (t1 - t0) / 1e6);
    }


    private static String nonNull(String s){ return (s == null) ? "" : s; }
    private static String safe(String[] a, Integer i){
        if (i == null || i < 0 || i >= a.length) return "";
        String s = a[i];
        return s == null ? "" : s;
    }
    private static String makeEdgeId(String src, String label, String dst) {
        String s = src + "|" + label + "|" + dst;
        long x = 1125899906842597L;
        for (int i=0;i<s.length();i++) x = (x * 1315423911L) ^ s.charAt(i);
        return Long.toUnsignedString(x);
    }

    private static String relType(String label) {
        String norm = label.replaceAll("[^A-Z0-9_]", "_");
        if (norm.isEmpty()) norm = "REL";
        // Asegurar que no empiece por numero
        if (Character.isDigit(norm.charAt(0))) norm = "_" + norm;
        return norm;
    }
    private static void clearDatabase(Session session) {
    System.out.println("Preparando BD para inserción…");
    long total = 0;
    final int LIMIT = 100; 

    while (true) {
        Result r = session.run("""
            MATCH (n)
            WITH n LIMIT $limit
            DETACH DELETE n
            RETURN count(*) AS c
        """, parameters("limit", LIMIT));

        long c = r.single().get("c").asLong();
        total += c;
        if (c == 0) break;
        System.out.printf(Locale.ROOT, "  borrados en este paso: %,d (acumulado: %,d)%n", c, total);
    }
    System.out.println("Base limpia.");
}


}
