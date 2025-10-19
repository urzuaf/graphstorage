package com.rocks;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Objects;

import com.rocks.db.GraphAPI;
import com.rocks.db.GraphStore;

public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) usage();

        // Subcomando "ingest" (igual que antes)
        if (Objects.equals(args[0], "ingest")) {
            if (args.length < 4) usage();
            Path nodes = Path.of(args[1]);
            Path edges = Path.of(args[2]);
            Path db    = Path.of(args[3]);
            try (GraphStore store = GraphStore.open(db)) {
                GraphAPI api = new GraphAPI(store);
                System.out.println("Ingestando nodos...");
                api.ingestNodes(nodes);
                System.out.println("Ingestando aristas...");
                api.ingestEdges(edges);
            }
            System.out.println("OK");
            return;
        }

        // Modo consultas por parámetros
        if (args.length < 2) usage();
        Path dbPath = Path.of(args[0]);
        String flag = args[1];

        try (GraphStore store = GraphStore.open(dbPath)) {
            GraphAPI api = new GraphAPI(store);

            switch (flag) {
                case "-g" -> { // ./db -g nodeId
                    if (args.length < 3) usage();
                    String nodeId = args[2];
                    long start = System.nanoTime();
                    var n = api.getNode(nodeId);
                    long end = System.nanoTime();
                    if (n == null) {
                        System.out.println("Node not found");
                    } else {
                        System.out.println("label=" + n.label);
                        System.out.println("props=" + n.props);
                    }
                    System.out.printf("Tiempo (getNode): %.3f ms%n", (end - start) / 1e6);
                }

                case "-gl" -> { // ./db -gl label
                    if (args.length < 3) usage();
                    String label = args[2];
                    long start = System.nanoTime();
                    AtomicLong c = new AtomicLong();
                    api.forEachEdgeIdByLabel(label, eid -> {
                        long idx = c.incrementAndGet();
                        if (idx <= 10) System.out.println(eid);
                    });
                    long end = System.nanoTime();
                    System.out.printf("Total edgeIds: %d (%.3f ms)%n", c.get(), (end - start) / 1e6);
                }

                case "-nv" -> { // ./db -nv key=value
                    if (args.length < 3 || !args[2].contains("=")) usage();
                    String[] kv = args[2].split("=", 2);
                    String key = kv[0];
                    String val = kv[1];
                    long start = System.nanoTime();
                    AtomicLong c = new AtomicLong();
                    api.forEachNodeByPropertyEquals(key, val, nid -> {
                        long idx = c.incrementAndGet();
                        if (idx <= 10) System.out.println(nid);
                    });
                    long end = System.nanoTime();
                    System.out.printf("Total nodes: %d (%.3f ms)%n", c.get(), (end - start) / 1e6);
                }

                default -> usage();
            }
        }
    }

    private static void usage() {
        System.err.println("""
          Uso:
            Ingesta:
              java -jar app.jar ingest nodes.pgdf edges.pgdf /path/to/db

            Consultas (sin TUI):
              java -jar app.jar /path/to/db -g  <nodeId>
              java -jar app.jar /path/to/db -gl <label>
              java -jar app.jar /path/to/db -nv key=value

          Notas:
            - Se imprimen hasta 10 resultados y el total, con tiempo en ms.
            - -nv requiere 'key=value'.
        """);
        System.exit(2);
    }
}
