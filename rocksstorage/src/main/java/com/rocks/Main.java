package com.rocks;

import java.nio.file.Path;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicLong;

import com.rocks.db.GraphAPI;
import com.rocks.db.GraphStore;

public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) usage();

        String cmd = args[0];

        switch (cmd) {
            case "ingest" -> {
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
            }

            default -> {
                Path dbPath = Path.of(cmd);
                try (GraphStore store = GraphStore.open(dbPath)) {
                    GraphAPI api = new GraphAPI(store);
                    runTUI(api);
                }
            }
        }
    }

    private static void runTUI(GraphAPI api) throws Exception {
        try (Scanner sc = new Scanner(System.in)) {
            while (true) {
                clearConsole();
                System.out.println("""
                    === Graph Query TUI ===
                    Seleccione una opción:
                      1) IDs de aristas por etiqueta
                      2) nodeIds que son source por etiqueta
                      3) nodeIds que son destination por etiqueta
                      4) nodo + todas sus propiedades
                      5) nodos con propiedad=valor
                      0) salir
                    """);
                System.out.print("Opción: ");
                String choice = sc.nextLine().trim();
                if (choice.equals("0") || choice.equalsIgnoreCase("exit")) {
                    System.out.println("Saliendo...");
                    break;
                }

                switch (choice) {
                    case "1" -> {
                        System.out.print("Etiqueta: ");
                        String label = sc.nextLine().trim();
                        long start = System.nanoTime();
                        AtomicLong c = new AtomicLong();
                        api.forEachEdgeIdByLabel(label, eid -> {
                            long idx = c.incrementAndGet();
                            if (idx <= 10) System.out.println(eid);
                        });
                        long end = System.nanoTime();
                        System.out.printf("Total edgeIds: %d (%.3f ms)%n", c.get(), (end - start) / 1e6);
                        promptEnterKey();
                    }
                    case "2" -> {
                        System.out.print("Etiqueta: ");
                        String label = sc.nextLine().trim();
                        long start = System.nanoTime();
                        AtomicLong c = new AtomicLong();
                        api.forEachSourceNodeByLabel(label, nid -> {
                            long idx = c.incrementAndGet();
                            if (idx <= 10) System.out.println(nid);
                        });
                        long end = System.nanoTime();
                        System.out.printf("Total source nodes: %d (%.3f ms)%n", c.get(), (end - start) / 1e6);
                        promptEnterKey();
                    }
                    case "3" -> {
                        System.out.print("Etiqueta: ");
                        String label = sc.nextLine().trim();
                        long start = System.nanoTime();
                        AtomicLong c = new AtomicLong();
                        api.forEachDestinationNodeByLabel(label, nid -> {
                            long idx = c.incrementAndGet();
                            if (idx <= 10) System.out.println(nid);
                        });
                        long end = System.nanoTime();
                        System.out.printf("Total destination nodes: %d (%.3f ms)%n", c.get(), (end - start) / 1e6);
                        promptEnterKey();
                    }
                    case "4" -> {
                        System.out.print("NodeId: ");
                        String nodeId = sc.nextLine().trim();
                        long start = System.nanoTime();
                        var n = api.getNode(nodeId);
                        long end = System.nanoTime();
                        if (n == null) {
                            System.out.println("Node not found");
                        } else {
                            System.out.println("label=" + n.label);
                            System.out.println("props=" + n.props);
                        }
                        System.out.printf("Consulta terminada en %.3f ms%n", (end - start) / 1e6);
                        promptEnterKey();
                    }
                    case "5" -> {
                        System.out.print("Propiedad: ");
                        String prop = sc.nextLine().trim();
                        System.out.print("Valor: ");
                        String val = sc.nextLine().trim();
                        long start = System.nanoTime();
                        AtomicLong c = new AtomicLong();
                        api.forEachNodeByPropertyEquals(prop, val, nid -> {
                            long idx = c.incrementAndGet();
                            if (idx <= 10) System.out.println(nid);
                        });
                        long end = System.nanoTime();
                        System.out.printf("Total nodes: %d (%.3f ms)%n", c.get(), (end - start) / 1e6);
                        promptEnterKey();
                    }
                    default -> {
                        System.out.println("Opción inválida.");
                        promptEnterKey();
                    }
                }
            }
        }
    }

    public static void clearConsole() {
        try {
            String os = System.getProperty("os.name");
            if (os.contains("Windows")) {
                new ProcessBuilder("cmd", "/c", "cls").inheritIO().start().waitFor();
            } else {
                System.out.print("\033[H\033[2J");
                System.out.flush();
            }
        } catch (Exception ignored) {}
    }

    private static void promptEnterKey() {
        System.out.println("Presiona ENTER para continuar...");
        try {
            System.in.read();
        } catch (Exception ignored) {}
    }

    private static void usage() {
        System.err.println("""
          Uso:
            Ingesta:
              java -jar app.jar ingest nodes.pgdf edges.pgdf /path/to/db

            TUI (consultas):
              java -jar app.jar /path/to/db
        """);
        System.exit(2);
    }
}
