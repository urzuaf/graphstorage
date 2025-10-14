package com.rocks;

import java.nio.file.Path;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicLong;
import java.util.List;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

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
            case "experiment" -> {
                String rutaArchivo = args[2];
                String db = args[1];

                try (GraphStore store = GraphStore.open(Path.of(db))) {
                    GraphAPI api = new GraphAPI(store);
                    medirTiempos(api, rutaArchivo);
                }
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
        public static void medirTiempos(GraphAPI api,String rutaArchivo) {
        List<Long> tiempos = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(rutaArchivo))) {
            String linea;
            int contador = 0;
            while ((linea = br.readLine()) != null) {
                try
                {
                    long inicio = System.nanoTime(); // tiempo en nanosegundos
                    api.getNode(linea);
                    long fin = System.nanoTime();
                    long duracion = fin - inicio;
                    tiempos.add(duracion);
                    System.out.printf("Línea %d procesada en %.3f ms%n", ++contador, duracion / 1_000_000.0);
                    
                }catch(Exception e)
                {
                    System.out.printf("Error al procesar la línea %d: %s%n", ++contador, e.getMessage());
                }

            }

            if (!tiempos.isEmpty()) {
                double promedio = tiempos.stream()
                                         .mapToLong(Long::longValue)
                                         .average()
                                         .orElse(0.0);
                System.out.printf("%nTiempo promedio: %.3f ms%n", promedio / 1_000_000.0);
            } else {
                System.out.println("El archivo está vacío.");
            }

        } catch (IOException e) {
            e.printStackTrace();
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
