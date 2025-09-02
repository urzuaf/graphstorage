package cbin;

import cbin.core.BinaryTransformer;
import cbin.io.BinaryGraphFiles;
import cbin.io.PgdfReader;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) usage();

        String cmd = args[0];
        switch (cmd) {
            case "ingest" -> {
                if (args.length < 4) usage();
                Path nodes = Paths.get(args[1]);
                Path edges = Paths.get(args[2]);
                Path out   = Paths.get(args[3]);

                System.out.println("data"+ nodes.toString()+ edges.toString()+ out.toString());
                long t0 = System.nanoTime();

                BinaryTransformer transformer = new BinaryTransformer(out);

                //Nodes streaming
                PgdfReader.readNodes(nodes, transformer::acceptNodeRow);
                transformer.finishNodesPass1(); // pools ids + props temporales

                // Construir nodes.id.lex 
                transformer.buildNodesIdLex();

                // Primera pasada de aristas para recolectar labels de edges
                PgdfReader.readEdges(edges, transformer::collectEdgeLabelOnly);

                // Construir diccionarios labels de nodos+aristas; propnames; propvals
                transformer.buildDictionaries();

                // Re-escribir nodes.rec con IDs definitivos a partir de props temporales
                transformer.materializeNodesRec();

                // Segunda pasada de aristas: escribir edges.rec/off + índices provisionales
                PgdfReader.readEdges(edges, transformer::acceptEdgeRow);
                transformer.finishEdges();

                // Construir índices finales posting lists + directorios
                transformer.buildIndexes();

                long t1 = System.nanoTime();
                System.out.printf(Locale.ROOT, "Ingesta terminada en %.3f ms%n", (t1 - t0) / 1e6);
            }
            case "q-node" -> {
                if (args.length < 3) usage();
                Path outDir = Paths.get(args[1]);
                String nodeId = args[2];

                long t0 = System.nanoTime();
                var res = BinaryGraphFiles.queryNodeById(outDir, nodeId);
                long t1 = System.nanoTime();
                System.out.printf(Locale.ROOT, "Ingesta terminada en %.3f ms%n", (t1 - t0) / 1e6);
                if (res == null) {
                    System.out.println("Node not found");
                } else {
                    System.out.println("label=" + res.label);
                    System.out.println("props=" + res.props);
                }
            }
            default -> usage();
        }
    }

    private static void usage() {
        System.err.println("""
          Uso:
            Ingest:
               ingest nodes.pgdf edges.pgdf /path/to/outDir

            Consulta nodo:
               q-node /path/to/outDir <nodeId>
        """);
        System.exit(2);
    }
}
