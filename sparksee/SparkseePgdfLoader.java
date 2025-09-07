import com.sparsity.sparksee.gdb.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/**
 * Cargador PGDF -> Sparksee 6.0.2
 * Lee Nodes.pgdf y Edges.pgdf y guarda un .gdb en disco.
 *
 * Uso:
 *   java -cp sparkseejava.jar:. SparkseePgdfLoader Nodes.pgdf Edges.pgdf [salida.gdb]
 */
public class SparkseePgdfLoader {

    private static final String DEFAULT_DB = "graph.gdb";
    private static final int NODE_BATCH = 50_000;
    private static final int EDGE_BATCH = 100_000;

    private final Map<String, Integer> nodeTypeIds = new HashMap<>();
    private final Map<String, Integer> edgeTypeIds = new HashMap<>();
    private final Map<Integer, Map<String, Integer>> attrsByType = new HashMap<>();
    private final Map<String, Long> oidByExtId = new HashMap<>(200_000);

    private int extIdAttr = Attribute.InvalidAttribute;

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Uso: java -cp sparkseejava.jar:. SparkseePgdfLoader <Nodes.pgdf> <Edges.pgdf> [salida.gdb]");
            System.exit(1);
        }
        String nodesPath = args[0];
        String edgesPath = args[1];
        String dbPath = (args.length >= 3) ? args[2] : DEFAULT_DB;

        SparkseePgdfLoader loader = new SparkseePgdfLoader();
        loader.run(nodesPath, edgesPath, dbPath);
    }

private void run(String nodesPath, String edgesPath, String dbPath) throws Exception {
    SparkseeConfig cfg = new SparkseeConfig("sparksee.cfg");
    String cid = System.getenv("CLIENT_ID");
    String lic = System.getenv("LICENSE_ID");
    if (cid != null && !cid.isEmpty()) cfg.setClientId(cid);
    if (lic != null && !lic.isEmpty()) cfg.setLicenseId(lic);

    Sparksee sparksee = new Sparksee(cfg);
    Database db = null;
    Session sess = null;
    try {
        db = sparksee.create(dbPath, "PGDF");
        db.disableRollback();  // opcional para ingesta masiva
        sess = db.newSession();
        Graph g = sess.getGraph();

        // Atributo global de id externo (único)
        extIdAttr = ensureAttribute(g, Type.NodesType, "ext_id", DataType.String, AttributeKind.Unique);

        System.out.println("Cargando nodos desde: " + nodesPath);
        loadNodes(sess, g, Paths.get(nodesPath));   // ya hace begin/commit internos

        System.out.println("Cargando aristas desde: " + edgesPath);
        loadEdges(sess, g, Paths.get(edgesPath));   // ya hace begin/commit internos

        // ¡Listo! No hagas commit aquí: no hay transacción abierta.
        System.out.println("Hecho. DB guardada en: " + dbPath);
    } finally {
        if (sess != null) sess.close();
        if (db != null) db.close();
        sparksee.close();
    }
}


    /* =========================
       NODES
       ========================= */
    private void loadNodes(Session sess, Graph g, Path nodesFile) throws IOException {
        try (BufferedReader br = Files.newBufferedReader(nodesFile, StandardCharsets.UTF_8)) {
            String line;
            String[] header = null;
            int count = 0;
            Value v = new Value();

            sess.beginUpdate(); // empezamos transacción de escritura

            while ((line = br.readLine()) != null) {
                if (line.isEmpty()) continue;
                // Encabezado: líneas que empiezan con "@id|"
                if (line.startsWith("@id|")) {
                    header = line.split("\\|", -1);
                    continue;
                }
                if (header == null) continue; // aún no hay encabezado

                String[] cols = line.split("\\|", -1);
                if (cols.length != header.length) {
                    System.err.println("Fila con columnas inválidas (saltando): " + line);
                    continue;
                }

                Map<String,String> row = new HashMap<>(header.length * 2);
                for (int i = 0; i < header.length; i++) row.put(header[i], cols[i]);

                String label = row.get("@label");
                String extId = row.get("@id");
                if (label == null || extId == null || label.isEmpty() || extId.isEmpty()) {
                    System.err.println("Fila sin @id/@label (saltando): " + line);
                    continue;
                }

                int typeId = ensureNodeType(g, label);
                // Crear nodo
                long oid = g.newNode(typeId);
                // Guardar map para aristas
                oidByExtId.put(extId, oid);
                // Setear ext_id único global
                g.setAttribute(oid, extIdAttr, v.setString(extId));

                // Atributos por fila (excepto especiales)
                for (int i = 0; i < header.length; i++) {
                    String name = header[i];
                    if (name.equals("@id") || name.equals("@label")) continue;
                    String val = cols[i];
                    if (val == null || val.isEmpty()) continue;

                    DataType dt = guessType(name);
                    int attrId = ensureAttribute(g, typeId, name, dt, AttributeKind.Basic);

                    switch (dt) {
                        case Integer:
                            try {
                                g.setAttribute(oid, attrId, v.setInteger(Integer.parseInt(val)));
                            } catch (NumberFormatException nfe) {
                                // fallback a string si viene sucio
                                g.setAttribute(oid, attrId, v.setString(val));
                            }
                            break;
                        default:
                            g.setAttribute(oid, attrId, v.setString(val));
                    }
                }

                count++;
                if (count % NODE_BATCH == 0) {
                    sess.commit();
                    sess.beginUpdate();
                    System.out.println("Nodos procesados: " + count);
                }
            }

            sess.commit();
            System.out.println("Total nodos: " + count);
        }
    }

    /* =========================
       EDGES
       ========================= */
    private void loadEdges(Session sess, Graph g, Path edgesFile) throws IOException {
        try (BufferedReader br = Files.newBufferedReader(edgesFile, StandardCharsets.UTF_8)) {
            String line = br.readLine();
            if (line == null) return;

            // Espera encabezado: @id|@label|@dir|@out|@in
            String[] header = line.split("\\|", -1);
            Map<String, Integer> idx = new HashMap<>();
            for (int i = 0; i < header.length; i++) idx.put(header[i], i);

            Value v = new Value();
            int count = 0;
            sess.beginUpdate();

            while ((line = br.readLine()) != null) {
                if (line.isEmpty()) continue;
                String[] cols = line.split("\\|", -1);
                if (cols.length != header.length) {
                    System.err.println("Arista con columnas inválidas (saltando): " + line);
                    continue;
                }

                String label = cols[idx.get("@label")];
                String dir = cols[idx.get("@dir")];
                String outExt = cols[idx.get("@out")];
                String inExt  = cols[idx.get("@in")];

                boolean directed = !"F".equalsIgnoreCase(dir); // 'T' => true
                int edgeType = ensureEdgeType(g, label, directed);

                Long outOid = oidByExtId.get(outExt);
                Long inOid  = oidByExtId.get(inExt);
                if (outOid == null || inOid == null) {
                    System.err.println("No se encontró nodo para arista: " + line);
                    continue;
                }

                g.newEdge(edgeType, outOid, inOid);

                count++;
                if (count % EDGE_BATCH == 0) {
                    sess.commit();
                    sess.beginUpdate();
                    System.out.println("Aristas procesadas: " + count);
                }
            }

            sess.commit();
            System.out.println("Total aristas: " + count);
        }
    }

    /* =========================
       HELPERS
       ========================= */
    private int ensureNodeType(Graph g, String label) {
        Integer cached = nodeTypeIds.get(label);
        if (cached != null) return cached;
        int type = g.findType(label);
        if (type == Type.InvalidType) {
            type = g.newNodeType(label);
        }
        nodeTypeIds.put(label, type);
        return type;
    }

    private int ensureEdgeType(Graph g, String label, boolean directed) {
        Integer cached = edgeTypeIds.get(label);
        if (cached != null) return cached;
        int type = g.findType(label);
        if (type == Type.InvalidType) {
            // true = crea índice de vecinos (mejora traversal)
            type = g.newEdgeType(label, directed, true);
        }
        edgeTypeIds.put(label, type);
        return type;
    }

    private int ensureAttribute(Graph g, int parentType, String name, DataType dt, AttributeKind kind) {
        Map<String, Integer> amap = attrsByType.computeIfAbsent(parentType, k -> new HashMap<>());
        Integer cached = amap.get(name);
        if (cached != null) return cached;

        int attr = g.findAttribute(parentType, name);
        if (attr == Attribute.InvalidAttribute) {
            attr = g.newAttribute(parentType, name, dt, kind);
        }
        amap.put(name, attr);
        return attr;
    }

    private static DataType guessType(String attr) {
        String a = attr.toLowerCase(Locale.ROOT);
        if (a.equals("age") || a.equals("birth_year") || a.equals("founded_year")) {
            return DataType.Integer;
        }
        return DataType.String;
    }
}
