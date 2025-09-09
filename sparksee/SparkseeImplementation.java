import com.sparsity.sparksee.gdb.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.Locale;

import org.w3c.dom.Attr;

import static com.sparsity.sparksee.gdb.Objects.InvalidOID;

/*
  Uso: java -cp sparkseejava.jar:. SparkseePgdfLoader Nodes.pgdf Edges.pgdf [salida.gdb] [--get <ext_id>]
 */
public class SparkseeImplementation {

    private static final String DEFAULT_DB = "graph.gdb";
    private static final int NODE_BATCH = 50_000;
    private static final int EDGE_BATCH = 100_000;

    private final Map<String, Integer> nodeTypeIds = new HashMap<>();
    private final Map<String, Integer> edgeTypeIds = new HashMap<>();

    private final Map<Integer, Map<String, Integer>> attrsByType = new HashMap<>();
    private final Map<String, Long> oidByExtId = new HashMap<>(200_000);

    private int extIdAttr = Attribute.InvalidAttribute;

    public static void main(String[] args) throws Exception {
        // Usage args saving graph: -n Nodes.pgdf -e Edges.pgdf -d [output.gdb]
        // Usage args querying graph: -d [input.gdb] -g <node_ext_id>
        if (args.length < 3) {
            System.err.println(
                    "Usage: java -cp sparkseejava.jar:. SparkseeImplementation -n Nodes.pgdf -e Edges.pgdf -d [output.gdb]\n or java -cp sparkseejava.jar:. SparkseeImplementation -d [input.gdb] -g <ext_id>");
            System.exit(1);
        }

        Map<String, String> params = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("-") && i + 1 < args.length) {
                params.put(args[i], args[i + 1]);
                i++;
            }
        }

        SparkseeImplementation loader = new SparkseeImplementation();

        if (params.containsKey("-n") && params.containsKey("-e") && params.containsKey("-d")) {
            System.out.println("Saving graph...");
            loader.run(params.get("-n"), params.get("-e"), params.get("-d"));
            System.out.println("Graph saved.");
            return;
        }
        if (params.containsKey("-d") && params.containsKey("-g")) {
            System.out.println("Querying graph...");
            loader.run(params.get("-d"), params.get("-g"));
            System.out.println("Graph queried.");
            return;
        }

        System.out.println("Invalid arguments.");
        return;
    }

    private void run(String nodesPath, String edgesPath, String dbPath) throws Exception {
        SparkseeConfig cfg = new SparkseeConfig("sparksee.cfg");
        Sparksee sparksee = new Sparksee(cfg);
        Database db = null;
        Session sess = null;
        try {
            Files.deleteIfExists(Paths.get(dbPath));
            db = sparksee.create(dbPath, "PGDF");
            db.disableRollback();
            sess = db.newSession();
            Graph g = sess.getGraph();

            extIdAttr = ensureAttribute(g, Type.NodesType, "ext_id", DataType.String, AttributeKind.Unique);

            System.out.println("Loading nodes from " + nodesPath);

            loadNodes(sess, g, Paths.get(nodesPath));

            System.out.println("Loading edges from " + edgesPath);
            loadEdges(sess, g, Paths.get(edgesPath));

            System.out.println("Data saved to " + dbPath);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (sess != null)
                sess.close();
            if (db != null)
                db.close();
            sparksee.close();
        }
    }

    private void run(String dbPath, String wantedId)
            throws Exception {
        SparkseeConfig cfg = new SparkseeConfig("sparksee.cfg");
        Sparksee sparksee = new Sparksee(cfg);
        Database db = null;
        Session sess = null;

        try {
            System.out.println("Loading graph from " + dbPath);
            db = sparksee.open(dbPath, true);
            db.disableRollback();
            sess = db.newSession();
            Graph g = sess.getGraph();
            System.out.println("Graph loaded from " + dbPath);
            System.out.println("Getting node " + wantedId + " ...");
            extIdAttr = ensureAttribute(g, Type.NodesType, "ext_id", DataType.String, AttributeKind.Unique);
            getWantedNode(wantedId, sess, g);

        } finally {
            if (sess != null)
                sess.close();
            if (db != null)
                db.close();
            sparksee.close();
        }

    }

    private void getWantedNode(String wantedId, Session sess, Graph g) {
        if (wantedId != null && !wantedId.isEmpty()) {
            if (extIdAttr == Attribute.InvalidAttribute) {
                extIdAttr = g.findAttribute(Type.NodesType, "ext_id");
            }
        }
        printNodeByExtId(sess, g, wantedId);
    }

    private void loadNodes(Session sess, Graph g, Path nodesFile) throws IOException {
        try (BufferedReader br = Files.newBufferedReader(nodesFile, StandardCharsets.UTF_8)) {
            String line;
            String[] header = null;
            int count = 0;
            Value v = new Value();

            sess.beginUpdate();

            while ((line = br.readLine()) != null) {
                if (line.isEmpty())
                    continue;
                if (line.startsWith("@id|")) {
                    header = line.split("\\|", -1);
                    continue;
                }
                if (header == null)
                    continue;

                String[] cols = line.split("\\|", -1);
                if (cols.length != header.length) {
                    System.err.println("Fila con columnas inválidas (saltando): " + line);
                    continue;
                }

                String label = null, extId = null;
                for (int i = 0; i < header.length; i++) {
                    if ("@label".equals(header[i]))
                        label = cols[i];
                    else if ("@id".equals(header[i]))
                        extId = cols[i];
                }
                if (label == null || extId == null || label.isEmpty() || extId.isEmpty()) {
                    System.err.println("Fila sin @id/@label (saltando): " + line);
                    continue;
                }

                int typeId = ensureNodeType(g, label);
                long oid = g.newNode(typeId);
                oidByExtId.put(extId, oid);
                g.setAttribute(oid, extIdAttr, v.setString(extId));

                for (int i = 0; i < header.length; i++) {
                    String name = header[i];
                    if (name.equals("@id") || name.equals("@label"))
                        continue;
                    String val = cols[i];
                    if (val == null || val.isEmpty())
                        continue;

                    DataType dt = guessType(name);
                    int attrId = ensureAttribute(g, typeId, name, dt, AttributeKind.Basic);

                    switch (dt) {
                        case Integer:
                            try {
                                g.setAttribute(oid, attrId, v.setInteger(Integer.parseInt(val)));
                            } catch (NumberFormatException nfe) {
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

    private void loadEdges(Session sess, Graph g, Path edgesFile) throws IOException {
        try (BufferedReader br = Files.newBufferedReader(edgesFile, StandardCharsets.UTF_8)) {
            String line = br.readLine();
            if (line == null)
                return;

            String[] header = line.split("\\|", -1);
            Map<String, Integer> idx = new HashMap<>();
            for (int i = 0; i < header.length; i++)
                idx.put(header[i], i);

            int count = 0;
            sess.beginUpdate();

            while ((line = br.readLine()) != null) {
                if (line.isEmpty())
                    continue;
                String[] cols = line.split("\\|", -1);
                if (cols.length != header.length) {
                    System.err.println("Arista con columnas inválidas (saltando): " + line);
                    continue;
                }

                String label = cols[idx.get("@label")];
                String dir = cols[idx.get("@dir")];
                String outExt = cols[idx.get("@out")];
                String inExt = cols[idx.get("@in")];

                boolean directed = !"F".equalsIgnoreCase(dir);
                int edgeType = ensureEdgeType(g, label, directed);

                Long outOid = oidByExtId.get(outExt);
                Long inOid = oidByExtId.get(inExt);
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

    private int ensureNodeType(Graph g, String label) {
        Integer cached = nodeTypeIds.get(label);
        if (cached != null)
            return cached;

        int type = g.findType(label);
        if (type == Type.InvalidType) {
            type = g.newNodeType(label);
        }
        nodeTypeIds.put(label, type);
        return type;
    }

    private int ensureEdgeType(Graph g, String label, boolean directed) {
        Integer cached = edgeTypeIds.get(label);
        if (cached != null)
            return cached;
        int type = g.findType(label);
        if (type == Type.InvalidType) {
            type = g.newEdgeType(label, directed, true);
        }
        edgeTypeIds.put(label, type);
        return type;
    }

    private static DataType guessType(String attr) {
        String a = attr.toLowerCase(Locale.ROOT);
        if (a.equals("age")) {
            return DataType.Integer;
        }
        return DataType.String;
    }

    private int ensureAttribute(Graph g, int parentType, String name, DataType dt, AttributeKind kind) {

        Map<String, Integer> amap = attrsByType.computeIfAbsent(parentType, k -> new HashMap<>());
        Integer cached = amap.get(name);
        if (cached != null)
            return cached;

        int attr = g.findAttribute(parentType, name);
        if (attr == Attribute.InvalidAttribute) {
            attr = g.newAttribute(parentType, name, dt, kind);
        }
        amap.put(name, attr);
        return attr;
    }

    private void printNodeByExtId(Session sess, Graph g, String extId) {
        System.out.println("searching node ext_id='" + extId + "' ...");
        long startTime = System.nanoTime();
        sess.begin();
        try {
            Value v = new Value();
            long oid = g.findObject(extIdAttr, v.setString(extId));
            if (oid == InvalidOID) {
                System.out.println("Not found.");
                sess.commit();
                return;
            }

            int typeId = g.getObjectType(oid);
            Type t = g.getType(typeId);
            System.out.println("=== Nodo " + extId + " (OID=" + oid + ", tipo=" + t.getName() + ") ===");

            AttributeList attrs = g.getAttributes(oid);
            for (Integer attrId : attrs) {
                Attribute meta = g.getAttribute(attrId);
                String name = meta.getName();
                DataType dt = meta.getDataType();

                if (dt == DataType.Text) {
                    try (TextStream ts = g.getAttributeText(oid, attrId)) {
                        if (ts == null || ts.isNull()) {
                            System.out.println(name + " = <NULL>");
                        } else {
                            StringBuilder sb = new StringBuilder();
                            char[] buf = new char[1024];
                            int n;
                            while ((n = ts.read(buf, buf.length)) > 0 && sb.length() < 2048) {
                                sb.append(buf, 0, n);
                            }
                            String s = sb.toString();
                            if (s.length() > 512)
                                s = s.substring(0, 512) + "…";
                            System.out.println(name + " (TEXT) = " + s);
                        }
                    }
                    continue;
                }

                Value val = new Value();
                g.getAttribute(oid, attrId, val);

                String out;
                switch (dt) {
                    case Boolean:
                        out = Boolean.toString(val.getBoolean());
                        break;
                    case Integer:
                        out = Integer.toString(val.getInteger());
                        break;
                    case Long:
                        out = Long.toString(val.getLong());
                        break;
                    case Double:
                        out = Double.toString(val.getDouble());
                        break;
                    case Timestamp:
                        out = new java.util.Date(val.getLong()).toString();
                        break;
                    case OID:
                        out = Long.toString(val.getOID());
                        break;
                    case String:
                    default:
                        out = val.getString();
                }
                System.out.println(name + " = " + out);
            }

            long endTime = System.nanoTime();
            System.out.printf(" (%.2f ms)\n", (endTime - startTime) / 1_000_000.0);
            sess.commit();
        } catch (RuntimeException e) {
            try {
                sess.rollback();
            } catch (Exception ignore) {
            }
            throw e;
        }
    }

}
