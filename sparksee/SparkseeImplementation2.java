import com.sparsity.sparksee.gdb.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.Locale;

import static com.sparsity.sparksee.gdb.Objects.InvalidOID;

/*
  Uso:

  Guardar grafo:
    java -cp sparkseejava.jar:. SparkseeImplementation -n Nodes.pgdf -e Edges.pgdf -d graph.gdb
  Consultar nodo por id:
    java -cp sparkseejava.jar:. SparkseeImplementation -d graph.gdb -g P42
  listar IDs de aristas por label:
    java -cp sparkseejava.jar:. SparkseeImplementation -d graph.gdb -gel Knows
 */
public class SparkseeImplementation2 {

    private static final String DEFAULT_DB = "graph.gdb";
    private static final int NODE_BATCH = 50_000;
    private static final int EDGE_BATCH = 100_000;

    private final Map<String, Integer> nodeTypeIds = new HashMap<>();
    private final Map<String, Integer> edgeTypeIds = new HashMap<>();

    private final Map<Integer, Map<String, Integer>> attrsByType = new HashMap<>();
    private final Map<String, Long> oidByExtId = new HashMap<>(200_000);

    private int extIdAttr = Attribute.InvalidAttribute;

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println(
                    "Usage:\n" +
                            "  Save:  java -cp sparkseejava.jar:. SparkseeImplementation -n Nodes.pgdf -e Edges.pgdf -d graph.gdb\n"
                            +
                            "  GetNodeById:  java -cp sparkseejava.jar:. SparkseeImplementation -d graph.gdb -g <ext_id>\n"
                            +
                            "  GetEdgesIdsByLabel: java -cp sparkseejava.jar:. SparkseeImplementation -d graph.gdb -gel <edge_label>\n"
                            +
                            "  FindNodesByAtrributeValue: java -cp sparkseejava.jar:. SparkseeImplementation -d graph.gdb -nv \"<Type>:<attr>=<value>\"\n");
            System.exit(1);
        }

        Map<String, String> params = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("-") && i + 1 < args.length) {
                params.put(args[i], args[i + 1]);
                i++;
            }
        }

        SparkseeImplementation2 app = new SparkseeImplementation2();
        long startTime;
        long endTime;

        

        if (params.containsKey("-n") && params.containsKey("-e") && params.containsKey("-d")) {
            System.out.println("Saving graph...");
            app.runIngest(params.get("-n"), params.get("-e"), params.get("-d"));
            System.out.println("Graph saved.");
            return;
        }
        if (params.containsKey("-d") && params.containsKey("-g")) {
            System.out.println("Querying node...");
            app.runGetNode(params.get("-d"), params.get("-g"));
            System.out.println("Done.");
            return;
        }
        if (params.containsKey("-d") && params.containsKey("-gel")) {
            System.out.println("Querying edge IDs by label...");
            app.runGetEdgeIdsByLabel(params.get("-d"), params.get("-gel"));
            System.out.println("Done.");
            return;
        }
        if (params.containsKey("-d") && params.containsKey("-nv")) {
            System.out.println("Querying nodes by attribute value...");
            app.runFindNodesByAttrAcrossTypes(params.get("-d"), params.get("-nv"));
            System.out.println("Done.");
            return;
        }

        if (params.containsKey("-e") && params.containsKey("-d")) {
            medirTiempos(app, params.get("-e"), params.get("-d"));
            System.out.println("Done.");
            return;
        }

        System.out.println("Invalid arguments.");
    }

        public static void medirTiempos(SparkseeImplementation2 app, String rutaArchivo, String dbPath) {
        List<Long> tiempos = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(rutaArchivo))) {
            String linea;
            int contador = 0;
            while ((linea = br.readLine()) != null) {
                try{

                    long inicio = System.nanoTime(); // tiempo en nanosegundos
                    app.runGetNode(dbPath, linea);
                    long fin = System.nanoTime();
    
                    long duracion = fin - inicio;
                    tiempos.add(duracion);
                    System.out.printf("Línea %d procesada en %.3f ms%n", ++contador, duracion / 1_000_000.0);
                }catch(Exception e){
                    System.out.printf("Línea %d produjo error: %s%n", ++contador, e.getMessage());
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

    private void runIngest(String nodesPath, String edgesPath, String dbPath) throws Exception {
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

    private void runGetNode(String dbPath, String wantedId) throws Exception {
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
            long startTime = System.nanoTime();
            extIdAttr = ensureAttribute(g, Type.NodesType, "ext_id", DataType.String, AttributeKind.Unique);
            getWantedNode(wantedId, sess, g);
            long endTime = System.nanoTime();
            System.out.printf("Query time: %.2f ms%n", (endTime - startTime) / 1_000_000.0);

        } finally {
            if (sess != null)
                sess.close();
            if (db != null)
                db.close();
            sparksee.close();
        }
    }

    private void runGetEdgeIdsByLabel(String dbPath, String edgeLabel) throws Exception {
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
            long startTime = System.nanoTime();
            printEdgeIdsByLabel(sess, g, edgeLabel);
            long endTime = System.nanoTime();
            System.out.printf("Query time: %.2f ms%n", (endTime - startTime) / 1_000_000.0);

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

    private void printEdgeIdsByLabel(Session sess, Graph g, String label) {
        System.out.println("searching edges label='" + label + "' ...");
        sess.begin();
        try {

            long t0 = System.nanoTime();
            int edgeType = g.findType(label);
            if (edgeType == Type.InvalidType) {
                System.out.println("Edge label not found: " + label);
                sess.commit();
                return;
            }
            int eidAttr = g.findAttribute(edgeType, "eid");
            if (eidAttr == Attribute.InvalidAttribute) {
                System.out.println("Edge type '" + label + "' has no 'eid' attribute. Re-ingest required.");
                sess.commit();
                return;
            }

            long total = 0;
            com.sparsity.sparksee.gdb.Objects objs = g.select(edgeType);
            com.sparsity.sparksee.gdb.ObjectsIterator it = objs.iterator();
            try {
                Value v = new Value();
                int count = 0;
                while (it.hasNext()) {
                    long eoid = it.next();
                    g.getAttribute(eoid, eidAttr, v);
                    if (!v.isNull() && count < 10) {
                        System.out.println(v.getString());
                    }
                    total++;
                    count++;
                }
            } finally {
                it.close();
                objs.close();
            }
            long t1 = System.nanoTime();
            System.out.printf("Total edges: %d (%.2f ms)%n", total, (t1 - t0) / 1_000_000.0);
            sess.commit();
        } catch (RuntimeException e) {
            try {
                sess.rollback();
            } catch (Exception ignore) {
            }
            throw e;
        }
    }

    private void printNodeByExtId(Session sess, Graph g, String extId) {
        System.out.println("searching node ext_id='" + extId + "' ...");
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
            sess.commit();
        } catch (RuntimeException e) {
            try {
                sess.rollback();
            } catch (Exception ignore) {
            }
            throw e;
        }
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
            Value v = new Value();
            sess.beginUpdate();

            while ((line = br.readLine()) != null) {
                if (line.isEmpty())
                    continue;
                String[] cols = line.split("\\|", -1);
                if (cols.length != header.length) {
                    System.err.println("Arista con columnas inválidas (saltando): " + line);
                    continue;
                }

                String eid = idx.containsKey("@id") ? cols[idx.get("@id")] : null;
                String label = cols[idx.get("@label")];
                String dir = cols[idx.get("@dir")];
                String outExt = cols[idx.get("@out")];
                String inExt = cols[idx.get("@in")];

                boolean directed = !"F".equalsIgnoreCase(dir);
                int edgeType = ensureEdgeType(g, label, directed);

                Long outOid = oidByExtId.get(outExt);
                Long inOid = oidByExtId.get(inExt);
                if (outOid == null || inOid == null) {
                    System.err.println("No se encontró nodo para arista (saltando): " + line);
                    continue;
                }

                long eoid = g.newEdge(edgeType, outOid, inOid);

                // Asegurar atributo 'eid' y guardarlo
                int eidAttr = ensureAttribute(g, edgeType, "eid", DataType.String, AttributeKind.Basic);
                if (eid == null || eid.isEmpty()) {
                    eid = outExt + "|" + label + "|" + inExt;
                }
                g.setAttribute(eoid, eidAttr, v.setString(eid));

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

    private void runFindNodesByAttrValue(String dbPath, String spec) throws Exception {
        // spec format: "<Type>:<attr>=<value>"
        int colon = spec.indexOf(':');
        int eq = spec.indexOf('=', colon + 1);
        if (colon <= 0 || eq <= colon + 1 || eq == spec.length() - 1) {
            throw new IllegalArgumentException("Invalid -nv format. Use \"<Type>:<attr>=<value>\"");
        }

        String typeName = spec.substring(0, colon).trim();
        String attrName = spec.substring(colon + 1, eq).trim();
        String rawValue = spec.substring(eq + 1).trim();

        SparkseeConfig cfg = new SparkseeConfig("sparksee.cfg");
        Sparksee sparksee = new Sparksee(cfg);
        Database db = null;
        Session sess = null;

        try {
            sess.begin();
            System.out.println("Loading graph from " + dbPath);
            db = sparksee.open(dbPath, true);
            db.disableRollback();
            sess = db.newSession();
            Graph g = sess.getGraph();
            System.out.println("Graph loaded from " + dbPath);


            long t0 = System.nanoTime();
            // check type and attribute
            int typeId = g.findType(typeName);
            if (typeId == Type.InvalidType) {
                System.out.println("Node type not found: " + typeName);
                return;
            }
            int attrId = g.findAttribute(typeId, attrName);
            if (attrId == Attribute.InvalidAttribute) {
                System.out.println("Attribute '" + attrName + "' not found on type '" + typeName + "'");
                return;
            }

            Attribute meta = g.getAttribute(attrId);
            DataType dt = meta.getDataType();

            // Prepare Value with proper type
            Value v = new Value();
            try {
                switch (dt) {
                    case Integer:
                        v.setInteger(Integer.parseInt(rawValue));
                        break;
                    case Long:
                        v.setLong(Long.parseLong(rawValue));
                        break;
                    case Double:
                        v.setDouble(Double.parseDouble(rawValue));
                        break;
                    case Boolean:
                        v.setBoolean(Boolean.parseBoolean(rawValue));
                        break;
                    default:
                        v.setString(rawValue);
                        break;
                }
            } catch (NumberFormatException nfe) {
                v.setString(rawValue);
            }


            // Select all objects whose <attrName> == <value>
            com.sparsity.sparksee.gdb.Objects results = g.select(attrId, Condition.Equal, v);

            long total = 0;
            com.sparsity.sparksee.gdb.ObjectsIterator it = results.iterator();
            try {
                int extAttr = (extIdAttr != Attribute.InvalidAttribute)
                        ? extIdAttr
                        : g.findAttribute(Type.NodesType, "ext_id");
                int limit = 10;
                int count = 0;
                Value out = new Value();
                while (it.hasNext()) {
                    long oid = it.next();
                    System.out.print("OID=" + oid);

                    if (extAttr != Attribute.InvalidAttribute) {
                        g.getAttribute(oid, extAttr, out);
                        if (!out.isNull() && count < limit) {
                            System.out.print("  ext_id=" + out.getString());
                            count++;
                        }
                    }
                    System.out.println();
                    total++;
                }

            long t1 = System.nanoTime();
            System.out.printf("Matched %d nodes (%.2f ms)%n", total, (t1 - t0) / 1_000_000.0);
            } finally {
                it.close();
                results.close();
            }

            sess.commit();

        } catch (RuntimeException e) {
            try {
                if (sess != null)
                    sess.rollback();
            } catch (Exception ignore) {
            }
            throw e;
        } finally {
            if (sess != null)
                sess.close();
            if (db != null)
                db.close();
            sparksee.close();
        }
    }

    private void runFindNodesByAttrAcrossTypes(String dbPath, String spec) throws Exception {
    // spec: "<attr>=<value>" or "<attr>:<value>"
    String attrName, rawValue;
    int sep = spec.indexOf('=');
    if (sep < 0) sep = spec.indexOf(':');
    if (sep <= 0 || sep == spec.length() - 1) {
        throw new IllegalArgumentException("Invalid -nvx. Use \"<attr>=<value>\" (or attr:value)");
    }
    attrName = spec.substring(0, sep).trim();
    rawValue = spec.substring(sep + 1).trim();

    SparkseeConfig cfg = new SparkseeConfig("sparksee.cfg");
    Sparksee sparksee = new Sparksee(cfg);
    Database db = null;
    Session sess = null;

    try {
        db = sparksee.open(dbPath, true);
        db.disableRollback();
        sess = db.newSession();
        Graph g = sess.getGraph();

        if (extIdAttr == Attribute.InvalidAttribute) {
            extIdAttr = g.findAttribute(Type.NodesType, "ext_id"); // optional pretty print
        }

        sess.begin();
        long t0 = System.nanoTime();

        com.sparsity.sparksee.gdb.Objects acc = null;

        // ✅ Iterate only node types
        TypeList tlist = g.findNodeTypes();
        TypeListIterator tIt = tlist.iterator();
        while (tIt.hasNext()) {
            int typeId = tIt.next();

            int attrId = g.findAttribute(typeId, attrName);
            if (attrId == Attribute.InvalidAttribute) continue;

            Attribute meta = g.getAttribute(attrId);
            DataType dt = meta.getDataType();

            Value v = new Value();
            if (!setValueForType(v, dt, rawValue)) continue;

            com.sparsity.sparksee.gdb.Objects part = g.select(attrId, Condition.Equal, v);

            if (acc == null) {
                acc = part; // take ownership
            } else {
                com.sparsity.sparksee.gdb.Objects tmp =
                        com.sparsity.sparksee.gdb.Objects.combineUnion(acc, part);
                acc.close();
                part.close();
                acc = tmp;
            }
        }
        // (no tlist.close())

        long total = 0;
        if (acc != null) {
            com.sparsity.sparksee.gdb.ObjectsIterator it = acc.iterator();
            try {
                Value out = new Value();
                while (it.hasNext()) {
                    long oid = it.next();
                    int tId = g.getObjectType(oid);
                    String tName = g.getType(tId).getName();

                    System.out.print("OID=" + oid + "  type=" + tName);
                    if (extIdAttr != Attribute.InvalidAttribute) {
                        g.getAttribute(oid, extIdAttr, out);
                        if (!out.isNull() && total < 10) System.out.print("  ext_id=" + out.getString());
                    }
                    System.out.println();
                    total++;
                }
            } finally {
                it.close();
                acc.close();
            }
        }

        long t1 = System.nanoTime();
        System.out.printf("Matched %d nodes across all types (%.2f ms)%n",
                total, (t1 - t0) / 1_000_000.0);
        sess.commit();

    } catch (RuntimeException e) {
        try { if (sess != null) sess.rollback(); } catch (Exception ignore) {}
        throw e;
    } finally {
        if (sess != null) sess.close();
        if (db != null) db.close();
        sparksee.close();
    }
}


    private static boolean setValueForType(Value v, DataType dt, String raw) {
        try {
            switch (dt) {
                case Integer:
                    v.setInteger(Integer.parseInt(raw));
                    return true;
                case Long:
                    v.setLong(Long.parseLong(raw));
                    return true;
                case Double:
                    v.setDouble(Double.parseDouble(raw));
                    return true;
                case Boolean:
                    v.setBoolean(Boolean.parseBoolean(raw));
                    return true;
                case Timestamp:
                    v.setLong(Long.parseLong(raw));
                    return true;
                case OID:
                    v.setOID(Long.parseLong(raw));
                    return true;
                case String:
                    v.setString(raw);
                    return true;
                case Text:
                    return false;
                default:
                    v.setString(raw);
                    return true;
            }
        } catch (NumberFormatException nfe) {
            return dt == DataType.String;
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
}
