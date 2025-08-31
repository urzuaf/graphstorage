package com.rocks.db;

import org.rocksdb.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Consumer;

public class GraphStore implements AutoCloseable {

    //  CF names 
    public static final String CF_NODES = "cf_nodes";
    public static final String CF_EDGES = "cf_edges";
    public static final String CF_INDEX = "cf_index";

    // separador NUL 
    private static final byte SEP = 0;

    //DB handles 
    private final RocksDB db;
    private final ColumnFamilyHandle cfNodes;
    private final ColumnFamilyHandle cfEdges;
    private final ColumnFamilyHandle cfIndex;

    private GraphStore(RocksDB db, ColumnFamilyHandle n, ColumnFamilyHandle e, ColumnFamilyHandle i) {
        this.db = db; this.cfNodes = n; this.cfEdges = e; this.cfIndex = i;
    }

    public static GraphStore open(Path dbPath) throws RocksDBException, IOException {
        RocksDB.loadLibrary();
        Files.createDirectories(dbPath);

        var tableCfg = new BlockBasedTableConfig()
                .setCacheIndexAndFilterBlocks(true)
                .setEnableIndexCompression(true);

        var cfOpts = new ColumnFamilyOptions()
                .setCompressionType(CompressionType.ZSTD_COMPRESSION)
                .setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION)
                .setTableFormatConfig(tableCfg);

        var cfIndexOpts = new ColumnFamilyOptions()
                .setCompressionType(CompressionType.ZSTD_COMPRESSION)
                .setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION)
                .setTableFormatConfig(tableCfg);

        List<ColumnFamilyDescriptor> cfds = List.of(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()),
                new ColumnFamilyDescriptor(CF_NODES.getBytes(StandardCharsets.UTF_8), cfOpts),
                new ColumnFamilyDescriptor(CF_EDGES.getBytes(StandardCharsets.UTF_8), cfOpts),
                new ColumnFamilyDescriptor(CF_INDEX.getBytes(StandardCharsets.UTF_8), cfIndexOpts)
        );
        List<ColumnFamilyHandle> handles = new ArrayList<>();

        var dbo = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true);

        RocksDB db = RocksDB.open(dbo, dbPath.toString(), cfds, handles);
        return new GraphStore(db, handles.get(1), handles.get(2), handles.get(3));
    }

    @Override public void close() {
        try { cfIndex.close(); } catch (Exception ignore) {}
        try { cfEdges.close(); } catch (Exception ignore) {}
        try { cfNodes.close(); } catch (Exception ignore) {}
        try { db.close(); } catch (Exception ignore) {}
    }

    // Keys 
    private static byte[] keyNode(String nodeId) { return ("node:" + nodeId).getBytes(StandardCharsets.UTF_8); }
    private static byte[] keyEdge(String edgeId) { return ("edge:" + edgeId).getBytes(StandardCharsets.UTF_8); }

    // idx key: "idx" + SEP + part1 + SEP + part2 + ...
    private static byte[] idxKey(String... parts) {
        int len = 3;
        for (String p : parts) len += 1 + p.getBytes(StandardCharsets.UTF_8).length;
        byte[] out = new byte[len];
        int i = 0;
        byte[] idx = "idx".getBytes(StandardCharsets.UTF_8);
        System.arraycopy(idx, 0, out, i, idx.length); i += idx.length;
        for (String p : parts) {
            out[i++] = SEP;
            byte[] b = p.getBytes(StandardCharsets.UTF_8);
            System.arraycopy(b, 0, out, i, b.length);
            i += b.length;
        }
        return out;
    }
    private static byte[] idxPrefix(String... partsWithoutLast) {
        return idxKey(partsWithoutLast);
    }
    private static boolean startsWith(byte[] a, byte[] p) {
        if (a.length < p.length) return false;
        for (int i=0;i<p.length;i++) if (a[i]!=p[i]) return false;
        return true;
    }
    private static String suffixAfterPrefix(byte[] key, byte[] prefix) {
        int start = prefix.length;
        if (start < key.length && key[start] == SEP) start++;
        return new String(key, start, key.length - start, StandardCharsets.UTF_8);
    }
    private static byte[] norm(String s) {
        return s.toLowerCase(Locale.ROOT).getBytes(StandardCharsets.UTF_8);
    }

    // === Blobs ===

    private static byte[] encodeNodeBlob(String label, Map<String,String> props){
        byte[] lb = label.getBytes(StandardCharsets.UTF_8);
        int size = 2 + lb.length + 2;
        for (var e: props.entrySet()){
            size += 2 + e.getKey().getBytes(StandardCharsets.UTF_8).length
                    + 4 + e.getValue().getBytes(StandardCharsets.UTF_8).length;
        }
        ByteBuffer bb = ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN);
        bb.putShort((short)lb.length).put(lb);
        bb.putShort((short)props.size());
        for (var e: props.entrySet()){
            byte[] k = e.getKey().getBytes(StandardCharsets.UTF_8);
            byte[] v = e.getValue().getBytes(StandardCharsets.UTF_8);
            bb.putShort((short)k.length).put(k);
            bb.putInt(v.length).put(v);
        }
        return bb.array();
    }
    public static final class NodeBlob {
        public final String label; public final Map<String,String> props;
        NodeBlob(String label, Map<String,String> props){ this.label=label; this.props=props; }
    }
    private static NodeBlob decodeNodeBlob(byte[] b){
        ByteBuffer bb = ByteBuffer.wrap(b).order(ByteOrder.BIG_ENDIAN);
        int ll = bb.getShort() & 0xFFFF; byte[] lb = new byte[ll]; bb.get(lb);
        String label = new String(lb, StandardCharsets.UTF_8);
        int pc = bb.getShort() & 0xFFFF;
        Map<String,String> props = new LinkedHashMap<>(pc);
        for (int i=0;i<pc;i++){
            int kl = bb.getShort() & 0xFFFF; byte[] kb = new byte[kl]; bb.get(kb);
            String k = new String(kb, StandardCharsets.UTF_8);
            int vl = bb.getInt(); byte[] vb = new byte[vl]; bb.get(vb);
            String v = new String(vb, StandardCharsets.UTF_8);
            props.put(k, v);
        }
        return new NodeBlob(label, props);
    }

    private static byte[] encodeEdgeBlob(String label, String src, String dst){
        byte[] lb = label.getBytes(StandardCharsets.UTF_8);
        byte[] sb = src.getBytes(StandardCharsets.UTF_8);
        byte[] db = dst.getBytes(StandardCharsets.UTF_8);
        ByteBuffer bb = ByteBuffer.allocate(2+lb.length + 2+sb.length + 2+db.length).order(ByteOrder.BIG_ENDIAN);
        bb.putShort((short)lb.length).put(lb);
        bb.putShort((short)sb.length).put(sb);
        bb.putShort((short)db.length).put(db);
        return bb.array();
    }
    public static final class EdgeBlob {
        public final String label, src, dst;
        EdgeBlob(String label, String src, String dst){ this.label=label; this.src=src; this.dst=dst; }
    }
    private static EdgeBlob decodeEdgeBlob(byte[] b){
        ByteBuffer bb = ByteBuffer.wrap(b).order(ByteOrder.BIG_ENDIAN);
        int ll=bb.getShort()&0xFFFF; byte[] lb=new byte[ll]; bb.get(lb);
        int sl=bb.getShort()&0xFFFF; byte[] sb=new byte[sl]; bb.get(sb);
        int dl=bb.getShort()&0xFFFF; byte[] db=new byte[dl]; bb.get(db);
        return new EdgeBlob(new String(lb, StandardCharsets.UTF_8),
                            new String(sb, StandardCharsets.UTF_8),
                            new String(db, StandardCharsets.UTF_8));
    }

    // Gets 
    public NodeBlob getNode(String nodeId) throws RocksDBException {
        byte[] v = db.get(cfNodes, keyNode(nodeId));
        return v==null ? null : decodeNodeBlob(v);
    }
    public EdgeBlob getEdge(String edgeId) throws RocksDBException {
        byte[] v = db.get(cfEdges, keyEdge(edgeId));
        return v==null ? null : decodeEdgeBlob(v);
    }

    // Iteradores 

    /** edgeIds label*/
    public void forEachEdgeIdByLabel(String label, Consumer<String> consumer){
        byte[] prefix = idxPrefix("label","edge",label);
        try (var it = db.newIterator(cfIndex)) {
            it.seek(prefix);
            while (it.isValid() && startsWith(it.key(), prefix)) {
                consumer.accept(suffixAfterPrefix(it.key(), prefix));
                it.next();
            }
        }
    }

    /** nodeIds source */
    public void forEachSourceNodeByLabel(String label, Consumer<String> consumer){
        byte[] prefix = idxPrefix("label","srcnodes",label);
        try (var it = db.newIterator(cfIndex)) {
            it.seek(prefix);
            while (it.isValid() && startsWith(it.key(), prefix)) {
                consumer.accept(suffixAfterPrefix(it.key(), prefix));
                it.next();
            }
        }
    }

    /** nodeIds target  */
    public void forEachDestinationNodeByLabel(String label, Consumer<String> consumer){
        byte[] prefix = idxPrefix("label","dstnodes",label);
        try (var it = db.newIterator(cfIndex)) {
            it.seek(prefix);
            while (it.isValid() && startsWith(it.key(), prefix)) {
                consumer.accept(suffixAfterPrefix(it.key(), prefix));
                it.next();
            }
        }
    }

    /** nodeIds propiedad=valor */
    public void forEachNodeByPropertyEquals(String propName, String propValue, Consumer<String> consumer){
        byte[] prefix = idxPrefix("prop", propName, new String(norm(propValue), StandardCharsets.UTF_8));
        try (var it = db.newIterator(cfIndex)) {
            it.seek(prefix);
            while (it.isValid() && startsWith(it.key(), prefix)) {
                consumer.accept(suffixAfterPrefix(it.key(), prefix));
                it.next();
            }
        }
    }

    // === Ingest 

    /** nodes.pgdf  */
    public void ingestNodes(Path nodesPgdf) throws IOException, RocksDBException {
        try (BufferedReader br = Files.newBufferedReader(nodesPgdf, StandardCharsets.UTF_8);
             WriteOptions wo = new WriteOptions()) {

            String line; String[] header = null;
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

                // guardar nodo
                db.put(cfNodes, keyNode(nodeId), encodeNodeBlob(label, props));

                // índice de propiedad (igualdad exacta)
                for (var e : props.entrySet()){
                    if (e.getValue()==null || e.getValue().isEmpty()) continue;
                    byte[] k = idxKey("prop", e.getKey(), new String(norm(e.getValue()), StandardCharsets.UTF_8), nodeId);
                    db.put(cfIndex, k, new byte[0]);
                }
            }
        }
    }

    /** edges.pgdf: @id|@label|@dir|@out|@in  */
    public void ingestEdges(Path edgesPgdf) throws IOException, RocksDBException {
        try (BufferedReader br = Files.newBufferedReader(edgesPgdf, StandardCharsets.UTF_8);
             WriteOptions wo = new WriteOptions()) {

            String line; String[] header = null;
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

                // guardar arista
                db.put(cfEdges, keyEdge(edgeId), encodeEdgeBlob(label, src, dst));

                // índices por-ítem
                db.put(cfIndex, idxKey("label","edge",     label, edgeId), new byte[0]);
                db.put(cfIndex, idxKey("label","srcnodes", label, src),    new byte[0]);
                db.put(cfIndex, idxKey("label","dstnodes", label, dst),    new byte[0]);
            }
        }
    }

    private static String makeEdgeId(String src, String label, String dst) {
        String s = src + "|" + label + "|" + dst;
        long x = 1125899906842597L; // FNV-like simple
        for (int i=0;i<s.length();i++) x = (x * 1315423911L) ^ s.charAt(i);
        return Long.toUnsignedString(x);
    }
}
