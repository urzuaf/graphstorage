package cbin.io;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/**
  I/O binario: helpers + consulta de nodo por id.
  Incluye:
   - VarInt LEB128 sin signo
   - writeDictionary (str/lex/ord2pos)
   - stringToId (binary search en lex)
   - nodeIdToOrdinal (binary search en nodes.id.lex)
   - buildLabelIndex / buildNodesByPropIndex (en memoria, dedup + varint+delta)
   - queryNodeById(outDir, nodeId)
 */
public class BinaryGraphFiles {

    public static class IO {
        public OutputStream openOut(Path p) throws IOException {
            Files.createDirectories(p.getParent() == null ? Path.of(".") : p.getParent());
            return new BufferedOutputStream(Files.newOutputStream(p, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE), 1<<20);
        }
    }

    public static class VarInt {
        public static void writeUnsigned(OutputStream out, long v) throws IOException {
            while (true) {
                long b = v & 0x7F;
                v >>>= 7;
                if (v == 0) { out.write((int)b); return; }
                out.write((int)(b | 0x80));
            }
        }
        public static long readUnsigned(FileChannel ch, long[] posRef) throws IOException {
            long pos = posRef[0];
            long result = 0, shift = 0;
            while (true) {
                ByteBuffer bb = ByteBuffer.allocate(1);
                ch.read(bb, pos++);
                int b = bb.array()[0] & 0xFF;
                result |= (long)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) break;
                shift += 7;
            }
            posRef[0] = pos;
            return result;
        }
    }


    public static void writeDictionary(Path strPath, Path lexPath, Path ord2posPath, List<String> values) throws IOException {
        int n = values.size();
        int[] off = new int[n];
        int[] len = new int[n];

        try (var os = new BufferedOutputStream(Files.newOutputStream(strPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE), 1<<20)) {
            int pos = 0;
            for (int i=0;i<n;i++){
                byte[] b = values.get(i).getBytes(StandardCharsets.UTF_8);
                os.write(b);
                off[i]=pos; len[i]=b.length; pos+=b.length;
            }
        }

        // ord2pos
        try (var os = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(ord2posPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE), 1<<20))) {
            for (int i=0;i<n;i++){
                os.writeInt(off[i]);
                os.writeInt(len[i]);
            }
        }

        // lex
        Integer[] ids = new Integer[n];
        for (int i=0;i<n;i++) ids[i]=i;
        Arrays.sort(ids, Comparator.comparing(values::get));
        try (var os = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(lexPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE), 1<<20))) {
            for (int id : ids) {
                os.writeInt(off[id]);
                os.writeInt(len[id]);
                os.writeInt(id);
            }
        }
    }

    public static int stringToId(String s, Path lex, Path str) throws IOException {
        byte[] target = s.getBytes(StandardCharsets.UTF_8);
        long size = Files.size(lex);
        long n = size / 12; // u32,u32,u32
        try (var chLex = FileChannel.open(lex, StandardOpenOption.READ);
             var chStr = FileChannel.open(str, StandardOpenOption.READ)) {
            long lo=0, hi=n-1;
            while (lo <= hi) {
                long mid = (lo + hi) >>> 1;
                ByteBuffer bb = ByteBuffer.allocate(12);
                chLex.read(bb, mid*12);
                bb.flip();
                int off = bb.getInt();
                int len = bb.getInt();
                int id  = bb.getInt();

                byte[] cur = new byte[len];
                chStr.read(ByteBuffer.wrap(cur), Integer.toUnsignedLong(off));
                int cmp = compareUtf8(cur, target);
                if (cmp == 0) return id;
                if (cmp < 0) lo = mid + 1; else hi = mid - 1;
            }
        }
        return -1;
    }

    private static int compareUtf8(byte[] a, byte[] b) {
        int la=a.length, lb=b.length, l=Math.min(la, lb);
        for (int i=0;i<l;i++){
            int ca = a[i] & 0xFF;
            int cb = b[i] & 0xFF;
            if (ca != cb) return Integer.compare(ca, cb);
        }
        return Integer.compare(la, lb);
    }

    public static int nodeIdToOrdinal(Path nodesIdLex, Path nodesIdStr, String nodeId) throws IOException {
        byte[] target = nodeId.getBytes(StandardCharsets.UTF_8);
        long size = Files.size(nodesIdLex);
        long n = size / 12;
        try (var chLex = FileChannel.open(nodesIdLex, StandardOpenOption.READ);
             var chStr = FileChannel.open(nodesIdStr, StandardOpenOption.READ)) {
            long lo=0, hi=n-1;
            while (lo <= hi) {
                long mid = (lo + hi) >>> 1;
                ByteBuffer bb = ByteBuffer.allocate(12);
                chLex.read(bb, mid*12);
                bb.flip();
                int off = bb.getInt();
                int len = bb.getInt();
                int ord = bb.getInt();
                byte[] cur = new byte[len];
                chStr.read(ByteBuffer.wrap(cur), Integer.toUnsignedLong(off));
                int cmp = compareUtf8(cur, target);
                if (cmp == 0) return ord;
                if (cmp < 0) lo = mid + 1; else hi = mid - 1;
            }
        }
        return -1;
    }

    public static String makeEdgeId(String src, String label, String dst) {
        String s = src + "|" + label + "|" + dst;
        long x = 1125899906842597L;
        for (int i=0;i<s.length();i++) x = (x * 1315423911L) ^ s.charAt(i);
        return Long.toUnsignedString(x);
    }

    public static void buildLabelIndex(Path tmpPairs, Path outPl, Path outDir) throws IOException {
        if (!Files.exists(tmpPairs)) {
            try (var os = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(outDir)))) {} // vacío
            try (var os = new BufferedOutputStream(Files.newOutputStream(outPl))) {}
            return;
        }
        Map<Integer, List<Integer>> map = new HashMap<>();
        try (var is = new DataInputStream(new BufferedInputStream(Files.newInputStream(tmpPairs), 1<<20))) {
            while (true) {
                try {
                    int label = is.readInt();
                    int val   = is.readInt();
                    map.computeIfAbsent(label, k -> new ArrayList<>()).add(val);
                } catch (EOFException eof) { break; }
            }
        }
        // escribir
        long offset = 0L;
        try (var pl = new BufferedOutputStream(Files.newOutputStream(outPl, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE), 1<<20);
             var dir = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(outDir, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE), 1<<20))) {
            var labels = new ArrayList<>(map.keySet());
            labels.sort(Integer::compareTo);
            for (int label : labels) {
                List<Integer> lst = map.get(label);
                lst.sort(Integer::compareTo);
                // unique
                int w=1;
                for (int i=1;i<lst.size();i++){
                    if (!lst.get(i).equals(lst.get(w-1))) {
                        if (w!=i) lst.set(w, lst.get(i));
                        w++;
                    }
                }
                if (w < lst.size()) lst.subList(w, lst.size()).clear();

                long before = offset;
                // posting list varint+delta
                if (!lst.isEmpty()) {
                    int prev = lst.get(0);
                    VarInt.writeUnsigned(pl, Integer.toUnsignedLong(prev));
                    int bytes = varintSize(prev);
                    for (int i=1;i<lst.size();i++){
                        int d = lst.get(i) - prev;
                        VarInt.writeUnsigned(pl, Integer.toUnsignedLong(d));
                        bytes += varintSize(d);
                        prev = lst.get(i);
                    }
                    offset += bytes;
                }
                // dir
                dir.writeInt(label);
                dir.writeLong(Long.reverseBytes(before));
                dir.writeInt(lst.size());
            }
        }
    }

    private static int varintSize(int v) {
        int n=1;
        while ((v >>>= 7) != 0) n++;
        return n;
    }

    // ============ Índice nodesByProp ============
    // tmpTriples: archivo con triples (propNameId:u32, propValId:u32, nodeOrd:u32)
    public static void buildNodesByPropIndex(Path tmpTriples, Path outPl, Path outDir) throws IOException {
        if (!Files.exists(tmpTriples)) {
            try (var os = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(outDir)))) {}
            try (var os = new BufferedOutputStream(Files.newOutputStream(outPl))) {}
            return;
        }
        // Cargar en memoria y agrupar (para datasets gigantes, cambiar por sort/merge externo)
        Map<Long, List<Integer>> map = new HashMap<>();
        try (var is = new DataInputStream(new BufferedInputStream(Files.newInputStream(tmpTriples), 1<<20))) {
            while (true) {
                try {
                    int pn = is.readInt();
                    int pv = is.readInt();
                    int ord = is.readInt();
                    long key = ((long)pn << 32) | (pv & 0xFFFFFFFFL);
                    map.computeIfAbsent(key, k -> new ArrayList<>()).add(ord);
                } catch (EOFException eof) { break; }
            }
        }
        var keys = new ArrayList<>(map.keySet());
        keys.sort(Comparator.comparingLong(Long::longValue));

        long offset = 0L;
        try (var pl = new BufferedOutputStream(Files.newOutputStream(outPl, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE), 1<<20);
             var dir = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(outDir, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE), 1<<20))) {
            for (long key : keys) {
                List<Integer> lst = map.get(key);
                lst.sort(Integer::compareTo);
                // unique
                int w=1;
                for (int i=1;i<lst.size();i++){
                    if (!lst.get(i).equals(lst.get(w-1))) {
                        if (w!=i) lst.set(w, lst.get(i));
                        w++;
                    }
                }
                if (w < lst.size()) lst.subList(w, lst.size()).clear();

                long before = offset;
                if (!lst.isEmpty()) {
                    int prev = lst.get(0);
                    VarInt.writeUnsigned(pl, Integer.toUnsignedLong(prev));
                    int bytes = varintSize(prev);
                    for (int i=1;i<lst.size();i++){
                        int d = lst.get(i) - prev;
                        VarInt.writeUnsigned(pl, Integer.toUnsignedLong(d));
                        bytes += varintSize(d);
                        prev = lst.get(i);
                    }
                    offset += bytes;
                }
                int pn = (int)((key>>>32) & 0xFFFFFFFFL);
                int pv = (int)( key       & 0xFFFFFFFFL);
                dir.writeInt(pn);
                dir.writeInt(pv);
                dir.writeLong(Long.reverseBytes(before));
                dir.writeInt(lst.size());
            }
        }
    }

    // Consulta: obtener un nodo por id 
    public static final class NodeView {
        public final String label;
        public final Map<String,String> props;
        public NodeView(String label, Map<String,String> props) { this.label=label; this.props=props; }
    }

    public static NodeView queryNodeById(Path outDir, String nodeId) throws IOException {
        Path nodesIdLex = outDir.resolve("nodes.id.lex");
        Path nodesIdStr = outDir.resolve("nodes.id.str");
        Path nodesOff   = outDir.resolve("nodes.off");
        Path nodesRec   = outDir.resolve("nodes.rec");

        int ord = nodeIdToOrdinal(nodesIdLex, nodesIdStr, nodeId);
        if (ord < 0) return null;

        long off;
        try (var chOff = FileChannel.open(nodesOff, StandardOpenOption.READ)) {
            ByteBuffer bb = ByteBuffer.allocate(8);
            chOff.read(bb, (long)ord * 8);
            bb.flip();
            long le = bb.getLong();
            off = Long.reverseBytes(le); // stored LE
        }

        // Decodificar NodeRecord
        int labelId;
        int propCount;
        List<Integer> pn = new ArrayList<>();
        List<Integer> pv = new ArrayList<>();

        try (var chRec = FileChannel.open(nodesRec, StandardOpenOption.READ)) {
            long[] posRef = new long[]{off};
            labelId = (int)VarInt.readUnsigned(chRec, posRef);
            propCount = (int)VarInt.readUnsigned(chRec, posRef);
            for (int i=0;i<propCount;i++){
                pn.add((int)VarInt.readUnsigned(chRec, posRef));
                pv.add((int)VarInt.readUnsigned(chRec, posRef));
            }
        }

        // Resolver strings usando dict.*.ord2pos (id -> off,len)  (OOM-safe)
        String label = idToString(outDir.resolve("dict.labels.ord2pos"), outDir.resolve("dict.labels.str"), labelId);
        Map<String,String> props = new LinkedHashMap<>();
        for (int i=0;i<propCount;i++){
            String name = idToString(outDir.resolve("dict.propname.ord2pos"), outDir.resolve("dict.propname.str"), pn.get(i));
            String val  = idToString(outDir.resolve("dict.propval.ord2pos"), outDir.resolve("dict.propval.str"), pv.get(i));
            props.put(name, val);
        }
        return new NodeView(label, props);
    }

    private static String idToString(Path ord2pos, Path str, int id) throws IOException {
        try (var ch = FileChannel.open(ord2pos, StandardOpenOption.READ);
             var chStr = FileChannel.open(str, StandardOpenOption.READ)) {
            ByteBuffer bb = ByteBuffer.allocate(8);
            ch.read(bb, (long)id * 8);
            bb.flip();
            int off = bb.getInt();
            int len = bb.getInt();
            byte[] b = new byte[len];
            chStr.read(ByteBuffer.wrap(b), Integer.toUnsignedLong(off));
            return new String(b, StandardCharsets.UTF_8);
        }
    }
}
