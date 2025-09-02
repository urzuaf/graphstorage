package cbin.core;

import cbin.io.BinaryGraphFiles;
import cbin.io.PgdfReader;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

public class BinaryTransformer {

    private final Path outDir;
    private final BinaryGraphFiles.IO io = new BinaryGraphFiles.IO();

    // tmp() NO depende de outDir, así que se puede crear aquí sin problema
    private final Path propValTmp = tmp("propvals.tmp");            // strings 1 por línea UTF-8
    private final Path nodesPropsTmp = tmp("nodes.props.tmp");      // props temporales por nodo
    private final Path tmpEdgesByLabel = tmp("idx.edgesByLabel.tmp");
    private final Path tmpSrcByLabel   = tmp("idx.srcByLabel.tmp");
    private final Path tmpDstByLabel   = tmp("idx.dstByLabel.tmp");
    private final Path tmpNodesByProp  = tmp("idx.nodesByProp.tmp");

    // Estos SÍ dependen de outDir -> se asignan en el constructor
    private Path nodesIdStr;
    private Path nodesIdOrd2Pos;
    private Path nodesIdLex;
    private Path nodesRec;
    private Path nodesOff;

    private Path edgesIdStr;
    private Path edgesIdOrd2Pos;
    private Path edgesIdLex;
    private Path edgesRec;
    private Path edgesOff;

    private Path idxEdgesByLabelPl;
    private Path idxEdgesByLabelDir;
    private Path idxSrcByLabelPl;
    private Path idxSrcByLabelDir;
    private Path idxDstByLabelPl;
    private Path idxDstByLabelDir;
    private Path idxNodesByPropPl;
    private Path idxNodesByPropDir;

    private Path dictLabelsStr;
    private Path dictLabelsLex;
    private Path dictLabelsOrd2Pos;
    private Path dictPropNameStr;
    private Path dictPropNameLex;
    private Path dictPropNameOrd2Pos;
    private Path dictPropValStr;
    private Path dictPropValLex;
    private Path dictPropValOrd2Pos;

    // Estado en memoria acotado
    private final Set<String> labelSet = new HashSet<>();
    private final Set<String> propNameSet = new HashSet<>();
    private Map<String,Integer> labelId;
    private Map<String,Integer> propNameId;

    private int nodeCount = 0;
    private final List<Integer> nodeIdOff = new ArrayList<>();
    private final List<Integer> nodeIdLen = new ArrayList<>();
    private List<String> nodeIdsForLex;  // se libera tras build

    private int edgeCount = 0;
    private final List<Integer> edgeIdOff = new ArrayList<>();
    private final List<Integer> edgeIdLen = new ArrayList<>();

    public BinaryTransformer(Path outDir) {
        this.outDir = outDir;
        try {
            Files.createDirectories(outDir);
        } catch (IOException e) {
            throw new RuntimeException("No se pudo crear el directorio de salida: " + outDir, e);
        }

        // A PARTIR DE AQUÍ ya podemos usar outFile(...)
        this.nodesIdStr      = outFile("nodes.id.str");
        this.nodesIdOrd2Pos  = outFile("nodes.id.ord2pos");
        this.nodesIdLex      = outFile("nodes.id.lex");
        this.nodesRec        = outFile("nodes.rec");
        this.nodesOff        = outFile("nodes.off");

        this.edgesIdStr      = outFile("edges.id.str");
        this.edgesIdOrd2Pos  = outFile("edges.id.ord2pos");
        this.edgesIdLex      = outFile("edges.id.lex");
        this.edgesRec        = outFile("edges.rec");
        this.edgesOff        = outFile("edges.off");

        this.idxEdgesByLabelPl  = outFile("idx.edgesByLabel.pl");
        this.idxEdgesByLabelDir = outFile("idx.edgesByLabel.dir");
        this.idxSrcByLabelPl    = outFile("idx.srcByLabel.pl");
        this.idxSrcByLabelDir   = outFile("idx.srcByLabel.dir");
        this.idxDstByLabelPl    = outFile("idx.dstByLabel.pl");
        this.idxDstByLabelDir   = outFile("idx.dstByLabel.dir");
        this.idxNodesByPropPl   = outFile("idx.nodesByProp.pl");
        this.idxNodesByPropDir  = outFile("idx.nodesByProp.dir");

        this.dictLabelsStr      = outFile("dict.labels.str");
        this.dictLabelsLex      = outFile("dict.labels.lex");
        this.dictLabelsOrd2Pos  = outFile("dict.labels.ord2pos");
        this.dictPropNameStr    = outFile("dict.propname.str");
        this.dictPropNameLex    = outFile("dict.propname.lex");
        this.dictPropNameOrd2Pos= outFile("dict.propname.ord2pos");
        this.dictPropValStr     = outFile("dict.propval.str");
        this.dictPropValLex     = outFile("dict.propval.lex");
        this.dictPropValOrd2Pos = outFile("dict.propval.ord2pos");
    }

    private Path outFile(String name){ return outDir.resolve(name); }
    private static Path tmp(String name){
        try { return Files.createTempFile("graphbin_", "_"+name); }
        catch(IOException e){ throw new RuntimeException(e); }
    }

    // ==========================
    // A partir de aquí tu lógica original (sin cambios),
    // usando los Paths ya inicializados en el constructor.
    // ==========================

    public void acceptNodeRow(PgdfReader.NodeRow row) {
        try {
            byte[] idb = row.nodeId.getBytes(StandardCharsets.UTF_8);
            try (var os = Files.newOutputStream(nodesIdStr, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
                os.write(idb);
            }
            nodeIdOff.add(nodeCount == 0 ? 0 : (nodeIdOff.get(nodeCount - 1) + nodeIdLen.get(nodeCount - 1)));
            nodeIdLen.add(idb.length);

            try (var os = new DataOutputStream(new BufferedOutputStream(
                    Files.newOutputStream(nodesPropsTmp, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND), 1 << 20))) {
                os.writeInt(nodeCount); // ordinal
                writeStr(os, row.label);
                labelSet.add(row.label);

                os.writeInt(row.props.size());
                for (var e : row.props.entrySet()) {
                    String name = e.getKey();
                    String val = e.getValue() == null ? "" : e.getValue();
                    String valLower = val.toLowerCase(Locale.ROOT);

                    writeStr(os, name);
                    writeStr(os, valLower);

                    propNameSet.add(name);
                    Files.writeString(propValTmp, valLower + "\n",
                            StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
                }
            }
            nodeCount++;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    private static void writeStr(DataOutputStream dos, String s) throws IOException {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        dos.writeInt(b.length);
        dos.write(b);
    }
    private static String readStr(DataInputStream dis) throws IOException {
        int len = dis.readInt();
        byte[] b = dis.readNBytes(len);
        return new String(b, StandardCharsets.UTF_8);
    }

    public void finishNodesPass1() throws IOException {
        try (var os = new DataOutputStream(new BufferedOutputStream(
                Files.newOutputStream(nodesIdOrd2Pos, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING), 1<<20))) {
            for (int i = 0; i < nodeCount; i++) {
                os.writeInt(nodeIdOff.get(i));
                os.writeInt(nodeIdLen.get(i));
            }
        }
    }

    public void buildNodesIdLex() throws IOException {
        nodeIdsForLex = new ArrayList<>(nodeCount);
        try (var is = new DataInputStream(new BufferedInputStream(Files.newInputStream(nodesIdOrd2Pos)))) {
            byte[] buf = Files.readAllBytes(nodesIdStr);
            for (int i = 0; i < nodeCount; i++) {
                int off = is.readInt();
                int len = is.readInt();
                String id = new String(buf, off, len, StandardCharsets.UTF_8);
                nodeIdsForLex.add(id);
            }
        }
        Integer[] ords = new Integer[nodeCount];
        for (int i=0;i<nodeCount;i++) ords[i]=i;
        Arrays.sort(ords, Comparator.comparing(i -> nodeIdsForLex.get(i), String::compareTo));
        try (var os = new DataOutputStream(new BufferedOutputStream(
                Files.newOutputStream(nodesIdLex, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING), 1<<20))) {
            for (int ord : ords) {
                os.writeInt(nodeIdOff.get(ord));
                os.writeInt(nodeIdLen.get(ord));
                os.writeInt(ord);
            }
        }
        nodeIdsForLex = null;
    }

    public void collectEdgeLabelOnly(PgdfReader.EdgeRow row) {
        if (row.label != null && !row.label.isEmpty()) labelSet.add(row.label);
    }

    public void buildDictionaries() throws IOException {
        var labels = new ArrayList<>(labelSet);
        labels.sort(String::compareTo);
        labelId = new LinkedHashMap<>();
        for (int i=0;i<labels.size();i++) labelId.put(labels.get(i), i);
        BinaryGraphFiles.writeDictionary(dictLabelsStr, dictLabelsLex, dictLabelsOrd2Pos, labels);

        var pnames = new ArrayList<>(propNameSet);
        pnames.sort(String::compareTo);
        propNameId = new LinkedHashMap<>();
        for (int i=0;i<pnames.size();i++) propNameId.put(pnames.get(i), i);
        BinaryGraphFiles.writeDictionary(dictPropNameStr, dictPropNameLex, dictPropNameOrd2Pos, pnames);

        List<String> vals = Files.readAllLines(propValTmp, StandardCharsets.UTF_8);
        vals.sort(String::compareTo);
        int w = 0;
        for (int i=0;i<vals.size();i++){
            if (w==0 || !vals.get(i).equals(vals.get(w-1))) {
                if (w!=i) vals.set(w, vals.get(i));
                w++;
            }
        }
        if (w < vals.size()) vals.subList(w, vals.size()).clear();
        BinaryGraphFiles.writeDictionary(dictPropValStr, dictPropValLex, dictPropValOrd2Pos, vals);
        Files.deleteIfExists(propValTmp);
    }

    public void materializeNodesRec() throws IOException {
        try (var dis = new DataInputStream(new BufferedInputStream(Files.newInputStream(nodesPropsTmp), 1<<20));
             var rec = new BufferedOutputStream(Files.newOutputStream(nodesRec, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE), 1<<20);
             var off = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(nodesOff, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE), 1<<20))) {

            long pos = 0L;
            for (int i=0;i<nodeCount;i++){
                int ordinal = dis.readInt();
                String labelS = readStr(dis);
                int pcount = dis.readInt();

                int label = BinaryGraphFiles.stringToId(labelS, dictLabelsLex, dictLabelsStr);
                List<Integer> pn = new ArrayList<>(pcount);
                List<Integer> pv = new ArrayList<>(pcount);
                for (int k=0;k<pcount;k++){
                    String name = readStr(dis);
                    String val  = readStr(dis); // lower
                    Integer pnid = propNameId.get(name);
                    if (pnid == null) pnid = BinaryGraphFiles.stringToId(name, dictPropNameLex, dictPropNameStr);
                    int pvid = BinaryGraphFiles.stringToId(val, dictPropValLex, dictPropValStr);
                    pn.add(pnid);
                    pv.add(pvid);
                }

                ByteArrayOutputStream buf = new ByteArrayOutputStream(64);
                BinaryGraphFiles.VarInt.writeUnsigned(buf, label);
                BinaryGraphFiles.VarInt.writeUnsigned(buf, pcount);
                for (int t=0;t<pcount;t++){
                    BinaryGraphFiles.VarInt.writeUnsigned(buf, pn.get(t));
                    BinaryGraphFiles.VarInt.writeUnsigned(buf, pv.get(t));
                }
                byte[] b = buf.toByteArray();
                rec.write(b);

                off.writeLong(Long.reverseBytes(pos)); // LE u64
                pos += b.length;
            }
        }
        Files.deleteIfExists(nodesPropsTmp);
    }

    public void acceptEdgeRow(PgdfReader.EdgeRow row) {
        try {
            if (row.label == null || row.label.isEmpty()) return;
            if (!"T".equalsIgnoreCase(row.dir == null ? "T" : row.dir)) return;
            if (row.outId == null || row.outId.isEmpty()) return;
            if (row.inId == null || row.inId.isEmpty()) return;

            String edgeId = row.edgeId;
            if (edgeId == null || edgeId.isEmpty()) {
                edgeId = BinaryGraphFiles.makeEdgeId(row.outId, row.label, row.inId);
            }

            byte[] idb = edgeId.getBytes(StandardCharsets.UTF_8);
            try (var os = Files.newOutputStream(edgesIdStr, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
                os.write(idb);
            }
            edgeIdOff.add(edgeCount == 0 ? 0 : (edgeIdOff.get(edgeCount - 1) + edgeIdLen.get(edgeCount - 1)));
            edgeIdLen.add(idb.length);

            int label = BinaryGraphFiles.stringToId(row.label, dictLabelsLex, dictLabelsStr);
            int srcOrd = BinaryGraphFiles.nodeIdToOrdinal(nodesIdLex, nodesIdStr, row.outId);
            int dstOrd = BinaryGraphFiles.nodeIdToOrdinal(nodesIdLex, nodesIdStr, row.inId);

            ByteArrayOutputStream buf = new ByteArrayOutputStream(32);
            BinaryGraphFiles.VarInt.writeUnsigned(buf, label);
            BinaryGraphFiles.VarInt.writeUnsigned(buf, srcOrd);
            BinaryGraphFiles.VarInt.writeUnsigned(buf, dstOrd);
            byte[] recBytes = buf.toByteArray();
            try (var osr = Files.newOutputStream(edgesRec, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
                osr.write(recBytes);
            }
            try (var oso = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(edgesOff, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND), 1<<16))) {
                long curSize = Files.size(edgesRec) - recBytes.length;
                oso.writeLong(Long.reverseBytes(curSize));
            }

            try (var os = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(tmpEdgesByLabel, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND), 1<<16))) {
                os.writeInt(label);
                os.writeInt(edgeCount);
            }
            try (var os = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(tmpSrcByLabel, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND), 1<<16))) {
                os.writeInt(label);
                os.writeInt(srcOrd);
            }
            try (var os = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(tmpDstByLabel, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND), 1<<16))) {
                os.writeInt(label);
                os.writeInt(dstOrd);
            }

            edgeCount++;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public void finishEdges() throws IOException {
        try (var os = new DataOutputStream(new BufferedOutputStream(
                Files.newOutputStream(edgesIdOrd2Pos, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE), 1<<20))) {
            for (int i=0;i<edgeCount;i++){
                os.writeInt(edgeIdOff.get(i));
                os.writeInt(edgeIdLen.get(i));
            }
        }
        List<String> eids = new ArrayList<>(edgeCount);
        byte[] all = Files.readAllBytes(edgesIdStr);
        try (var is = new DataInputStream(new BufferedInputStream(Files.newInputStream(edgesIdOrd2Pos)))) {
            for (int i=0;i<edgeCount;i++){
                int off = is.readInt(), len = is.readInt();
                eids.add(new String(all, off, len, StandardCharsets.UTF_8));
            }
        }
        Integer[] ords = new Integer[edgeCount];
        for (int i=0;i<edgeCount;i++) ords[i]=i;
        Arrays.sort(ords, Comparator.comparing(eids::get));
        try (var os = new DataOutputStream(new BufferedOutputStream(
                Files.newOutputStream(edgesIdLex, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE), 1<<20))) {
            for (int ord : ords) {
                os.writeInt(edgeIdOff.get(ord));
                os.writeInt(edgeIdLen.get(ord));
                os.writeInt(ord);
            }
        }
    }

    public void buildIndexes() throws IOException {
        BinaryGraphFiles.buildLabelIndex(tmpEdgesByLabel, idxEdgesByLabelPl, idxEdgesByLabelDir);
        BinaryGraphFiles.buildLabelIndex(tmpSrcByLabel,   idxSrcByLabelPl,   idxSrcByLabelDir);
        BinaryGraphFiles.buildLabelIndex(tmpDstByLabel,   idxDstByLabelPl,   idxDstByLabelDir);
        Files.deleteIfExists(tmpEdgesByLabel);
        Files.deleteIfExists(tmpSrcByLabel);
        Files.deleteIfExists(tmpDstByLabel);

        BinaryGraphFiles.buildNodesByPropIndex(tmpNodesByProp, idxNodesByPropPl, idxNodesByPropDir);
        Files.deleteIfExists(tmpNodesByProp);
    }
}
