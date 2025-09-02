package cbin.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class PgdfReader {

    public static final class NodeRow {
        public final String nodeId;
        public final String label;
        public final Map<String,String> props;
        public NodeRow(String nodeId, String label, Map<String,String> props) {
            this.nodeId = nodeId; this.label = label; this.props = props;
        }
    }

    public static final class EdgeRow {
        public final String edgeId;
        public final String label;
        public final String dir;
        public final String outId;
        public final String inId;
        public EdgeRow(String edgeId, String label, String dir, String outId, String inId) {
            this.edgeId=edgeId; this.label=label; this.dir=dir; this.outId=outId; this.inId=inId;
        }
    }

    // ===== Stream nodes =====
    public static void readNodes(Path nodesPgdf, java.util.function.Consumer<NodeRow> onRow) throws IOException {
        try (BufferedReader br = Files.newBufferedReader(nodesPgdf, StandardCharsets.UTF_8)) {
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
                String labelS = row.getOrDefault("@label","").trim();
                if (nodeId.isEmpty() || labelS.isEmpty()) continue;

                Map<String,String> props = new LinkedHashMap<>();
                for (var e : row.entrySet()){
                    String k=e.getKey();
                    if ("@id".equals(k) || "@label".equals(k)) continue;
                    props.put(k, e.getValue()==null? "" : e.getValue());
                }
                onRow.accept(new NodeRow(nodeId, labelS, props));
            }
        }
    }

    // ===== Stream edges =====
    public static void readEdges(Path edgesPgdf, java.util.function.Consumer<EdgeRow> onRow) throws IOException {
        try (BufferedReader br = Files.newBufferedReader(edgesPgdf, StandardCharsets.UTF_8)) {
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
                String labelS = row.getOrDefault("@label","").trim();
                String dir    = row.getOrDefault("@dir","T").trim();
                String srcId  = row.getOrDefault("@out","").trim();
                String dstId  = row.getOrDefault("@in","").trim();

                if (labelS.isEmpty() || srcId.isEmpty() || dstId.isEmpty()) continue;

                onRow.accept(new EdgeRow(edgeId, labelS, dir, srcId, dstId));
            }
        }
    }
}
