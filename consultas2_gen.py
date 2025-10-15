#!/usr/bin/env python3
import argparse
import os
from pathlib import Path

LABELS = ["Likes", "Knows", "IsLocatedIn", "HasCreator", "ReplyOf"]
PROPS  = [
    "locationIP=61.17.197.42",
    "locationIP=109.200.162.181",
    "locationIP=41.138.47.146",
    "firstName=John",
    "firstName=Carlos",
    "firstName=Rahul",
]

HEADER = "#!/usr/bin/env bash\nset -euo pipefail\n\n"

def make_lines_for_label(motor: str, label: str) -> str:
    if motor == "pg":
        return (f"java -cp postgresql-42.7.8.jar:. PostgresColumns "
                f"jdbc:postgresql://localhost:5432/gbd fernando fernando -gl {label} "
                f">> resultados_pg\n")
    if motor == "neo4j":
        return ('mvn compile exec:java -Dexec.mainClass="com.neo4j.Main" '
                f'-Dexec.args="bolt://localhost:7687 neo4j F3rn4nd0 -gl {label}" '
                f">> resultados_neo4j\n")
    if motor == "mapdb":
        return ('mvn compile exec:java -Dexec.mainClass="com.map.Main" '
                f'-Dexec.args="graph.gbd -gl {label}" '
                f">> resultados_mapdb\n")
    if motor == "rocksdb":
        return ('mvn compile exec:java -Dexec.mainClass="com.rocks.Main" '
                f'-Dexec.args="graph.gbd -gl {label}" '
                f">> resultados_rocksdb\n")
    if motor == "sparksee":
        return (f"java -cp sparkseejava.jar:. SparkseeImplementation2 -d graph.gdb -gl {label} "
                f">> resultados_sparksee\n")
    raise ValueError(motor)

def make_lines_for_prop(motor: str, kv: str) -> str:
    if motor == "pg":
        return (f"java -cp postgresql-42.7.8.jar:. PostgresColumns "
                f"jdbc:postgresql://localhost:5432/gbd fernando fernando -nv {kv} "
                f">> resultados_pg\n")
    if motor == "neo4j":
        return ('mvn compile exec:java -Dexec.mainClass="com.neo4j.Main" '
                f'-Dexec.args="bolt://localhost:7687 neo4j F3rn4nd0 -nv {kv}" '
                f">> resultados_neo4j\n")
    if motor == "mapdb":
        return ('mvn compile exec:java -Dexec.mainClass="com.map.Main" '
                f'-Dexec.args="graph.gbd -nv {kv}" '
                f">> resultados_mapdb\n")
    if motor == "rocksdb":
        return ('mvn compile exec:java -Dexec.mainClass="com.rocks.Main" '
                f'-Dexec.args="graph.gbd -nv {kv}" '
                f">> resultados_rocksdb\n")
    if motor == "sparksee":
        return (f"java -cp sparkseejava.jar:. SparkseeImplementation2 -d graph.gdb -nv {kv} "
                f">> resultados_sparksee\n")
    raise ValueError(motor)

def write_script(path: Path, lines: list[str]):
    path.write_text(HEADER + "".join(lines), encoding="utf-8")
    os.chmod(path, 0o755)

def main():
    ap = argparse.ArgumentParser(
        description="Genera scripts de consultas (-gl labels fijos, -nv props fijas) para 5 motores."
    )
    ap.add_argument("-o", "--outdir", default=".", help="Directorio de salida (default: .)")
    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # Archivos para labels (-gl)
    gl_files = {
        "pg":      outdir / "consultas_pg_labels.sh",
        "neo4j":   outdir / "consultas_neo4j_labels.sh",
        "mapdb":   outdir / "consultas_mapdb_labels.sh",
        "rocksdb": outdir / "consultas_rocksdb_labels.sh",
        "sparksee":outdir / "consultas_sparksee_labels.sh",
    }

    # Archivos para props (-nv)
    nv_files = {
        "pg":      outdir / "consultas_pg_props.sh",
        "neo4j":   outdir / "consultas_neo4j_props.sh",
        "mapdb":   outdir / "consultas_mapdb_props.sh",
        "rocksdb": outdir / "consultas_rocksdb_props.sh",
        "sparksee":outdir / "consultas_sparksee_props.sh",
    }

    # Generar líneas y escribir
    for motor, path in gl_files.items():
        lines = [make_lines_for_label(motor, label) for label in LABELS]
        write_script(path, lines)

    for motor, path in nv_files.items():
        lines = [make_lines_for_prop(motor, kv) for kv in PROPS]
        write_script(path, lines)

    print("Generados scripts:")
    for d in (gl_files, nv_files):
        for motor, p in d.items():
            print(f"- {p}")

if __name__ == "__main__":
    main()
