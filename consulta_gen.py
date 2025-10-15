#!/usr/bin/env python3
import argparse
import os
from pathlib import Path

def main():
    p = argparse.ArgumentParser(description="Genera scripts de consultas para 5 motores a partir de una lista de nodeids.")
    p.add_argument("input_file", help="Archivo de entrada con IDs de nodos, uno por línea")
    p.add_argument("-o", "--outdir", default=".", help="Directorio de salida (por defecto: actual)")
    args = p.parse_args()

    in_path = Path(args.input_file)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # Archivos de salida
    files = {
        "pg": outdir / "consultas_pg.sh",
        "neo4j": outdir / "consultas_neo4j.sh",
        "mapdb": outdir / "consultas_mapdb.sh",
        "rocksdb": outdir / "consultas_rocksdb.sh",
        "sparksee": outdir / "consultas_sparksee.sh",
    }

    # Cabecera bash (opcional pero útil)
    header = "#!/usr/bin/env bash\nset -euo pipefail\n\n"

    # Abrimos todos los archivos y escribimos cabecera
    fps = {k: open(v, "w", encoding="utf-8") for k, v in files.items()}
    try:
        for f in fps.values():
            f.write(header)

        with in_path.open("r", encoding="utf-8") as fin:
            for raw in fin.read().splitlines():
                nodeid = raw.strip()

                if nodeid == "":
                    # Preserva número de líneas: agrega línea en blanco en cada archivo
                    for f in fps.values():
                        f.write("\n")
                    continue

                # Construcción de líneas según tu especificación + redirección de resultados
                line_pg = (
                    f"java -cp postgresql-42.7.8.jar:. PostgresColumns "
                    f"jdbc:postgresql://localhost:5432/gbd fernando fernando -g {nodeid} "
                    f">> resultados_pg\n"
                )

                line_neo4j = (
                    'mvn compile exec:java -Dexec.mainClass="com.neo4j.Main" '
                    f'-Dexec.args="bolt://localhost:7687 neo4j F3rn4nd0 -g {nodeid}" '
                    f">> resultados_neo4j\n"
                )

                line_mapdb = (
                    'mvn compile exec:java -Dexec.mainClass="com.map.Main" '
                    f'-Dexec.args="graph.gbd -g {nodeid}" '
                    f">> resultados_mapdb\n"
                )

                line_rocksdb = (
                    'mvn compile exec:java -Dexec.mainClass="com.rocks.Main" '
                    f'-Dexec.args="graph.gbd -g {nodeid}" '
                    f">> resultados_rocksdb\n"
                )

                line_sparksee = (
                    f"java -cp sparkseejava.jar:. SparkseeImplementation2 -d graph.gdb -g {nodeid} "
                    f">> resultados_sparksee\n"
                )

                fps["pg"].write(line_pg)
                fps["neo4j"].write(line_neo4j)
                fps["mapdb"].write(line_mapdb)
                fps["rocksdb"].write(line_rocksdb)
                fps["sparksee"].write(line_sparksee)

    finally:
        for f in fps.values():
            f.close()

    # Hacer ejecutables
    for path in files.values():
        os.chmod(path, 0o755)

    print("Generados:")
    for k, v in files.items():
        print(f"- {v}")

if __name__ == "__main__":
    main()
