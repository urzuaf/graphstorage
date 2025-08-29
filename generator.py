from __future__ import annotations
import random
import sys
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

# =========================
# CONFIG
# =========================
SEED = 1337
NUM_NODOS = 100000
NUM_ARISTAS = 450000

OUT_NODES = Path("Nodes.pgdf")
OUT_EDGES = Path("Edegs.pgdf")

# Ratio of persons vs orgs
PERSON_RATIO = 0.85
ORG_RATIO = 1.0 - PERSON_RATIO

BASE_URL = "https://example.org/node/"
BASE_SITE = "https://example.org/"

# Person & Organization schema variants.
# IMPORTANT: Each tuple is the header (columns). Must include '@id' and '@label'.
PERSON_SCHEMAS: List[Tuple[str, ...]] = [
    ("@id", "@label", "name", "age", "city", "email", "url"),
    ("@id", "@label", "name", "birth_year", "city", "profession", "url"),
    ("@id", "@label", "name", "age", "city", "phone", "url"),
]

ORG_SCHEMAS: List[Tuple[str, ...]] = [
    ("@id", "@label", "name", "type", "city", "website", "founded_year", "url"),
    ("@id", "@label", "name", "industry", "hq_city", "website", "url"),
]

# Proportions for how we split nodes across schema variants (normalized internally)
PERSON_SCHEMA_WEIGHTS = [0.5, 0.3, 0.2]
ORG_SCHEMA_WEIGHTS = [0.7, 0.3]

# Names & data pools (kept small on purpose; sampled repeatedly)
FIRST_NAMES = [
    "Juan", "Alexis", "María", "Lucía", "Bart", "Lisa", "Maggie", "Homer",
    "Milhouse", "Apu", "Lenny", "Barney", "Moe", "Sofía", "Carlos", "Elena",
]
LAST_NAMES = [
    "García", "Smith", "Pérez", "González", "Lopez", "Martínez", "Rodríguez",
    "Fernández", "Santos", "Romero", "Vega", "Núñez", "Silva", "Rojas",
]
CITIES = ["Springfield", "Shelbyville", "Denver", "Omaha", "Las Vegas", "Lima", "Santiago", "Cochabamba"]
PROFESSIONS = ["Engineer", "Teacher", "Designer", "Developer", "Nurse", "Chef", "Sales", "Analyst"]
ORG_TYPES = ["food", "retail", "tech", "education", "healthcare", "logistics"]
INDUSTRIES = ["Food & Beverage", "Retail", "Software", "Education", "Healthcare", "Transportation"]

# =========================
# UTILITIES
# =========================

def choice_rand(rng: random.Random, arr: Sequence[str]) -> str:
    return arr[rng.randrange(len(arr))]

def make_person_name(rng: random.Random) -> str:
    return f"{choice_rand(rng, FIRST_NAMES)} {choice_rand(rng, LAST_NAMES)}"

def make_org_name(rng: random.Random, idx: int) -> str:
    # Stable readable names
    base = choice_rand(rng, ["Global", "Prime", "Nova", "Andes", "Pioneer", "Vertex", "Atlas", "Nimbus"])
    tail = choice_rand(rng, ["Labs", "Foods", "Retail", "Systems", "Group", "Logistics", "Health", "Academy"])
    return f"{base} {tail} {idx}"

def make_email(name: str) -> str:
    user = name.lower().replace(" ", ".")
    domain = choice_rand(random, ["mail.com", "example.org", "inbox.net"])
    return f"{user}@{domain}"

def make_phone(rng: random.Random) -> str:
    return f"+{rng.randrange(1, 90)}-{rng.randrange(100, 999)}-{rng.randrange(100000, 999999)}"

def make_website(rng: random.Random, slug: str) -> str:
    host = slug.lower().replace(" ", "")
    tld = choice_rand(rng, [".com", ".org", ".net"])
    return f"https://{host}{tld}"

def make_url(node_id: str) -> str:
    return f"{BASE_URL}{node_id}"

def split_counts(total: int, weights: Sequence[float]) -> List[int]:
    """Split 'total' approximately according to 'weights' (which need not sum to 1)."""
    wsum = float(sum(weights))
    raw = [total * (w / wsum) for w in weights]
    ints = [int(x) for x in raw]
    # fix rounding
    while sum(ints) < total:
        # add 1 to the bucket with the largest fractional remainder
        idx = max(range(len(raw)), key=lambda i: raw[i] - ints[i])
        ints[idx] += 1
    return ints

# =========================
# MAIN GENERATION
# =========================

def main() -> None:
    rng = random.Random(SEED)

    # Determine counts
    num_persons = int(NUM_NODOS * PERSON_RATIO)
    num_orgs = NUM_NODOS - num_persons

    person_schema_counts = split_counts(num_persons, PERSON_SCHEMA_WEIGHTS)
    org_schema_counts = split_counts(num_orgs, ORG_SCHEMA_WEIGHTS)

    # We'll store only IDs (strings) for edge generation, to keep memory reasonable
    person_ids: List[str] = []
    org_ids: List[str] = []

    # Generate nodes (by blocks to keep headers tidy and file easy to parse)
    with OUT_NODES.open("w", encoding="utf-8") as f_nodes:
        node_counter = 0

        # --- PERSON blocks
        for schema_idx, schema in enumerate(PERSON_SCHEMAS):
            count = person_schema_counts[schema_idx]
            if count <= 0:
                continue

            # Header for this block
            f_nodes.write("|".join(schema) + "\n")

            for _ in range(count):
                node_counter += 1
                node_id = f"P{node_counter}"   # P-prefix for people
                person_ids.append(node_id)

                # Base fields
                row: Dict[str, str] = {
                    "@id": node_id,
                    "@label": "Person",
                }

                # Fill variable fields
                name = make_person_name(rng)

                if "name" in schema:
                    row["name"] = name
                if "age" in schema:
                    row["age"] = str(rng.randrange(18, 75))
                if "birth_year" in schema:
                    row["birth_year"] = str(rng.randrange(1945, 2010))
                if "city" in schema:
                    row["city"] = choice_rand(rng, CITIES)
                if "profession" in schema:
                    row["profession"] = choice_rand(rng, PROFessions:=PROFESSIONS)
                if "email" in schema:
                    row["email"] = make_email(name)
                if "phone" in schema:
                    row["phone"] = make_phone(rng)
                if "url" in schema:
                    row["url"] = make_url(node_id)

                # Emit row in schema order
                f_nodes.write("|".join(row.get(col, "") for col in schema) + "\n")

        # --- ORGANIZATION blocks
        org_counter = 0
        for schema_idx, schema in enumerate(ORG_SCHEMAS):
            count = org_schema_counts[schema_idx]
            if count <= 0:
                continue

            # Header for this block
            f_nodes.write("|".join(schema) + "\n")

            for _ in range(count):
                org_counter += 1
                node_id = f"O{org_counter}"  # O-prefix for orgs
                org_ids.append(node_id)

                row: Dict[str, str] = {
                    "@id": node_id,
                    "@label": "Organization",
                }

                # Variable fields
                name = make_org_name(rng, org_counter)
                if "name" in schema:
                    row["name"] = name
                if "type" in schema:
                    row["type"] = choice_rand(rng, ORG_TYPES)
                if "industry" in schema:
                    row["industry"] = choice_rand(rng, INDUSTRIES)
                # city / hq_city
                if "city" in schema:
                    row["city"] = choice_rand(rng, CITIES)
                if "hq_city" in schema:
                    row["hq_city"] = choice_rand(rng, CITIES)
                if "website" in schema:
                    row["website"] = make_website(rng, name)
                if "founded_year" in schema:
                    row["founded_year"] = str(rng.randrange(1900, 2022))
                if "url" in schema:
                    row["url"] = make_url(node_id)

                f_nodes.write("|".join(row.get(col, "") for col in schema) + "\n")

    # Safety checks for edges
    if not person_ids:
        print("No Person nodes generated; cannot create typed edges.", file=sys.stderr)
        return
    if not org_ids:
        print("Warning: No Organization nodes generated; 'Works_for' and some 'Likes' edges will be limited.", file=sys.stderr)

    # Generate edges with constraints
    with OUT_EDGES.open("w", encoding="utf-8") as f_edges:
        f_edges.write("@id|@label|@dir|@out|@in\n")
        # Prebind choices to avoid repeated global lookups
        prand = rng.randrange
        choose_person = lambda: person_ids[prand(0, len(person_ids))]
        choose_org = (lambda: org_ids[prand(0, len(org_ids))]) if org_ids else choose_person

        # We’ll distribute labels roughly: 40% Knows, 30% Works_for, 30% Likes
        label_buckets = [("Knows", 0.4), ("Works_for", 0.3), ("Likes", 0.3)]
        edge_counts = split_counts(NUM_ARISTAS, [w for _, w in label_buckets])

        eid = 0

        # Knows: Person -> Person
        for _ in range(edge_counts[0]):
            eid += 1
            out_p = choose_person()
            in_p = choose_person()
            while in_p == out_p:
                in_p = choose_person()
            f_edges.write(f"E{eid}|Knows|T|{out_p}|{in_p}\n")

        # Works_for: Person -> Organization  (if no orgs, fallback to Person->Person just to keep count consistent)
        for _ in range(edge_counts[1]):
            eid += 1
            out_p = choose_person()
            if org_ids:
                in_o = choose_org()
                f_edges.write(f"E{eid}|Works_for|T|{out_p}|{in_o}\n")
            else:
                in_p = choose_person()
                while in_p == out_p:
                    in_p = choose_person()
                f_edges.write(f"E{eid}|Works_for|T|{out_p}|{in_p}\n")

        # Likes: Person -> (Person | Organization)
        for _ in range(edge_counts[2]):
            eid += 1
            out_p = choose_person()
            if org_ids and rng.random() < 0.5:
                in_node = choose_org()
            else:
                in_node = choose_person()
                while in_node == out_p:
                    in_node = choose_person()
            f_edges.write(f"E{eid}|Likes|T|{out_p}|{in_node}\n")


if __name__ == "__main__":
    main()
