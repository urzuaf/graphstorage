from __future__ import annotations
import random
import sys
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

# =========================
# CONFIG
# =========================
SEED = 1337
NUM_NODOS = 100_000
NUM_ARISTAS = 450_000

OUT_NODES = Path("Nodes.pgdf")
OUT_EDGES = Path("Edges.pgdf")

# Ratio of persons vs orgs
PERSON_RATIO = 0.85
ORG_RATIO = 1.0 - PERSON_RATIO

BASE_URL = "https://example.org/node/"

# Person & Organization schema variants.
PERSON_SCHEMAS: List[Tuple[str, ...]] = [
    ("@id", "@label", "name", "age", "city", "email", "url"),
    ("@id", "@label", "name", "birth_year", "city", "profession", "url"),
    ("@id", "@label", "name", "age", "city", "phone", "url"),
]

ORG_SCHEMAS: List[Tuple[str, ...]] = [
    ("@id", "@label", "name", "type", "city", "website", "founded_year", "url"),
    ("@id", "@label", "name", "industry", "hq_city", "website", "url"),
]

# Names & data pools
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

# =========================
# MAIN GENERATION
# =========================

def main() -> None:
    rng = random.Random(SEED)

    # Determine counts
    num_persons = int(NUM_NODOS * PERSON_RATIO)
    num_orgs = NUM_NODOS - num_persons

    # Pick one schema per type
    person_schema = choice_rand(rng, PERSON_SCHEMAS)
    org_schema = choice_rand(rng, ORG_SCHEMAS)

    person_ids: List[str] = []
    org_ids: List[str] = []

    # Generate nodes
    with OUT_NODES.open("w", encoding="utf-8") as f_nodes:
        # --- PERSONS ---
        f_nodes.write("|".join(person_schema) + "\n")
        for i in range(1, num_persons + 1):
            node_id = f"P{i}"
            person_ids.append(node_id)

            row: Dict[str, str] = {"@id": node_id, "@label": "Person"}
            name = make_person_name(rng)

            if "name" in person_schema:
                row["name"] = name
            if "age" in person_schema:
                row["age"] = str(rng.randrange(18, 75))
            if "birth_year" in person_schema:
                row["birth_year"] = str(rng.randrange(1945, 2010))
            if "city" in person_schema:
                row["city"] = choice_rand(rng, CITIES)
            if "profession" in person_schema:
                row["profession"] = choice_rand(rng, PROFESSIONS)
            if "email" in person_schema:
                row["email"] = make_email(name)
            if "phone" in person_schema:
                row["phone"] = make_phone(rng)
            if "url" in person_schema:
                row["url"] = make_url(node_id)

            f_nodes.write("|".join(row.get(col, "") for col in person_schema) + "\n")

        # --- ORGANIZATIONS ---
        f_nodes.write("|".join(org_schema) + "\n")
        for i in range(1, num_orgs + 1):
            node_id = f"O{i}"
            org_ids.append(node_id)

            row: Dict[str, str] = {"@id": node_id, "@label": "Organization"}
            name = make_org_name(rng, i)

            if "name" in org_schema:
                row["name"] = name
            if "type" in org_schema:
                row["type"] = choice_rand(rng, ORG_TYPES)
            if "industry" in org_schema:
                row["industry"] = choice_rand(rng, INDUSTRIES)
            if "city" in org_schema:
                row["city"] = choice_rand(rng, CITIES)
            if "hq_city" in org_schema:
                row["hq_city"] = choice_rand(rng, CITIES)
            if "website" in org_schema:
                row["website"] = make_website(rng, name)
            if "founded_year" in org_schema:
                row["founded_year"] = str(rng.randrange(1900, 2022))
            if "url" in org_schema:
                row["url"] = make_url(node_id)

            f_nodes.write("|".join(row.get(col, "") for col in org_schema) + "\n")

    # --- EDGES ---
    if not person_ids:
        print("No Person nodes generated; cannot create typed edges.", file=sys.stderr)
        return
    if not org_ids:
        print("Warning: No Organization nodes generated; 'Works_for' and some 'Likes' edges will be limited.", file=sys.stderr)

    with OUT_EDGES.open("w", encoding="utf-8") as f_edges:
        f_edges.write("@id|@label|@dir|@out|@in\n")

        prand = rng.randrange
        choose_person = lambda: person_ids[prand(0, len(person_ids))]
        choose_org = (lambda: org_ids[prand(0, len(org_ids))]) if org_ids else choose_person

        label_buckets = [("Knows", 0.4), ("Works_for", 0.3), ("Likes", 0.3)]
        edge_counts = [int(NUM_ARISTAS * w) for _, w in label_buckets]

        eid = 0
        # Knows: Person -> Person
        for _ in range(edge_counts[0]):
            eid += 1
            out_p = choose_person()
            in_p = choose_person()
            while in_p == out_p:
                in_p = choose_person()
            f_edges.write(f"E{eid}|Knows|T|{out_p}|{in_p}\n")

        # Works_for: Person -> Organization
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
