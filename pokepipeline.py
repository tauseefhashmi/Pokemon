

#```python
#!/usr/bin/env python3
"""
pokepipeline.py
Fetches pokemon data from PokeAPI, transforms and loads into SQLite.
Usage:
    python pokepipeline.py --start-id 1 --end-id 20
    python pokepipeline.py --ids 1 4 7 25
"""

import argparse
import sqlite3
import time
import requests
from typing import List, Dict, Any, Optional, Tuple

BASE = "https://pokeapi.co/api/v2"

# -----------------------
# Utilities: HTTP fetch with retries
# -----------------------
def fetch_json(url: str, max_retries: int = 3, backoff: float = 0.5) -> Optional[Dict[str, Any]]:
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 404:
                return None
            else:
                print(f"Warning: {url} returned status {r.status_code}. Attempt {attempt}/{max_retries}")
        except requests.RequestException as e:
            print(f"Request exception for {url}: {e}. Attempt {attempt}/{max_retries}")
        time.sleep(backoff * attempt)
    print(f"Failed to fetch {url} after {max_retries} attempts.")
    return None

# -----------------------
# DB schema and helpers
# -----------------------
SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS pokemon (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    height INTEGER,
    weight INTEGER,
    base_experience INTEGER,
    species_url TEXT,
    evolution_chain_id INTEGER
);

CREATE TABLE IF NOT EXISTS types (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS abilities (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS pokemon_types (
    pokemon_id INTEGER,
    type_id INTEGER,
    slot INTEGER,
    PRIMARY KEY (pokemon_id, type_id),
    FOREIGN KEY (pokemon_id) REFERENCES pokemon(id) ON DELETE CASCADE,
    FOREIGN KEY (type_id) REFERENCES types(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS pokemon_abilities (
    pokemon_id INTEGER,
    ability_id INTEGER,
    slot INTEGER,
    is_hidden INTEGER,
    PRIMARY KEY (pokemon_id, ability_id),
    FOREIGN KEY (pokemon_id) REFERENCES pokemon(id) ON DELETE CASCADE,
    FOREIGN KEY (ability_id) REFERENCES abilities(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS stats (
    pokemon_id INTEGER,
    stat_name TEXT,
    base_stat INTEGER,
    effort INTEGER,
    PRIMARY KEY (pokemon_id, stat_name),
    FOREIGN KEY (pokemon_id) REFERENCES pokemon(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS evolutions (
    chain_id INTEGER,
    from_species TEXT,
    to_species TEXT,
    PRIMARY KEY (chain_id, from_species, to_species)
);
"""

def init_db(conn: sqlite3.Connection):
    conn.executescript(SCHEMA)
    conn.commit()

# -----------------------
# Transform helpers
# -----------------------
def extract_pokemon_basic(raw: Dict[str, Any]) -> Dict[str, Any]:
    # raw is /pokemon/{id}
    return {
        "id": raw["id"],
        "name": raw["name"],
        "height": raw.get("height"),
        "weight": raw.get("weight"),
        "base_experience": raw.get("base_experience"),
        "species_url": raw["species"]["url"] if raw.get("species") else None
    }

def extract_types(raw: Dict[str, Any]) -> List[Tuple[int, str, int]]:
    # returns list of (type_id (we use id parsed from url), type_name, slot)
    res = []
    for t in raw.get("types", []):
        type_url = t["type"]["url"]
        type_name = t["type"]["name"]
        # parse id from url e.g. https://pokeapi.co/api/v2/type/3/
        try:
            type_id = int(type_url.rstrip("/").split("/")[-1])
        except:
            type_id = None
        res.append((type_id, type_name, t.get("slot", 0)))
    return res

def extract_abilities(raw: Dict[str, Any]) -> List[Tuple[int, str, int, bool]]:
    res = []
    for a in raw.get("abilities", []):
        ability_url = a["ability"]["url"]
        ability_name = a["ability"]["name"]
        try:
            ability_id = int(ability_url.rstrip("/").split("/")[-1])
        except:
            ability_id = None
        res.append((ability_id, ability_name, a.get("slot", 0), a.get("is_hidden", False)))
    return res

def extract_stats(raw: Dict[str, Any]) -> List[Tuple[str, int, int]]:
    res = []
    for s in raw.get("stats", []):
        stat_name = s["stat"]["name"]
        res.append((stat_name, s.get("base_stat", 0), s.get("effort", 0)))
    return res

# -----------------------
# Evolution chain flattening
# -----------------------
def flatten_evolution_chain(chain: Dict[str, Any]) -> List[Tuple[str, str]]:
    """
    Given the 'chain' node from evolution-chain endpoint, returns list of edges (from_species, to_species)
    Handles multiple evolves_to entries by flattening.
    """
    edges = []

    def walk(node):
        from_species = node["species"]["name"]
        for child in node.get("evolves_to", []):
            to_species = child["species"]["name"]
            edges.append((from_species, to_species))
            walk(child)

    walk(chain)
    return edges

# -----------------------
# Insert helpers
# -----------------------
def upsert_pokemon(conn: sqlite3.Connection, p: Dict[str, Any], evolution_chain_id: Optional[int]):
    conn.execute("""
        INSERT OR REPLACE INTO pokemon (id, name, height, weight, base_experience, species_url, evolution_chain_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (p["id"], p["name"], p["height"], p["weight"], p["base_experience"], p["species_url"], evolution_chain_id))

def upsert_type(conn: sqlite3.Connection, type_id: Optional[int], type_name: str) -> int:
    if type_id is None:
        # try insert or get id by name
        cur = conn.execute("SELECT id FROM types WHERE name = ?", (type_name,))
        row = cur.fetchone()
        if row:
            return row[0]
        cur = conn.execute("INSERT INTO types (name) VALUES (?)", (type_name,))
        return cur.lastrowid
    else:
        conn.execute("INSERT OR IGNORE INTO types (id, name) VALUES (?, ?)", (type_id, type_name))
        return type_id

def upsert_ability(conn: sqlite3.Connection, ability_id: Optional[int], ability_name: str) -> int:
    if ability_id is None:
        cur = conn.execute("SELECT id FROM abilities WHERE name = ?", (ability_name,))
        row = cur.fetchone()
        if row:
            return row[0]
        cur = conn.execute("INSERT INTO abilities (name) VALUES (?)", (ability_name,))
        return cur.lastrowid
    else:
        conn.execute("INSERT OR IGNORE INTO abilities (id, name) VALUES (?, ?)", (ability_id, ability_name))
        return ability_id

def insert_pokemon_type(conn: sqlite3.Connection, pokemon_id: int, type_id: int, slot: int):
    conn.execute("""
        INSERT OR REPLACE INTO pokemon_types (pokemon_id, type_id, slot)
        VALUES (?, ?, ?)
    """, (pokemon_id, type_id, slot))

def insert_pokemon_ability(conn: sqlite3.Connection, pokemon_id: int, ability_id: int, slot: int, is_hidden: bool):
    conn.execute("""
        INSERT OR REPLACE INTO pokemon_abilities (pokemon_id, ability_id, slot, is_hidden)
        VALUES (?, ?, ?, ?)
    """, (pokemon_id, ability_id, slot, int(is_hidden)))

def insert_stat(conn: sqlite3.Connection, pokemon_id: int, stat_name: str, base_stat: int, effort: int):
    conn.execute("""
        INSERT OR REPLACE INTO stats (pokemon_id, stat_name, base_stat, effort)
        VALUES (?, ?, ?, ?)
    """, (pokemon_id, stat_name, base_stat, effort))

def insert_evolutions(conn: sqlite3.Connection, chain_id: int, edges: List[Tuple[str, str]]):
    for from_sp, to_sp in edges:
        conn.execute("""
            INSERT OR REPLACE INTO evolutions (chain_id, from_species, to_species)
            VALUES (?, ?, ?)
        """, (chain_id, from_sp, to_sp))

# -----------------------
# Main pipeline logic
# -----------------------
def fetch_and_store_pokemon(conn: sqlite3.Connection, pokemon_ids: List[int]):
    for pid in pokemon_ids:
        print(f"Fetching pokemon id={pid} ...")
        p_raw = fetch_json(f"{BASE}/pokemon/{pid}")
        if not p_raw:
            print(f"Skipping id {pid} (no data).")
            continue

        basic = extract_pokemon_basic(p_raw)
        types = extract_types(p_raw)
        abilities = extract_abilities(p_raw)
        stats = extract_stats(p_raw)

        # Try to fetch evolution chain id via species endpoint
        evolution_chain_id = None
        if basic["species_url"]:
            species = fetch_json(basic["species_url"])
            if species and species.get("evolution_chain") and species["evolution_chain"].get("url"):
                ec_url = species["evolution_chain"]["url"]
                # parse integer id at end of ec_url
                try:
                    evolution_chain_id = int(ec_url.rstrip("/").split("/")[-1])
                except:
                    evolution_chain_id = None

                # fetch the chain and insert evolutions (best effort)
                chain_json = fetch_json(ec_url)
                if chain_json and chain_json.get("chain"):
                    edges = flatten_evolution_chain(chain_json["chain"])
                    if edges:
                        insert_evolutions(conn, evolution_chain_id, edges)
                        print(f"Inserted {len(edges)} evolution edges for chain {evolution_chain_id}")

        # Upsert pokemon row
        upsert_pokemon(conn, basic, evolution_chain_id)

        # Types
        for t_id, t_name, slot in types:
            real_type_id = upsert_type(conn, t_id, t_name)
            insert_pokemon_type(conn, basic["id"], real_type_id, slot)

        # Abilities
        for a_id, a_name, slot, is_hidden in abilities:
            real_ability_id = upsert_ability(conn, a_id, a_name)
            insert_pokemon_ability(conn, basic["id"], real_ability_id, slot, is_hidden)

        # Stats
        for stat_name, base_stat, effort in stats:
            insert_stat(conn, basic["id"], stat_name, base_stat, effort)

        conn.commit()
        print(f"Stored pokemon {basic['name']} (id={basic['id']})")

# -----------------------
# CLI
# -----------------------
def parse_args():
    p = argparse.ArgumentParser(description="PokéPipeline: fetch and store Pokemon data")
    group = p.add_mutually_exclusive_group(required=False)
    group.add_argument("--ids", nargs="+", type=int, help="List of pokemon IDs to fetch")
    group.add_argument("--start-id", type=int, help="Start ID (inclusive)")
    p.add_argument("--end-id", type=int, help="End ID (inclusive) when using --start-id")
    p.add_argument("--db", default="pokepipeline.db", help="Path to SQLite DB")
    return p.parse_args()

def main():
    args = parse_args()
    if args.ids:
        pokemon_ids = args.ids
    elif args.start_id:
        end = args.end_id if args.end_id else args.start_id
        pokemon_ids = list(range(args.start_id, end + 1))
    else:
        pokemon_ids = list(range(1, 21))  # default 1..20

    conn = sqlite3.connect(args.db)
    init_db(conn)
    try:
        fetch_and_store_pokemon(conn, pokemon_ids)
    finally:
        conn.close()
    print("Done.")

if __name__ == "__main__":
    main()

