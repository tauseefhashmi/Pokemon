#!/usr/bin/env python3
"""
PokéPipeline: Fetch Pokémon data from PokeAPI and store in local SQLite database.
Includes Pokémon info, types, abilities, stats, and evolution chain.
"""

import argparse
import sqlite3
import time
import requests
from typing import List, Dict, Any, Optional, Tuple

# -----------------------
# Constants
# -----------------------
BASE = "https://pokeapi.co/api/v2"  # Base URL for PokeAPI
DB_FILE = "pokemon.db"              # Default SQLite database file

# -----------------------
# Utility functions
# -----------------------
def fetch_json(url: str, max_retries: int = 3, backoff: float = 0.5) -> Optional[Dict[str, Any]]:
    """
    Fetch JSON data from a given URL with retry and exponential backoff.
    Returns the JSON dict on success, None on failure or 404.
    """
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 404:
                return None
            else:
                print(f"[WARN] {url} returned status {r.status_code} (Attempt {attempt}/{max_retries})")
        except requests.RequestException as e:
            print(f"[ERROR] Request exception for {url}: {e} (Attempt {attempt}/{max_retries})")
        time.sleep(backoff * attempt)
    print(f"[FAIL] Could not fetch {url} after {max_retries} attempts.")
    return None

# -----------------------
# Database schema
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

CREATE TABLE IF NOT EXISTS pokemon_types (
    pokemon_id INTEGER,
    type_id INTEGER,
    slot INTEGER,
    PRIMARY KEY (pokemon_id, type_id),
    FOREIGN KEY (pokemon_id) REFERENCES pokemon(id) ON DELETE CASCADE,
    FOREIGN KEY (type_id) REFERENCES types(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS abilities (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS pokemon_abilities (
    pokemon_id INTEGER,
    ability_id INTEGER,
    slot INTEGER,
    is_hidden BOOLEAN,
    PRIMARY KEY (pokemon_id, ability_id),
    FOREIGN KEY (pokemon_id) REFERENCES pokemon(id) ON DELETE CASCADE,
    FOREIGN KEY (ability_id) REFERENCES abilities(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS stats (
    pokemon_id INTEGER,
    name TEXT,
    base_stat INTEGER,
    effort INTEGER,
    PRIMARY KEY (pokemon_id, name),
    FOREIGN KEY (pokemon_id) REFERENCES pokemon(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS evolutions (
    from_pokemon_id INTEGER,
    to_pokemon_id INTEGER,
    PRIMARY KEY (from_pokemon_id, to_pokemon_id),
    FOREIGN KEY (from_pokemon_id) REFERENCES pokemon(id),
    FOREIGN KEY (to_pokemon_id) REFERENCES pokemon(id)
);
"""

def init_db(conn: sqlite3.Connection):
    """
    Initialize the SQLite database and create tables if they do not exist.
    """
    conn.executescript(SCHEMA)
    conn.commit()
    print(f"[INFO] Database initialized and tables created (if not exist).")

# -----------------------
# Transform helpers
# -----------------------
def extract_pokemon_basic(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract basic Pokémon info from raw JSON.
    """
    return {
        "id": raw["id"],
        "name": raw["name"],
        "height": raw.get("height"),
        "weight": raw.get("weight"),
        "base_experience": raw.get("base_experience"),
        "species_url": raw["species"]["url"] if raw.get("species") else None
    }

def extract_types(raw: Dict[str, Any]) -> List[Tuple[int, str, int]]:
    """
    Extract Pokémon types from raw JSON.
    Returns list of tuples: (type_id, type_name, slot)
    """
    res = []
    for t in raw.get("types", []):
        type_url = t["type"]["url"]
        type_name = t["type"]["name"]
        try:
            type_id = int(type_url.rstrip("/").split("/")[-1])
        except:
            type_id = None
        res.append((type_id, type_name, t.get("slot", 0)))
    return res

def extract_abilities(raw: Dict[str, Any]) -> List[Tuple[int, str, int, bool]]:
    """
    Extract Pokémon abilities from raw JSON.
    Returns list of tuples: (ability_id, ability_name, slot, is_hidden)
    """
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
    """
    Extract Pokémon stats from raw JSON.
    Returns list of tuples: (stat_name, base_stat, effort)
    """
    res = []
    for s in raw.get("stats", []):
        res.append((s["stat"]["name"], s.get("base_stat", 0), s.get("effort", 0)))
    return res

def extract_evolution_chain(species_url: str) -> Optional[int]:
    """
    Extract evolution chain ID from species URL.
    """
    species_data = fetch_json(species_url)
    if species_data and species_data.get("evolution_chain"):
        chain_url = species_data["evolution_chain"]["url"]
        try:
            return int(chain_url.rstrip("/").split("/")[-1])
        except:
            return None
    return None

# -----------------------
# Database insert helpers
# -----------------------
def upsert_pokemon(conn: sqlite3.Connection, p: Dict[str, Any], evolution_chain_id: Optional[int]):
    """
    Insert or replace Pokémon basic info into the 'pokemon' table.
    """
    conn.execute("""
        INSERT OR REPLACE INTO pokemon (id, name, height, weight, base_experience, species_url, evolution_chain_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (p["id"], p["name"], p["height"], p["weight"], p["base_experience"], p["species_url"], evolution_chain_id))

def upsert_type(conn: sqlite3.Connection, type_id: Optional[int], type_name: str) -> int:
    """
    Insert type into 'types' table or get existing ID.
    Returns the type_id used.
    """
    if type_id is None:
        cur = conn.execute("SELECT id FROM types WHERE name = ?", (type_name,))
        row = cur.fetchone()
        if row:
            return row[0]
        cur = conn.execute("INSERT INTO types (name) VALUES (?)", (type_name,))
        return cur.lastrowid
    else:
        conn.execute("INSERT OR IGNORE INTO types (id, name) VALUES (?, ?)", (type_id, type_name))
        return type_id

def insert_pokemon_type(conn: sqlite3.Connection, pokemon_id: int, type_id: int, slot: int):
    """
    Link Pokémon with type in 'pokemon_types' table.
    """
    conn.execute("""
        INSERT OR REPLACE INTO pokemon_types (pokemon_id, type_id, slot)
        VALUES (?, ?, ?)
    """, (pokemon_id, type_id, slot))

def upsert_ability(conn: sqlite3.Connection, ability_id: Optional[int], ability_name: str) -> int:
    """
    Insert ability into 'abilities' table or get existing ID.
    Returns the ability_id used.
    """
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

def insert_pokemon_ability(conn: sqlite3.Connection, pokemon_id: int, ability_id: int, slot: int, is_hidden: bool):
    """
    Link Pokémon with ability in 'pokemon_abilities' table.
    """
    conn.execute("""
        INSERT OR REPLACE INTO pokemon_abilities (pokemon_id, ability_id, slot, is_hidden)
        VALUES (?, ?, ?, ?)
    """, (pokemon_id, ability_id, slot, is_hidden))

def insert_stat(conn: sqlite3.Connection, pokemon_id: int, name: str, base_stat: int, effort: int):
    """
    Insert a stat for a Pokémon into 'stats' table.
    """
    conn.execute("""
        INSERT OR REPLACE INTO stats (pokemon_id, name, base_stat, effort)
        VALUES (?, ?, ?, ?)
    """, (pokemon_id, name, base_stat, effort))

# -----------------------
# Main pipeline
# -----------------------
def fetch_and_store_pokemon(conn: sqlite3.Connection, pokemon_ids: List[int]):
    """
    Fetch Pokémon data for a list of IDs and store in SQLite database.
    """
    for pid in pokemon_ids:
        print(f"[INFO] Fetching Pokémon ID {pid}...")
        raw = fetch_json(f"{BASE}/pokemon/{pid}")
        if not raw:
            print(f"[WARN] Skipping ID {pid}.")
            continue

        # Extract data
        basic = extract_pokemon_basic(raw)
        types = extract_types(raw)
        abilities = extract_abilities(raw)
        stats = extract_stats(raw)
        evolution_chain_id = extract_evolution_chain(basic["species_url"]) if basic["species_url"] else None

        # Insert Pokémon
        upsert_pokemon(conn, basic, evolution_chain_id)

        # Insert types
        for t_id, t_name, slot in types:
            real_type_id = upsert_type(conn, t_id, t_name)
            insert_pokemon_type(conn, basic["id"], real_type_id, slot)

        # Insert abilities
        for a_id, a_name, slot, is_hidden in abilities:
            real_ability_id = upsert_ability(conn, a_id, a_name)
            insert_pokemon_ability(conn, basic["id"], real_ability_id, slot, is_hidden)

        # Insert stats
        for stat_name, base_stat, effort in stats:
            insert_stat(conn, basic["id"], stat_name, base_stat, effort)

        conn.commit()
        print(f"[SUCCESS] Stored Pokémon: {basic['name']} (ID {basic['id']})")

# -----------------------
# CLI argument parsing
# -----------------------
def parse_args():
    """
    Parse command-line arguments for the pipeline.
    """
    p = argparse.ArgumentParser(description="PokéPipeline: Fetch and store Pokémon data")
    group = p.add_mutually_exclusive_group()
    group.add_argument("--ids", nargs="+", type=int, help="Specific Pokémon IDs")
    group.add_argument("--start-id", type=int, help="Start ID (inclusive)")
    p.add_argument("--end-id", type=int, help="End ID (inclusive) for --start-id")
    p.add_argument("--db", default=DB_FILE, help="SQLite database file path")
    return p.parse_args()

def main():
    """
    Main function: Determine Pokémon IDs, connect to DB, fetch data, and store.
    """
    args = parse_args()

    # Determine IDs to fetch
    if args.ids:
        pokemon_ids = args.ids
    elif args.start_id:
        end = args.end_id if args.end_id else args.start_id
        pokemon_ids = list(range(args.start_id, end + 1))
    else:
        pokemon_ids = list(range(1, 21))  # default first 20 Pokémon

    # Connect to DB
    conn = sqlite3.connect(args.db)
    init_db(conn)

    try:
        fetch_and_store_pokemon(conn, pokemon_ids)
    finally:
        conn.close()
        print(f"[INFO] Database connection closed. Data stored in {args.db}")

if __name__ == "__main__":
    main()
