
## Portfolio Highlights 🚀
- Built a complete **ETL pipeline** in Python to fetch, transform, and store Pokémon data from a public API.
- Designed a **normalized relational database schema** (Pokémon, types, abilities, stats, evolutions) in SQLite.
- Implemented **robust API handling** with retries and error handling for reliable data fetching.
- Modular, clean, and maintainable Python code with CLI for flexible usage.
- Ready for extension: could add Docker, GraphQL API, or interactive front-end.


# PokéPipeline 🐾

**A data pipeline that fetches, transforms, and stores Pokémon data from the public [PokeAPI](https://pokeapi.co/) into a relational SQLite database.**  

This project demonstrates building a complete ETL pipeline, handling nested JSON data, designing a normalized database schema, and storing structured data for easy querying.

---

## Project Overview
PokéPipeline extracts Pokémon data from the **PokeAPI**, transforms nested API responses into a relational format, and loads it into an **SQLite database**.  

Key entities include:
- Pokémon basic info (name, height, weight, base experience)  
- Types  
- Abilities  
- Stats  
- Evolution chain  

The pipeline is modular, Pythonic, and **Windows 11 ready**, making it easy to run and inspect the stored data.

---

## Features
- Fetch Pokémon data for specific IDs or ranges (default: first 20 Pokémon).  
- Transform raw JSON into normalized relational data.  
- Store data in **SQLite**, including relationships between Pokémon, types, abilities, stats, and evolutions.  
- Robust API handling with retries and error logging.  
- Modular Python functions for easy extension.  

---

## Database Schema
| Table | Description |
|-------|-------------|
| `pokemon` | Stores basic info for each Pokémon, including evolution chain reference. |
| `types` | Master table of Pokémon types. |
| `pokemon_types` | Many-to-many mapping between Pokémon and their types. |
| `abilities` | Master table of abilities. |
| `pokemon_abilities` | Many-to-many mapping between Pokémon and abilities, with hidden/slot info. |
| `stats` | Stores each Pokémon’s base stats (HP, Attack, Defense, etc.). |
| `evolutions` | Maps evolution relationships between Pokémon. |

## Database Relationships (ER Diagram)
- `pokemon` ⬌ `pokemon_types` ⬌ `types`  
  (Many-to-many: A Pokémon can have multiple types)
- `pokemon` ⬌ `pokemon_abilities` ⬌ `abilities`  
  (Many-to-many: A Pokémon can have multiple abilities)
- `pokemon` ⬌ `stats`  
  (One-to-many: Each Pokémon has multiple stats)
- `pokemon` ⬌ `evolutions` ⬌ `pokemon`  
  (Self-referential Many-to-many: Represents evolution relationships)


---

## Setup & Installation
### Requirements
- Python 3.8+  
- `requests` library  
- SQLite (built-in with Python)  

### Installation
1. Clone the repository:
```bash
git clone https://github.com/your-username/pokepipeline.git
cd pokepipeline
```
2. Install Python dependencies:
```bash
pip install requests
```
3.The pipeline uses SQLite by default — no additional installation is required.

### Usage
1.Run the pipeline with default first 20 Pokémon:
```
python pokepipeline.py
```
2.Fetch specific Pokémon IDs:
```
python pokepipeline.py --ids 1 4 7
```
3.Fetch a range of Pokémon:
```
python pokepipeline.py --start-id 1 --end-id 20
```
Optional: specify a custom database path:
```
python pokepipeline.py --db "C:/Users/YourUser/pokemon.db"
```

## Viewing Stored Data

1.Option A: SQLite CLI
```
sqlite3 pokemon.db
.tables
SELECT * FROM pokemon LIMIT 5;
SELECT * FROM types LIMIT 5;
SELECT * FROM abilities LIMIT 5;
.exit
```
2. Option B: Python
```
import sqlite3
conn = sqlite3.connect("pokemon.db")
cursor = conn.cursor()
cursor.execute("SELECT * FROM pokemon LIMIT 5")
for row in cursor.fetchall():
    print(row)
conn.close()
```

## Design Choices

- Normalized schema: Separate tables for Pokémon, types, abilities, stats, and evolutions.
- ETL modularity: Functions separated into extraction, transformation, and loading.
- SQLite: Chosen for simplicity and portability. Easy to switch to PostgreSQL.
- Error handling: API requests have retry logic with exponential backoff.
- Flexibility: CLI arguments allow fetching specific Pokémon IDs or ranges.

## Assumptions

- Evolution chain IDs are derived from species data URLs.
- Pokémon with missing API fields are skipped or stored with NULL where appropriate.
- Only the first 20 Pokémon are fetched by default for demo purposes.

## Potential Improvements

- Database: Switch to PostgreSQL for production-grade storage.
- API caching: Avoid repeated calls to PokeAPI.
- Front-end: Simple web UI to view Pokémon and filter/search by type or ability.
- GraphQL endpoint: Expose transformed Pokémon data via API.
- Docker: Containerize pipeline for easy deployment.
- Automated tests: Unit tests for extraction, transformation, and loading steps.
