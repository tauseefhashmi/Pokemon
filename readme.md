# PokéPipeline

A small data pipeline that fetches Pokémon data from PokeAPI, transforms it into relational form and stores it in a SQLite database.

## What it does
- Fetches details for a list of Pokémon (default: IDs 1–20).
- Extracts and transforms types, abilities, stats, and a best-effort evolution chain.
- Loads normalized data into a SQLite database `pokepipeline.db`.

## Design choices
- **Relational schema**: `pokemon`, `types`, `abilities`, `pokemon_types`, `pokemon_abilities`, `stats`, `evolutions`. This allows easy joins and analytics (e.g., find Pokémon by type or ability, compare stats).
- **Evolution chain**: Retrieves evolution chain from species endpoint and flattens it into edges (from -> to). This simplifies relational mapping.
- **SQLite**: Chosen for simplicity and portability. Production setup would prefer Postgres and migrations.

## Requirements
- Python 3.9+ (works with 3.8+)
- Dependencies in `requirements.txt` (requests)

## Quick start (local)
1. Clone this repo.
2. Create and activate a virtual environment (recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate   # Windows: venv\Scripts\activate
   pip install -r requirements.txt
