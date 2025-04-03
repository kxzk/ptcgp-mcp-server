import json
from typing import Any

import httpx
import pandas as pd
from fuzzywuzzy import process
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("ptcgp")


def _load_data():
    """Load and preprocess card data with error handling"""
    try:
        df = pd.read_csv("cards_2025-03-26.csv")
        # Convert stringified JSON columns to Python objects
        df["attack"] = df["attack"].apply(
            lambda x: json.loads(x) if pd.notna(x) else []
        )
        df["ability"] = df["ability"].apply(
            lambda x: json.loads(x) if pd.notna(x) else []
        )
        return df
    except FileNotFoundError:
        return None


@mcp.tool()
async def get_card_data(card_id: str) -> Any:
    """Get card data by exact ID match"""
    df = _load_data()
    if df is None:
        return {"error": "Card database missing"}, 500

    card = df[df["id"] == card_id].to_dict(orient="records")
    return card[0] if card else {"error": "Card not found"}, 404


@mcp.tool()
async def fuzzy_search_pokemon(name: str) -> Any:
    """Fuzzy search Pokémon by name"""
    df = _load_data()
    if df is None:
        return {"error": "Card database missing"}, 500

    names = df["name"].unique().tolist()
    best_match = process.extractOne(name, names)

    if best_match[1] < 60:
        return {"error": "No close match found"}, 404

    return df[df["name"] == best_match[0]].to_dict(orient="records")


@mcp.tool()
async def get_all_card_ids() -> Any:
    """Retrieve all card IDs"""
    df = _load_data()
    if df is None:
        return {"error": "Card database missing"}, 500

    return df["id"].unique().tolist()


@mcp.tool()
async def filter_by_color(color: str) -> Any:
    """Filter cards by color"""
    df = _load_data()
    if df is None:
        return {"error": "Card database missing"}, 500

    filtered = df[df["color"].str.lower() == color.lower()]
    return (
        filtered.to_dict(orient="records")
        if not filtered.empty
        else {"error": "No cards found"}
    ), 404


@mcp.tool()
async def fuzzy_search_ability(ability_query: str) -> Any:
    """Fuzzy search cards by ability text"""
    df = _load_data()
    if df is None:
        return {"error": "Card database missing"}, 500

    # Extract and flatten abilities
    all_abilities = []
    for _, row in df.iterrows():
        for ability in row["ability"]:
            all_abilities.append((ability["info"], row["id"]))

    # Find best ability match
    ability_texts = [a[0] for a in all_abilities]
    best_match = process.extractOne(ability_query, ability_texts)

    if not best_match or best_match[1] < 60:
        return {"error": "No matching abilities found"}, 404

    # Get all cards with this ability
    matched_ids = [a[1] for a in all_abilities if a[0] == best_match[0]]
    results = df[df["id"].isin(matched_ids)]

    return results.to_dict(orient="records")


if __name__ == "__main__":
    mcp.run(transport="stdio")
