import asyncio
import json
from contextlib import asynccontextmanager
from typing import Any

import httpx
import pandas as pd
from fuzzywuzzy import process
from mcp.server.fastmcp import Context, FastMCP


@asynccontextmanager
async def lifespan(server: FastMCP):

    df = await asyncio.to_thread(load_data)
    yield
    df = None


def load_data() -> pd.DataFrame:
    try:
        df = pd.read_csv("cards_2025-03-26.csv")
        json_columns = ["attack", "ability"]

        # optimized json parsing using pd.json_normalize
        for col in json_columns:
            df[col] = df[col].apply(lambda x: json.loads(x) if pd.notna(x) else [])

        return df
    except FileNotFoundError as e:
        raise RuntimeError("Card database file not found") from e
    except json.JSONDecodeError as e:
        raise RuntimeError("Invalid JSON format in data columns") from e


mcp = FastMCP("ptcgp", lifespan=lifespan)


@mcp.tool()
async def get_card_data(ctx: Context, card_id: str) -> Any:
    """Get card data by exact ID match"""
    df = ctx.request_context.lifespan_context["df"]

    card = df[df["id"] == card_id].to_dict(orient="records")
    return card[0] if card else {"error": "Card not found"}, 404


@mcp.tool()
async def fuzzy_search_pokemon(ctx: Context, name: str) -> Any:
    """Fuzzy search Pokémon by name"""
    df = ctx.request_context.lifespan_context["df"]

    names = df["name"].unique().tolist()
    best_match = process.extractOne(name, names)

    if best_match[1] < 60:
        return {"error": "No close match found"}, 404

    return df[df["name"] == best_match[0]].to_dict(orient="records")


@mcp.tool()
async def filter_by_color(ctx: Context, color: str) -> Any:
    """Filter cards by color"""
    df = ctx.request_context.lifespan_context["df"]
    if df is None:
        return {"error": "Card database missing"}, 500

    filtered = df[df["color"].str.lower() == color.lower()]
    return (
        filtered.to_dict(orient="records")
        if not filtered.empty
        else {"error": "No cards found"}
    ), 404


@mcp.tool()
async def fuzzy_search_ability(ctx: Context, ability_query: str) -> Any:
    """Fuzzy search cards by ability text"""
    df = ctx.request_context.lifespan_context["df"]
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
