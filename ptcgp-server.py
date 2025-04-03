import json
import os
from typing import Any

import httpx
import pandas as pd
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("ptcgp")


@mcp.tool()
async def get_card_data(card_id: str) -> Any:
    try:
        csv_path = "cards_2025-03-26.csv"
        df = pd.read_csv(csv_path)
        card = df[df["id"] == card_id]

        if card.empty:
            return {"error": "Card not found"}, 404

        return json.loads(card.to_json(orient="records")[1:-1])

    except FileNotFoundError:
        return {"error": "Card database missing"}, 500


if __name__ == "__main__":
    mcp.run(transport="stdio")
