from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse
import json

router = APIRouter()

@router.get("/heatmap", response_class=HTMLResponse)
async def get_heatmap_page():
    try:
        with open("templates/index.html", "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="index.html not found")

@router.get("/api/heatmap-data")
async def get_heatmap_data():
    try:
        with open('data.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except FileNotFoundError:
        return []
