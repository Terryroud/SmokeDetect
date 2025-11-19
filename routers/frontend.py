from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

router = APIRouter()
templates = Jinja2Templates(directory="templates", auto_reload=True)

@router.get("/test", response_class=HTMLResponse)
async def read_item(request: Request):
    return templates.TemplateResponse("test.html", {"request": request})

@router.get("/watch", response_class=HTMLResponse)
async def read_viewer(request: Request):
    return templates.TemplateResponse("viewer.html", {"request": request})
