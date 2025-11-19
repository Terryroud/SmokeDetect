from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import ping, streaming, frontend, heatmap, video_processing

app = FastAPI(
    title="Video Streaming API",
    description="""
    API для стриминга видеоданных через WebSocket.
    
    ## WebSocket эндпоинт
    
    **URL:** `/ws/stream/{stream_id}`
    
    WebSocket эндпоинт для стриминга видеоданных. Подробную информацию можно получить через `/stream/websocket-info`.
    
    **Процесс работы:**
    1. Создайте стрим через `/stream/request`
    2. Подключитесь к WebSocket по адресу `/ws/stream/{stream_id}`
    3. Отправляйте видеокадры в формате base64 data URI
    4. Получайте обработанные данные через WebSocket
    
    **Формат данных:**
    - Отправка: `data:image/jpeg;base64,...`
    - Получение: `{ "time": float, "frames": [...] }`
    
    ---
    
    ## Тепловая карта
    
    **URL:** `/heatmap`
    
    Отображает тепловую карту на основе данных из `data.json`.
    
    **Эндпоинт данных:** `/api/heatmap-data`
    """,
    version="1.0.0"
)

# Add CORS middleware for ngrok and cross-origin requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify exact origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(ping.router)
app.include_router(streaming.router)
app.include_router(streaming.websocket_router)  # WebSocket роутер без префикса
app.include_router(frontend.router)
app.include_router(heatmap.router)
app.include_router(video_processing.router)

@app.get("/")
async def root():
    return {"message": "Hello World"}
