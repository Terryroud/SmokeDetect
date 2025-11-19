"""
Модуль стриминга для FastAPI приложения.
Обрабатывает WebSocket видеопотоки, обработку кадров и управление стримами.
"""

import asyncio
import time
import uuid
import base64
import os
import re
import requests as http_requests
from typing import List, Dict
from collections import deque
from openai import OpenAI
import cv2
import numpy as np
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException, Body, Path, File, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from typing import List, Optional
from utils import detect_smoking, extract_hls_url_from_page

router = APIRouter(prefix="/stream", tags=["Streaming"])
websocket_router = APIRouter()


# ============================================================================
# PYDANTIC МОДЕЛИ ДЛЯ API
# ============================================================================

class FrameData(BaseModel):
    """Данные одного кадра с метрикой и координатами."""
    metric: float = Field(..., description="Метрика уверенности от 0.0 до 1.0", ge=0.0, le=1.0)
    cord: List[int] = Field(..., description="Координаты ограничивающего прямоугольника [x, y, width, height]", min_items=4, max_items=4)

class BroadcastRequest(BaseModel):
    """Запрос на отправку данных в стрим через WebSocket."""
    frames: List[FrameData] = Field(..., description="Массив данных кадров для отправки")
    


class StreamResponse(BaseModel):
    """Ответ при создании стрима."""
    stream_token: str = Field(..., description="Токен стрима (UUID)")
    stream_id: str = Field(..., description="ID стрима (UUID, совпадает с stream_token)")
    video_uuid: str = Field(..., description="UUID видео (совпадает с stream_id)")
    video_id: str = Field(..., description="ID видео (совпадает с stream_id)")
    websocket_url: str = Field(..., description="URL для подключения к WebSocket")
    status: str = Field(..., description="Статус стрима")

class BroadcastResponse(BaseModel):
    """Ответ при отправке данных в стрим."""
    message: str = Field(..., description="Сообщение о результате")
    stream_id: str = Field(..., description="ID стрима")
    recipient_count: int = Field(..., description="Количество получателей данных")
    data: dict = Field(..., description="Отправленные данные с временной меткой")

class CloseStreamResponse(BaseModel):
    """Ответ при закрытии стрима."""
    message: str = Field(..., description="Сообщение о результате")
    stream_id: str = Field(..., description="ID стрима")
    status: str = Field(..., description="Статус стрима")
    closed_at: float = Field(..., description="Временная метка закрытия")

class StreamListResponse(BaseModel):
    """Ответ со списком активных стримов."""
    active_streams: List[str] = Field(..., description="Список ID активных стримов")
    count: int = Field(..., description="Количество активных стримов")

class SmokingDetectionResponse(BaseModel):
    """Ответ при детекции курения на фото."""
    verdict: str = Field(..., description="Результат детекции: Yes или No")
    timestamp: float = Field(..., description="Временная метка обработки")

class StreamUrlRequest(BaseModel):
    """Запрос на открытие стрима по URL."""
    url: str = Field(..., description="URL видео потока (HLS, RTSP, HTTP, blob и т.д.)")
    detection_interval: Optional[int] = Field(5, description="Интервал детекции курения в секундах (по умолчанию 5)")

class StreamUrlResponse(BaseModel):
    """Ответ при открытии стрима по URL."""
    stream_id: str = Field(..., description="ID созданного стрима")
    url: str = Field(..., description="URL источника видео")
    status: str = Field(..., description="Статус стрима")
    websocket_url: str = Field(..., description="WebSocket URL для получения результатов детекции")
    video_url: str = Field(..., description="URL для просмотра MJPEG потока")
    message: str = Field(..., description="Информационное сообщение")

# Хранилище сессий стримов в памяти
# Формат: { stream_id: { "live": bool, "closing": bool, "last_frame": bytes, "video_writer": cv2.VideoWriter, ... } }
# Каждый stream_id - это UUID, который служит одновременно токеном стрима и ID видео
# Состояния стрима:
#   - "live": True - стрим активен и принимает кадры
#   - "closing": True - стрим закрывается, обработка кадров завершается
#   - "live": False - стрим полностью закрыт
stream_sessions: Dict[str, Dict] = {}

# Хранилище последних кадров для каждого стрима (для MJPEG потока)
# Формат: { stream_id: bytes } - JPEG закодированный кадр
last_frames: Dict[str, bytes] = {}

# Очереди результатов детекции для упорядоченного вывода
# Формат: { stream_id: { 'queue': deque, 'next_frame': int, 'lock': asyncio.Lock } }
detection_queues: Dict[str, Dict] = {}

# Флаг для управления отображением видео (установите False для серверов без GUI)
# По умолчанию False, так как большинство серверов работают без GUI
ENABLE_VIDEO_DISPLAY = False

class ConnectionManager:
    """
    Управляет WebSocket соединениями для нескольких сессий стримов.
    Позволяет нескольким клиентам подключаться к одному стриму и получать транслируемые данные.
    """
    
    def __init__(self):
        # Словарь для отслеживания активных WebSocket соединений по стримам
        # Формат: { stream_id: [WebSocket1, WebSocket2, ...] }
        self.active_connections: Dict[str, List[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, token: str):
        """
        Добавляет WebSocket соединение в менеджер для конкретного стрима.
        
        Args:
            websocket: WebSocket соединение (должно быть уже принято)
            token: Stream ID для ассоциации соединения
        """
        # Примечание: websocket.accept() должен быть вызван до этого метода
        if token not in self.active_connections:
            self.active_connections[token] = []
        self.active_connections[token].append(websocket)

    def disconnect(self, websocket: WebSocket, token: str):
        """
        Удаляет WebSocket соединение из менеджера.
        
        ВАЖНО: Не удаляет стрим из stream_sessions, так как стрим должен оставаться
        доступным для:
        - Закрытия через /stream/close/{token}
        - Повторного подключения
        - Просмотра через /stream/video/{token}
        
        Args:
            websocket: WebSocket соединение для удаления
            token: Stream ID, связанный с соединением
        """
        if token in self.active_connections:
            if websocket in self.active_connections[token]:
                self.active_connections[token].remove(websocket)
            # Очищаем список соединений, если он пуст
            if not self.active_connections[token]:
                del self.active_connections[token]
                print(f"Все WebSocket соединения для стрима {token} отключены")
            else:
                print(f"Осталось {len(self.active_connections[token])} соединений для стрима {token}")

    async def broadcast_json(self, data: dict, token: str):
        """
        Транслирует JSON данные всем подключенным клиентам для конкретного стрима.
        
        Args:
            data: Словарь для отправки как JSON
            token: Stream ID для трансляции
        """
        if token in self.active_connections:
            for connection in self.active_connections[token]:
                await connection.send_json(data)

manager = ConnectionManager()

def get_active_streams() -> List[str]:
    """
    Внутренняя функция для получения списка активных Stream ID.
    Возвращает список Stream ID, которые в данный момент активны.
    """
    active_ids = list(stream_sessions.keys())
    print(f"get_active_streams вызвана. Найдено {len(active_ids)} стримов: {active_ids}")
    return active_ids

@router.get(
    "/websocket-info",
    summary="Информация о WebSocket эндпоинте",
    description="""
    Возвращает подробную информацию о WebSocket эндпоинте для стриминга видеоданных.
    
    **WebSocket URL:** `/ws/stream/{stream_id}`
    
    **Процесс работы:**
    1. Создайте стрим через `/stream/request` и получите `stream_id`
    2. Подключитесь к WebSocket по адресу `/ws/stream/{stream_id}`
    3. Отправляйте видеокадры в формате base64 data URI
    4. Получайте обработанные данные через WebSocket (если отправлены через `/stream/broadcast/{token}`)
    
    **Формат отправки кадров:**
    Кадры должны быть отправлены как base64-закодированные data URI:
    ```
    data:image/jpeg;base64,/9j/4AAQSkZJRg...
    ```
    
    **Пример использования (JavaScript):**
    ```javascript
    const streamId = "a0a0a0a0-a0a0-a0a0-a0a0-a0a0a0a0a0a0";
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}/ws/stream/${streamId}`;
    const socket = new WebSocket(wsUrl);
    
    socket.onopen = () => {
        console.log('WebSocket connected');
        const canvas = document.getElementById('canvas');
        const frame = canvas.toDataURL('image/jpeg', 0.7);
        socket.send(frame);
    };
    
    socket.onmessage = (event) => {
        const data = JSON.parse(event.data);
        console.log("Получены данные:", data);
    };
    ```
    
    **Пример использования (Python):**
    ```python
    import asyncio
    import websockets
    import base64
    import cv2
    
    async def stream_video():
        stream_id = "a0a0a0a0-a0a0-a0a0-a0a0-a0a0a0a0a0a0"
        uri = f"ws://localhost:8000/ws/stream/{stream_id}"
        
        async with websockets.connect(uri) as websocket:
            cap = cv2.VideoCapture(0)
            while True:
                ret, frame = cap.read()
                if not ret:
                    break
                _, buffer = cv2.imencode('.jpg', frame)
                img_base64 = base64.b64encode(buffer).decode('utf-8')
                data_uri = f"data:image/jpeg;base64,{img_base64}"
                await websocket.send(data_uri)
                await asyncio.sleep(0.033)  # ~30 FPS
            cap.release()
    
    asyncio.run(stream_video())
    ```
    
    **Коды ошибок WebSocket:**
    - `4000`: Stream ID не найден в stream_sessions
    - `1006`: Аномальное закрытие соединения (проверьте сервер)
    """,
    tags=["Streaming"]
)
async def websocket_info():
    """
    Возвращает информацию о WebSocket эндпоинте для стриминга.
    """
    return {
        "websocket_url": "/ws/stream/{stream_id}",
        "description": "WebSocket эндпоинт для стриминга видеоданных",
        "protocol": "WebSocket (ws://) или Secure WebSocket (wss://)",
        "format": "base64 data URI: data:image/jpeg;base64,...",
        "response_format": "JSON: { time: float, frames: [...] }",
        "example_javascript": """
const streamId = "YOUR_STREAM_ID";
const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
const wsUrl = `${protocol}//${window.location.host}/ws/stream/${streamId}`;
const socket = new WebSocket(wsUrl);

socket.onopen = () => {
    const canvas = document.getElementById('canvas');
    const frame = canvas.toDataURL('image/jpeg', 0.7);
    socket.send(frame);
};

socket.onmessage = (event) => {
    const data = JSON.parse(event.data);
    console.log("Получены данные:", data);
};
        """,
        "example_python": """
import asyncio
import websockets
import base64
import cv2

async def stream_video():
    stream_id = "YOUR_STREAM_ID"
    uri = f"ws://localhost:8000/ws/stream/{stream_id}"
    
    async with websockets.connect(uri) as websocket:
        cap = cv2.VideoCapture(0)
        while True:
            ret, frame = cap.read()
            if not ret:
                break
            _, buffer = cv2.imencode('.jpg', frame)
            img_base64 = base64.b64encode(buffer).decode('utf-8')
            data_uri = f"data:image/jpeg;base64,{img_base64}"
            await websocket.send(data_uri)
            await asyncio.sleep(0.033)  # ~30 FPS
        cap.release()

asyncio.run(stream_video())
        """,
        "error_codes": {
            "4000": "Stream ID не найден в stream_sessions",
            "1006": "Аномальное закрытие соединения (проверьте сервер)"
        },
        "notes": [
            "Если стрим не существует, он будет автоматически создан при подключении",
            "Разрешение видео берется из кадра камеры пользователя",
            "Частота кадров: 30 FPS",
            "Видео сохраняется в файл {stream_id}.mp4 в корне проекта"
        ]
    }

@router.get(
    "/list",
    response_model=StreamListResponse,
    summary="Получить список активных стримов",
    description="""
    Возвращает список всех активных Stream ID.
    
    Используйте этот эндпоинт для получения списка всех стримов, которые в данный момент активны.
    Стрим считается активным, если он был создан через `/stream/request` и еще не был закрыт через `/stream/close/{token}`.
    
    **Пример использования:**
    ```python
    import requests
    response = requests.get("http://localhost:8000/stream/list")
    active_streams = response.json()["active_streams"]
    ```
    """,
    tags=["Streaming"]
)
async def list_active_streams():
    """
    Возвращает список всех активных Stream ID.
    """
    print(f"Запрошен список активных стримов через /stream/list")
    print(f"Текущее состояние stream_sessions: {stream_sessions}")
    print(f"ID объекта stream_sessions: {id(stream_sessions)}")
    active_stream_ids = get_active_streams()
    print(f"Запрошен список активных стримов. Найдено {len(active_stream_ids)} стримов: {active_stream_ids}")
    return {
        "active_streams": active_stream_ids,
        "count": len(active_stream_ids),
        "debug": {
            "stream_sessions_keys": list(stream_sessions.keys()),
            "stream_sessions_count": len(stream_sessions),
            "stream_sessions_id": str(id(stream_sessions))
        }
    }

@router.get(
    "/status/{stream_id}",
    summary="Получить статус стрима",
    description="Возвращает текущий статус стрима, включая ошибки если есть",
    tags=["Streaming"]
)
async def get_stream_status(stream_id: str = Path(..., description="ID стрима")):
    """
    Получает статус стрима и информацию об ошибках.
    """
    if stream_id not in stream_sessions:
        raise HTTPException(status_code=404, detail=f"Stream {stream_id} not found")

    session = stream_sessions[stream_id]

    return {
        "stream_id": stream_id,
        "status": session.get("status", "unknown"),
        "live": session.get("live", False),
        "closing": session.get("closing", False),
        "error": session.get("error"),
        "url": session.get("url"),
        "created_at": session.get("created_at"),
        "type": session.get("type", "websocket")
    }

@router.post(
    "/detect-smoking",
    response_model=SmokingDetectionResponse,
    summary="Детекция курения на фото",
    description="""
    Загружает фото и определяет, курит ли человек на нем (сигарета или вейп).

    **Формат:**
    Отправьте фото в формате multipart/form-data (поддерживаются JPEG, PNG).

    **Что происходит:**
    1. Фото загружается на сервер
    2. Изображение отправляется в AI модель (nvidia/nemotron-nano-12b-v2-vl)
    3. Модель анализирует фото и возвращает ответ: "Yes" или "No"

    **Пример использования (Python):**
    ```python
    import requests

    with open("photo.jpg", "rb") as f:
        files = {"file": f}
        response = requests.post("http://localhost:8000/stream/detect-smoking", files=files)

    result = response.json()
    print(f"Курение обнаружено: {result['verdict']}")
    ```

    **Пример использования (cURL):**
    ```bash
    curl -X POST "http://localhost:8000/stream/detect-smoking" \
         -F "file=@photo.jpg"
    ```

    **Пример использования (JavaScript):**
    ```javascript
    const formData = new FormData();
    formData.append('file', fileInput.files[0]);

    fetch('http://localhost:8000/stream/detect-smoking', {
        method: 'POST',
        body: formData
    })
    .then(response => response.json())
    .then(data => {
        console.log('Результат детекции:', data.verdict);
    });
    ```

    **Ответ:**
    - `verdict`: "Yes" или "No"
    - `timestamp`: временная метка обработки

    **Ошибки:**
    - `400`: Неверный формат файла или ошибка обработки
    - `500`: Ошибка при вызове AI модели
    """,
    tags=["Smoking Detection"]
)
async def detect_smoking_photo(file: UploadFile = File(..., description="Фото для анализа (JPEG, PNG)")):
    """
    Определяет, курит ли человек на загруженном фото.

    Args:
        file: Загруженный файл изображения

    Returns:
        dict: Результат детекции с вердиктом (Yes/No) и временной меткой
    """
    try:
        # Читаем содержимое файла
        contents = await file.read()

        # Проверяем, что файл не пустой
        if not contents:
            raise HTTPException(status_code=400, detail="Uploaded file is empty")

        # Декодируем изображение с помощью OpenCV
        np_arr = np.frombuffer(contents, np.uint8)
        img = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)

        if img is None:
            raise HTTPException(status_code=400, detail="Invalid image format. Please upload JPEG or PNG.")

        # Кодируем изображение в base64 для отправки в API
        _, buffer = cv2.imencode('.png', img)
        b64_image = base64.b64encode(buffer).decode('utf-8')

        # Вызываем функцию детекции курения
        print(f"Отправка фото для детекции курения (размер: {len(contents)} байт)")
        verdict_raw = detect_smoking(b64_image)

        if verdict_raw is None:
            raise HTTPException(status_code=500, detail="Failed to get response from AI model")

        timestamp = time.time()

        # Нормализуем ответ: если в ответе есть "yes" -> "Yes", если "no" -> "No"
        verdict_lower = verdict_raw.strip().lower()
        if "yes" in verdict_lower:
            verdict = "Yes"
        elif "no" in verdict_lower:
            verdict = "No"
        else:
            # Если не нашли ни yes, ни no - возвращаем оригинальный ответ
            verdict = verdict_raw.strip()

        print(f"Детекция завершена. Исходный ответ: '{verdict_raw}' -> Нормализованный: '{verdict}'")

        return {
            "verdict": verdict,
            "timestamp": timestamp
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Ошибка при обработке фото: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=400, detail=f"Error processing image: {str(e)}")

async def process_video_stream_from_url(stream_id: str, url: str, detection_interval: int = 5):
    """
    Фоновая задача для обработки видео потока с URL.

    Args:
        stream_id: ID стрима
        url: URL видео потока
        detection_interval: Интервал детекции курения в секундах
    """
    print(f"[{stream_id}] Запуск обработки видео потока с URL: {url}")

    cap = None
    video_writer = None
    video_path = None
    last_detection_time = 0
    frame_count = 0
    actual_url = url

    try:
        # Если это blob: или веб-страница, пытаемся извлечь реальный HLS URL
        if url.startswith('blob:') or (url.startswith('http') and not url.endswith(('.m3u8', '.ts', '.mp4', '.avi', '.mov'))):
            print(f"[{stream_id}] Обнаружен веб-страничный URL, пытаемся извлечь HLS URL")
            # Убираем blob: префикс если есть
            page_url = url.replace('blob:', '') if url.startswith('blob:') else url
            extracted_url = await asyncio.to_thread(extract_hls_url_from_page, page_url)
            if extracted_url:
                actual_url = extracted_url
            print(f"[{stream_id}] Используем URL: {actual_url}")

        # Открываем видео поток
        print(f"[{stream_id}] Попытка открыть видео поток через OpenCV")
        cap = cv2.VideoCapture(actual_url)

        if not cap.isOpened():
            error_msg = (
                f"Failed to open video stream.\n\n"
                f"Tried URL: {actual_url}\n\n"
                f"Possible reasons:\n"
                f"1. Invalid URL format (supported: HLS .m3u8, RTSP, HTTP video)\n"
                f"2. Stream requires authentication\n"
                f"3. URL is not accessible or blocked\n"
                f"4. OpenCV compiled without support for this protocol\n\n"
                f"For blob: URLs from websites like sochi.camera:\n"
                f"- You need to find the actual .m3u8 URL\n"
                f"- Open DevTools (F12) → Network tab → Filter 'm3u8'\n"
                f"- Copy the real HLS URL and use it\n"
                f"- See HOW_TO_FIND_STREAM_URL.md for detailed instructions"
            )
            print(f"[{stream_id}] ❌ Ошибка: не удалось открыть видео поток")
            print(f"[{stream_id}] Использован URL: {actual_url}")
            print(f"[{stream_id}] ")
            print(f"[{stream_id}] 💡 Для сайтов типа sochi.camera:")
            print(f"[{stream_id}]    1. Откройте DevTools (F12)")
            print(f"[{stream_id}]    2. Network → Фильтр 'm3u8'")
            print(f"[{stream_id}]    3. Найдите и скопируйте .m3u8 URL")
            print(f"[{stream_id}]    4. См. HOW_TO_FIND_STREAM_URL.md")
            stream_sessions[stream_id]["status"] = "error"
            stream_sessions[stream_id]["error"] = error_msg

            # Отправляем сообщение об ошибке через WebSocket
            error_payload = {
                "type": "error",
                "message": "Failed to open video stream",
                "details": error_msg,
                "timestamp": time.time()
            }
            try:
                await manager.broadcast_json(error_payload, stream_id)
            except:
                pass

            return

        print(f"[{stream_id}] Видео поток успешно открыт")
        stream_sessions[stream_id]["status"] = "streaming"

        # Получаем параметры видео
        fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

        print(f"[{stream_id}] Параметры потока: {width}x{height} @ {fps} FPS")

        # Подготавливаем видеописатель для сохранения
        stream_folder = "stream"
        os.makedirs(stream_folder, exist_ok=True)
        video_path = os.path.join(stream_folder, f"{stream_id}.mp4")
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        video_writer = cv2.VideoWriter(video_path, fourcc, fps, (width, height))
        stream_sessions[stream_id]["video_writer"] = video_writer
        stream_sessions[stream_id]["video_path"] = video_path

        # Инициализируем очередь для упорядоченного вывода результатов детекции
        detection_queues[stream_id] = {
            'results': {},  # {frame_number: payload}
            'next_frame': 1,  # Следующий ожидаемый номер кадра
            'lock': asyncio.Lock()
        }

        # Запускаем фоновую задачу для отправки результатов в порядке очереди
        async def send_ordered_results():
            """Отправляет результаты детекции строго по порядку номеров кадров."""
            while stream_sessions.get(stream_id, {}).get("live", False):
                queue_data = detection_queues.get(stream_id)
                if not queue_data:
                    await asyncio.sleep(0.1)
                    continue

                async with queue_data['lock']:
                    next_frame_num = queue_data['next_frame']

                    # Проверяем, есть ли результат для следующего ожидаемого кадра
                    if next_frame_num in queue_data['results']:
                        payload = queue_data['results'].pop(next_frame_num)

                        # Отправляем результат
                        await manager.broadcast_json(payload, stream_id)
                        print(f"[{stream_id}] 📤 Отправлен результат для кадра #{next_frame_num}: {payload['verdict']}")

                        # Переходим к следующему кадру
                        queue_data['next_frame'] += 1
                    else:
                        # Результат еще не готов, ждем
                        await asyncio.sleep(0.01)

                await asyncio.sleep(0.001)

        # Запускаем задачу отправки результатов
        send_task = asyncio.create_task(send_ordered_results())

        # Основной цикл обработки кадров
        while stream_sessions.get(stream_id, {}).get("live", False):
            ret, frame = cap.read()

            if not ret:
                print(f"[{stream_id}] Не удалось прочитать кадр, завершаем обработку")
                break

            frame_count += 1
            current_time = time.time()

            # Сохраняем кадр в видеофайл
            if video_writer is not None:
                video_writer.write(frame)

            # Сохраняем последний кадр для MJPEG потока
            try:
                _, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 85])
                last_frames[stream_id] = buffer.tobytes()
            except Exception as e:
                print(f"[{stream_id}] Ошибка при сохранении кадра для MJPEG: {e}")

            # Детекция курения ТОЛЬКО если есть подключенные WebSocket клиенты
            has_websocket_clients = stream_id in manager.active_connections and len(manager.active_connections[stream_id]) > 0

            if has_websocket_clients and (current_time - last_detection_time >= detection_interval):
                last_detection_time = current_time

                # Сохраняем номер кадра и время ПЕРЕД запуском детекции
                detection_frame_number = frame_count
                detection_timestamp = current_time

                # Копируем кадр для безопасности (чтобы он не изменился во время обработки)
                frame_copy = frame.copy()

                # Запускаем детекцию в фоновой задаче (полностью асинхронно)
                async def run_detection(frame_num: int, timestamp: float, frame_data):
                    try:
                        client_count = len(manager.active_connections.get(stream_id, []))
                        print(f"[{stream_id}] 🔍 Запуск детекции для кадра #{frame_num} ({client_count} клиентов)")

                        # Кодируем кадр в отдельном потоке (не блокирует event loop)
                        def encode_frame():
                            _, buffer = cv2.imencode('.png', frame_data)
                            return base64.b64encode(buffer).decode('utf-8')

                        b64_image = await asyncio.to_thread(encode_frame)

                        # Вызываем детекцию в отдельном потоке
                        verdict_raw = await asyncio.to_thread(detect_smoking, b64_image)

                        if verdict_raw is not None:
                            # Нормализуем ответ
                            verdict_lower = verdict_raw.strip().lower()
                            if "yes" in verdict_lower:
                                verdict = "Yes"
                            elif "no" in verdict_lower:
                                verdict = "No"
                            else:
                                verdict = verdict_raw.strip()

                            # Отправляем результат через WebSocket
                            payload = {
                                "type": "smoking_detection",
                                "timestamp": timestamp,
                                "verdict": verdict,
                                "frame_number": frame_num
                            }
                            await manager.broadcast_json(payload, stream_id)
                            print(f"[{stream_id}] ✅ Результат детекции: {verdict} (кадр #{frame_num})")

                    except Exception as e:
                        print(f"[{stream_id}] ❌ Ошибка при детекции курения: {e}")

                # Запускаем детекцию в фоне (fire-and-forget)
                asyncio.create_task(run_detection(detection_frame_number, detection_timestamp, frame_copy))

            # Минимальная задержка для снижения нагрузки на CPU
            # Не блокируем слишком долго, чтобы видео было плавным
            await asyncio.sleep(0.001)  # 1ms задержка для переключения контекста

            # Проверяем, не закрывается ли стрим
            if stream_sessions.get(stream_id, {}).get("closing", False):
                print(f"[{stream_id}] Стрим закрывается")
                break

        print(f"[{stream_id}] Обработка завершена. Всего обработано кадров: {frame_count}")

    except Exception as e:
        print(f"[{stream_id}] Критическая ошибка при обработке потока: {e}")
        import traceback
        traceback.print_exc()
        stream_sessions[stream_id]["status"] = "error"
        stream_sessions[stream_id]["error"] = str(e)

    finally:
        # Освобождаем ресурсы
        if cap is not None:
            cap.release()
            print(f"[{stream_id}] VideoCapture освобожден")

        if video_writer is not None:
            video_writer.release()
            print(f"[{stream_id}] VideoWriter освобожден, видео сохранено: {video_path}")

        # Обновляем статус
        if stream_id in stream_sessions:
            stream_sessions[stream_id]["live"] = False
            stream_sessions[stream_id]["status"] = "stopped"

@router.post(
    "/open-stream-url",
    response_model=StreamUrlResponse,
    summary="Открыть стрим по URL",
    description="""
    Открывает видео поток по URL и начинает его обработку на сервере с детекцией курения.

    **Поддерживаемые форматы:**
    - HLS (HTTP Live Streaming) - .m3u8
    - RTSP (Real Time Streaming Protocol)
    - HTTP/HTTPS прямые видео потоки
    - Локальные файлы

    **Работа с веб-камерами (sochi.camera и подобными):**

    Если у вас есть URL типа `blob:https://sochi.camera/...`, это временный URL браузера.

    **Способ 1:** Укажите URL страницы с камерой, сервер попытается автоматически извлечь HLS URL:
    ```json
    {
        "url": "https://sochi.camera/ebba653d-2484-4df3-93b7-ca562fdf3f30"
    }
    ```

    **Способ 2 (рекомендуется):** Найдите прямой HLS URL (.m3u8):
    1. Откройте DevTools браузера (F12)
    2. Перейдите на вкладку Network
    3. Найдите запрос к .m3u8 файлу
    4. Скопируйте полный URL и используйте его:
    ```json
    {
        "url": "https://example.com/path/to/stream.m3u8"
    }
    ```

    **Примечание:** blob: URL не поддерживаются напрямую. Сервер попытается автоматически
    найти реальный HLS URL, но это может не всегда работать из-за защиты сайта.

    **Что происходит:**
    1. Сервер создает новый стрим с уникальным ID
    2. Подключается к указанному URL и начинает читать видео поток
    3. Видео сохраняется в файл для последующего просмотра
    4. Последний кадр постоянно обновляется и доступен через MJPEG поток

    **Детекция курения:**
    - Детекция курения НЕ запускается автоматически
    - Она активируется ТОЛЬКО при подключении к WebSocket
    - Каждые N секунд (по умолчанию 5) кадр отправляется на детекцию
    - Результаты передаются через WebSocket всем подключенным клиентам
    - Это экономит ресурсы AI - детекция работает только когда нужна

    **Пример использования (Python):**
    ```python
    import requests

    data = {
        "url": "https://example.com/stream.m3u8",
        "detection_interval": 5
    }

    response = requests.post("http://localhost:8000/stream/open-stream-url", json=data)
    result = response.json()

    print(f"Stream ID: {result['stream_id']}")
    print(f"WebSocket URL: {result['websocket_url']}")
    print(f"Video URL: {result['video_url']}")
    ```

    **Пример использования (cURL):**
    ```bash
    curl -X POST "http://localhost:8000/stream/open-stream-url" \\
         -H "Content-Type: application/json" \\
         -d '{"url": "https://example.com/stream.m3u8", "detection_interval": 5}'
    ```

    **Подключение к результатам через WebSocket (JavaScript):**
    ```javascript
    const streamId = "YOUR_STREAM_ID";
    const ws = new WebSocket(`ws://localhost:8000/ws/stream/${streamId}`);

    ws.onmessage = (event) => {
        const data = JSON.parse(event.data);
        if (data.type === 'smoking_detection') {
            console.log(`Кадр #${data.frame_number}: ${data.verdict}`);
        }
    };
    ```

    **Просмотр видео (БЕЗ детекции):**
    - MJPEG поток: `http://localhost:8000/stream/video/{stream_id}`
    - HTML: `<img src="http://localhost:8000/stream/video/{stream_id}" />`
    - Просто показывает видео без AI анализа

    **Получение результатов детекции (С детекцией):**
    - Подключитесь к WebSocket: `ws://localhost:8000/ws/stream/{stream_id}`
    - Детекция запустится автоматически при подключении
    - Будете получать результаты каждые N секунд

    **Остановка стрима:**
    ```bash
    curl -X POST "http://localhost:8000/stream/close/{stream_id}"
    ```
    """,
    tags=["Stream URL Processing"]
)
async def open_stream_from_url(request: StreamUrlRequest = Body(...)):
    """
    Открывает видео поток по URL и запускает его обработку на сервере.

    Args:
        request: Объект запроса с URL потока и интервалом детекции

    Returns:
        dict: Информация о созданном стриме
    """
    try:
        # Генерируем уникальный ID для стрима
        stream_id = str(uuid.uuid4())

        # Создаем сессию стрима
        stream_sessions[stream_id] = {
            "live": True,
            "closing": False,
            "created_at": time.time(),
            "url": request.url,
            "detection_interval": request.detection_interval,
            "status": "initializing",
            "type": "url_stream"
        }

        print(f"Создан новый стрим по URL: {stream_id}")
        print(f"URL: {request.url}")
        print(f"Интервал детекции: {request.detection_interval} сек")

        # Запускаем фоновую задачу для обработки потока
        asyncio.create_task(process_video_stream_from_url(
            stream_id,
            request.url,
            request.detection_interval
        ))

        return {
            "stream_id": stream_id,
            "url": request.url,
            "status": "initializing",
            "websocket_url": f"/ws/stream/{stream_id}",
            "video_url": f"/stream/video/{stream_id}",
            "message": "Stream is being initialized. Connect to WebSocket to receive detection results."
        }

    except Exception as e:
        print(f"Ошибка при открытии стрима по URL: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=400, detail=f"Failed to open stream: {str(e)}")

@router.post(
    "/request",
    response_model=StreamResponse,
    summary="Создать новую трансляцию",
    description="""
    Создает новую трансляцию и возвращает UUID стрима.
    
    **Важно:**
    - Stream ID и Video ID - это один и тот же UUID
    - Токен сразу готов для стриминга и просмотра
    - Не требуется дополнительный вызов `/stream/start/{token}`
    
    **Процесс работы:**
    1. Вызовите этот эндпоинт для создания стрима
    2. Получите `stream_id` из ответа
    3. Подключитесь к WebSocket по адресу `/ws/stream/{stream_id}`
    4. Начните отправлять видеокадры через WebSocket
    
    **Пример использования:**
    ```python
    import requests
    response = requests.post("http://localhost:8000/stream/request")
    stream_data = response.json()
    stream_id = stream_data["stream_id"]
    websocket_url = stream_data["websocket_url"]
    ```
    """,
    tags=["Streaming"]
)
async def request_stream_token():
    """
    Создает новую трансляцию и возвращает UUID стрима.
    
    Stream ID и Video ID - это один и тот же UUID.
    Токен сразу готов для стриминга и просмотра.
    """
    # Генерируем UUID, который будет служить одновременно stream_token и video_uuid
    stream_id = str(uuid.uuid4())
    
    # Токен сразу активен и готов к использованию
    stream_sessions[stream_id] = {
        "live": True,
        "closing": False,
        "created_at": time.time()
    }
    print(f"Создан новый стрим с ID: {stream_id} (сразу готов)")
    print(f"Stream ID = Video ID = {stream_id}")
    print(f"Всего активных стримов: {len(stream_sessions)}")
    print(f"Все активные stream_ids: {list(stream_sessions.keys())}")
    return {
        "stream_token": stream_id,
        "stream_id": stream_id,
        "video_uuid": stream_id,
        "video_id": stream_id,
        "websocket_url": f"/ws/stream/{stream_id}",
        "status": "active"
    }

@router.post(
    "/broadcast/{token}",
    response_model=BroadcastResponse,
    summary="Отправить данные в стрим",
    description="""
    Отправляет данные всем подключенным пользователям стрима через WebSocket.
    
    **Формат данных:**
    Данные должны быть в формате, который был при генерации:
    ```json
    {
      "frames": [
        {
          "metric": 0.95,
          "cord": [10, 20, 100, 200]
        }
      ]
    }
    ```
    
    **Параметры:**
    - `metric`: float от 0.0 до 1.0 - метрика уверенности
    - `cord`: список из 4 целых чисел [x, y, width, height] - координаты ограничивающего прямоугольника
    
    **Что происходит:**
    1. Данные принимаются через HTTP POST запрос
    2. К данным добавляется временная метка (`time`)
    3. Данные отправляются всем подключенным клиентам через WebSocket
    4. Возвращается количество получателей
    
    **Пример использования:**
    ```python
    import requests
    
   stream_id = "a0a0a0a0-a0a0-a0a0-a0a0-a0a0a0a0a0a0"
   data_to_send = {
       "frames": [
           {
               "metric": 0.95,
               "cord": [10, 20, 100, 200]
           }
       ]
   }
   
   response = requests.post(
       f"http://localhost:8000/stream/broadcast/{stream_id}",
       json=data_to_send
   )
   result = response.json()
   print(f"Данные отправлены {result['recipient_count']} получателям")
    ```
    
    **Ошибки:**
    - `404`: Stream ID не найден
    - `400`: Стрим не активен
    - `500`: Ошибка при отправке данных
    """,
    tags=["Streaming"]
)
async def broadcast_to_stream(
    token: str = Path(..., description="Stream ID стрима для отправки данных"),
    data: BroadcastRequest = Body(..., description="Данные для отправки в формате frames")
):
    """
    Отправляет данные всем подключенным пользователям стрима через WebSocket.
    
    Формат данных должен соответствовать формату, который был при генерации:
    {
        "frames": [
            {
                "metric": 0.95,  # float от 0.0 до 1.0
                "cord": [10, 20, 100, 200]  # список из 4 целых чисел [x, y, width, height]
            }
        ]
    }
    
    Args:
        token: Stream ID стрима для отправки данных
        data: Словарь с данными в формате {"frames": [{"metric": float, "cord": [int, int, int, int]}]}
    
    Returns:
        dict: Статус отправки и количество получателей
    """
    print(f"Запрос на отправку данных в стрим {token}")
    
    # Проверяем, что стрим существует
    if token not in stream_sessions:
        raise HTTPException(status_code=404, detail=f"Stream ID {token} not found")
    
    # Проверяем, что стрим активен
    if not stream_sessions[token].get("live", False):
        raise HTTPException(status_code=400, detail=f"Stream {token} is not active")
    
    # Отправляем данные всем подключенным клиентам
    try:
        # Формируем payload в том же формате, что был при генерации
        frames_data = [{"metric": f.metric, "cord": f.cord} for f in data.frames]
        payload = {
            "time": time.time(),  # Временная метка обработанного кадра
            "frames": frames_data  # Массив объектов с metric и cord
        }
        
        # Транслируем данные через менеджер соединений
        await manager.broadcast_json(payload, token)
        
        # Подсчитываем количество получателей
        recipient_count = len(manager.active_connections.get(token, []))
        
        print(f"Данные отправлены в стрим {token} для {recipient_count} получателей")
        print(f"Формат данных: {payload}")
        
        return {
            "message": "Data broadcasted successfully",
            "stream_id": token,
            "recipient_count": recipient_count,
            "data": payload
        }
    except Exception as e:
        print(f"Ошибка при отправке данных в стрим {token}: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to broadcast data: {str(e)}")

@router.post(
    "/close/{token}",
    response_model=CloseStreamResponse,
    summary="Закрыть трансляцию",
    description="""
    Закрывает трансляцию и дожидается завершения обработки всех кадров.
    
    **Процесс закрытия:**
    1. Помечает стрим как закрывающийся (`closing: True`)
    2. Дожидается завершения обработки всех кадров (2 секунды)
    3. Освобождает ресурсы (видеописатель, окна отображения)
    4. Помечает стрим как закрытый (`live: False`)
    
    **Важно:**
    - Этот эндпоинт дожидается завершения обработки всех кадров перед закрытием
    - Это предотвращает потерю данных при раннем закрытии WebSocket
    - После закрытия стрим нельзя использовать повторно (нужно создать новый)
    
    **Пример использования:**
    ```python
    import requests
    
   stream_id = "a0a0a0a0-a0a0-a0a0-a0a0-a0a0a0a0a0a0"
   response = requests.post(f"http://localhost:8000/stream/close/{stream_id}")
   result = response.json()
   print(f"Стрим закрыт: {result['status']}")
    ```
    
    **Ошибки:**
    - `404`: Stream ID не найден
    """,
    tags=["Streaming"]
)
async def close_stream(
    token: str = Path(..., description="Stream ID стрима для закрытия")
):
    """
    Закрывает трансляцию и дожидается завершения обработки всех кадров.
    
    Этот эндпоинт:
    1. Помечает стрим как закрывающийся (closing)
    2. Дожидается завершения обработки всех кадров в очереди
    3. Освобождает ресурсы (видеописатель, окна и т.д.)
    4. Помечает стрим как закрытый
    
    Args:
        token: Stream ID стрима для закрытия
    
    Returns:
        dict: Статус закрытия стрима
    """
    print(f"Запрос на закрытие стрима: {token}")
    
    # Проверяем, что стрим существует
    if token not in stream_sessions:
        raise HTTPException(status_code=404, detail=f"Stream ID {token} not found")
    
    session = stream_sessions[token]
    
    # Если стрим уже закрывается или закрыт
    if session.get("closing", False):
        return {
            "message": "Stream is already closing",
            "stream_id": token,
            "status": "closing"
        }
    
    if not session.get("live", False):
        return {
            "message": "Stream is already closed",
            "stream_id": token,
            "status": "closed"
        }
    
    # Помечаем стрим как закрывающийся
    session["closing"] = True
    print(f"Стрим {token} помечен как закрывающийся")
    
    # Ждем завершения обработки (даем время на обработку оставшихся кадров)
    # Обычно достаточно 1-2 секунд для завершения обработки
    await asyncio.sleep(2)
    
    # Освобождаем ресурсы видеописателя, если он был создан
    if "video_writer" in session and session["video_writer"] is not None:
        try:
            video_writer = session["video_writer"]
            video_writer.release()
            print(f"Видеописатель для стрима {token} освобожден")
        except Exception as e:
            print(f"Ошибка при освобождении видеописателя для стрима {token}: {e}")
    
    # Закрываем окно отображения, если оно было открыто
    if "display_window_name" in session and session["display_window_name"]:
        try:
            if ENABLE_VIDEO_DISPLAY:
                cv2.destroyWindow(session["display_window_name"])
                print(f"Окно отображения для стрима {token} закрыто")
        except Exception as e:
            print(f"Ошибка при закрытии окна для стрима {token}: {e}")
    
    # Помечаем стрим как закрытый
    session["live"] = False
    session["closing"] = False
    session["closed_at"] = time.time()
    
    print(f"Стрим {token} успешно закрыт")
    print(f"Активных стримов: {len([s for s in stream_sessions.values() if s.get('live', False)])}")
    
    return {
        "message": "Stream closed successfully",
        "stream_id": token,
        "status": "closed",
        "closed_at": session["closed_at"]
    }

@router.post("/stream/start/{token}")
async def start_stream(token: str):
    """
    Запускает сессию стрима для данного токена.
    ПРИМЕЧАНИЕ: Этот эндпоинт теперь опционален - токены готовы сразу после создания.
    Сохранен для обратной совместимости.
    """
    print(f"Запуск стрима для токена: {token}")
    print(f"Доступные токены перед запуском: {list(stream_sessions.keys())}")
    
    if token not in stream_sessions:
        print(f"ОШИБКА: Токен {token} не найден")
        raise HTTPException(status_code=404, detail="Stream token not found")

    # Если уже активен, просто возвращаем успех
    if stream_sessions[token].get("live", False):
        print(f"ИНФО: Стрим {token} уже активен")
        return {
            "message": "Stream already active", 
            "stream_id": token,
            "video_uuid": token,
            "video_id": token
        }
    
    stream_sessions[token]["live"] = True
    
    print(f"Стрим {token} успешно запущен")
    print(f"Состояние сессии стрима: {stream_sessions[token]}")

    return {
        "message": "Stream started successfully", 
        "stream_id": token,
        "video_uuid": token,
        "video_id": token
    }

@router.get(
    "/video/{token}",
    summary="Получить прямой видеопоток (MJPEG)",
    description="""
    Получает прямой видеопоток от стрима в формате MJPEG.
    
    **Формат:**
    Возвращает непрерывный поток JPEG кадров в формате `multipart/x-mixed-replace`.
    
    **Использование:**
    - В HTML: `<img src="http://localhost:8000/stream/video/{stream_id}" />`
    - В OpenCV: `cap = cv2.VideoCapture(f"http://localhost:8000/stream/video/{stream_id}")`
    - В браузере: можно использовать напрямую в теге `<img>`
    
    **Как это работает:**
    1. Эндпоинт возвращает последний сохраненный кадр стрима
    2. Кадры обновляются по мере поступления через WebSocket
    3. Формат MJPEG позволяет непрерывно отображать видео в браузере
    
    **Пример использования:**
    ```html
    <img src="http://localhost:8000/stream/video/a0a0a0a0-a0a0-a0a0-a0a0-a0a0a0a0a0a0" alt="Video Stream" />
    ```
    
    ```python
    import cv2
    
    stream_id = "a0a0a0a0-a0a0-a0a0-a0a0-a0a0a0a0a0a0"
    url = f"http://localhost:8000/stream/video/{stream_id}"
    cap = cv2.VideoCapture(url)
    
    while True:
        ret, frame = cap.read()
        if not ret:
            break
        cv2.imshow('Stream', frame)
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break
    
    cap.release()
    cv2.destroyAllWindows()
    ```
    
    **Ошибки:**
    - `404`: Stream ID не найден
    """,
    tags=["Streaming"]
)
async def get_video_stream(
    token: str = Path(..., description="Stream ID стрима для получения видеопотока")
):
    """
    Получает прямой видеопоток от стрима в формате MJPEG.
    
    Этот эндпоинт возвращает непрерывный поток JPEG кадров в формате multipart/x-mixed-replace.
    Можно использовать в HTML теге <img> или видеоплеерах, поддерживающих MJPEG.
    
    Args:
        token: Stream ID стрима для получения видеопотока
    
    Returns:
        StreamingResponse с MJPEG видеопотоком
    """
    # Проверяем, что стрим существует
    if token not in stream_sessions:
        raise HTTPException(status_code=404, detail=f"Stream ID {token} not found")
    
    async def generate_frames():
        """
        Генератор для создания MJPEG потока.
        Отправляет последний кадр стрима в бесконечном цикле.
        """
        while True:
            # Проверяем, что стрим все еще активен
            if token not in stream_sessions:
                break
            
            # Получаем последний кадр для этого стрима
            if token in last_frames:
                frame_bytes = last_frames[token]
                # Формируем MJPEG фрейм
                yield (b'--frame\r\n'
                       b'Content-Type: image/jpeg\r\n\r\n' + frame_bytes + b'\r\n')
            else:
                # Если кадров еще нет, ждем немного
                await asyncio.sleep(0.1)
                continue
            
            # Задержка между кадрами (примерно 10 FPS)
            await asyncio.sleep(0.1)
    
    return StreamingResponse(
        generate_frames(),
        media_type="multipart/x-mixed-replace; boundary=frame"
    )

@websocket_router.websocket("/ws/stream/{token}")
async def websocket_endpoint(websocket: WebSocket, token: str):
    """
    WebSocket эндпоинт для стриминга видеоданных.
    
    **ВАЖНО:** WebSocket эндпоинты не отображаются в Swagger UI напрямую.
    Используйте этот эндпоинт для подключения к стриму и отправки видеокадров.
    
    **Процесс работы:**
    1. Создайте стрим через `/stream/request` и получите `stream_id`
    2. Подключитесь к WebSocket по адресу `/ws/stream/{stream_id}`
    3. Отправляйте видеокадры в формате base64 data URI
    4. Получайте обработанные данные через WebSocket (если отправлены через `/stream/broadcast/{token}`)
    
    **Формат отправки кадров:**
    Кадры должны быть отправлены как base64-закодированные data URI:
    ```
    data:image/jpeg;base64,/9j/4AAQSkZJRg...
    ```
    
    **Что происходит на сервере:**
    1. Принимает видеокадры от клиентов (как base64-закодированные изображения)
    2. Декодирует и обрабатывает каждый кадр
    3. Сохраняет кадры в MP4 файл с именем `{stream_id}.mp4`
    4. Сохраняет последний кадр для MJPEG потока (`/stream/video/{token}`)
    5. Отображает видеопоток в реальном времени (если `ENABLE_VIDEO_DISPLAY = True`)
    
    **Пример использования (JavaScript):**
    ```javascript
    const streamId = "24db39b8-f8d0-440b-ba74-4c75b902e478";
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}/ws/stream/${streamId}`;
    const socket = new WebSocket(wsUrl);
    
    socket.onopen = () => {
        console.log('WebSocket connected');
        // Отправка кадра
        const canvas = document.getElementById('canvas');
        const frame = canvas.toDataURL('image/jpeg', 0.7);
        socket.send(frame);
    };
    
    socket.onmessage = (event) => {
        const data = JSON.parse(event.data);
        console.log("Получены данные:", data);
        // data содержит: { time: 1234567890.123, frames: [...] }
    };
    
    socket.onerror = (error) => {
        console.error('WebSocket error:', error);
    };
    
    socket.onclose = (event) => {
        console.log('WebSocket closed:', event.code, event.reason);
    };
    ```
    
    **Пример использования (Python):**
    ```python
    import asyncio
    import websockets
    import base64
    import cv2
    
    async def stream_video():
        stream_id = "24db39b8-f8d0-440b-ba74-4c75b902e478"
        uri = f"ws://localhost:8000/ws/stream/{stream_id}"
        
        async with websockets.connect(uri) as websocket:
            # Открыть камеру
            cap = cv2.VideoCapture(0)
            
            while True:
                ret, frame = cap.read()
                if not ret:
                    break
                
                # Кодировать кадр в base64
                _, buffer = cv2.imencode('.jpg', frame)
                img_base64 = base64.b64encode(buffer).decode('utf-8')
                data_uri = f"data:image/jpeg;base64,{img_base64}"
                
                # Отправить кадр
                await websocket.send(data_uri)
                
                # Получить ответ (если есть)
                try:
                    response = await asyncio.wait_for(websocket.recv(), timeout=0.1)
                    data = json.loads(response)
                    print("Получены данные:", data)
                except asyncio.TimeoutError:
                    pass
                
                await asyncio.sleep(0.033)  # ~30 FPS
            
            cap.release()
    
    asyncio.run(stream_video())
    ```
    
    **Коды ошибок WebSocket:**
    - `4000`: Stream ID не найден в stream_sessions
    - `1006`: Аномальное закрытие соединения (проверьте сервер)
    
    **Примечания:**
    - Если стрим не существует, он будет автоматически создан при подключении
    - Разрешение видео берется из кадра камеры пользователя
    - Частота кадров: 30 FPS
    - Видео сохраняется в файл `{stream_id}.mp4` в корне проекта
    
    Args:
        websocket: Объект WebSocket соединения
        token: Stream ID (UUID), который идентифицирует сессию стрима
    """
    # Логируем детали подключения для отладки
    client_host = websocket.client.host if websocket.client else "unknown"
    print(f"Попытка WebSocket подключения от {client_host} для stream_id: {token}")
    print(f"Заголовки: {dict(websocket.headers)}")
    print(f"Доступные stream_ids: {list(stream_sessions.keys())}")
    
    try:
        # Принимаем WebSocket соединение первым делом (требуется перед любым общением)
        # Это работает с ngrok и другими обратными прокси
        await websocket.accept()
        print(f"WebSocket соединение принято для stream_id: {token}")
    except Exception as e:
        print(f"ОШИБКА при принятии WebSocket соединения: {e}")
        return
    
    # Проверяем, что токен стрима существует и активен
    if token not in stream_sessions or not stream_sessions[token].get("live", False):
        # Если стрим не найден или не активен, закрываем соединение
        reason = f"Stream {token} not found or has been closed."
        print(f"ОТКЛОНЕНО: {reason}")
        await websocket.close(code=4000, reason=reason)
        return
    
    # Убеждаемся, что стрим помечен как активный (автоактивация при необходимости)
    if not stream_sessions[token].get("live", False):
        stream_sessions[token]["live"] = True
        print(f"Автоактивация стрима для stream_id {token}")
    
    print(f"Stream ID {token} успешно проверен, готов для стриминга/просмотра")
    print(f"Stream ID = Video ID = {token}")

    # Добавляем это соединение в менеджер соединений для трансляции
    await manager.connect(websocket, token)
    
    # Инициализируем видеописатель для сохранения кадров в файл
    video_writer = None
    video_path = None
    
    # Имя окна для отображения видео (будет создано для каждого стрима)
    display_window_name = f"Stream: {token[:8]}..." if ENABLE_VIDEO_DISPLAY else None
    
    # Сохраняем ссылки на ресурсы в сессии для возможности закрытия извне
    stream_sessions[token]["video_writer"] = None  # Будет установлен позже
    stream_sessions[token]["display_window_name"] = display_window_name

    try:
        # Главный цикл: непрерывно принимаем и обрабатываем видеокадры
        frame_count = 0
        last_checked = time.time()
        while True:
            try:
                # Принимаем видеокадр от клиента как base64-закодированный data URI
                # Формат: "data:image/jpeg;base64,/9j/4AAQSkZJRg..."
                data = await websocket.receive_text()
                frame_count += 1
                
                if frame_count == 1:
                    print(f"Получен первый кадр для стрима {token}")
                
                # Декодируем base64 данные изображения
                try:
                    # Разделяем data URI, чтобы получить base64 часть
                    if "," not in data:
                        print(f"ОШИБКА: Неверный формат data URI для кадра {frame_count}: отсутствует запятая")
                        continue
                    
                    header, encoded = data.split(",", 1)
                    # Декодируем base64 в бинарные данные изображения
                    img_data = base64.b64decode(encoded)
                    # Преобразуем бинарные данные в numpy массив
                    np_arr = np.frombuffer(img_data, np.uint8)
                    # Декодируем изображение с помощью OpenCV
                    frame = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
                    
                    # Проверяем, успешно ли декодирован кадр
                    if frame is None:
                        print(f"Не удалось декодировать кадр {frame_count}: неверные данные изображения")
                        continue
                except Exception as e:
                    print(f"ОШИБКА при декодировании кадра {frame_count}: {e}")
                    import traceback
                    traceback.print_exc()
                    continue

                current_time = time.time()
                if current_time - last_checked >= 5:
                    last_checked = current_time
                    print(f"Preparing to send frame to OpenAI for stream {token}")
                    _, buffer = cv2.imencode('.png', frame)
                    b64_image = base64.b64encode(buffer).decode('utf-8')

                    # Offload the OpenAI API call to a separate thread to avoid blocking the event loop
                    verdict_raw = await asyncio.to_thread(detect_smoking, b64_image)

                    if verdict_raw is not None:
                        # Нормализуем ответ: если в ответе есть "yes" -> "Yes", если "no" -> "No"
                        verdict_lower = verdict_raw.strip().lower()
                        if "yes" in verdict_lower:
                            verdict = "Yes"
                        elif "no" in verdict_lower:
                            verdict = "No"
                        else:
                            verdict = verdict_raw.strip()

                        payload = {
                            "type": "smoking_detection",
                            "timestamp": current_time,
                            "verdict": verdict
                        }
                        await manager.broadcast_json(payload, token)
                        print(f"Broadcasted smoking detection verdict for stream {token}: {verdict} (raw: {verdict_raw})")

                # Проверяем, не закрывается ли стрим
                if stream_sessions.get(token, {}).get("closing", False):
                    print(f"Стрим {token} закрывается, прекращаем обработку новых кадров")
                    break
                
                # Инициализируем видеописатель на первом кадре
                if video_writer is None:
                    try:
                        # Убедимся, что папка для сохранения стримов существует
                        stream_folder = "stream"
                        os.makedirs(stream_folder, exist_ok=True)
                        
                        # Stream ID совпадает с Video ID (единый идентификатор)
                        video_id = token
                        video_path = os.path.join(stream_folder, f"{video_id}.mp4")
                        height, width, _ = frame.shape
                        
                        # Используем кодек MP4V для кодирования видео
                        fourcc = cv2.VideoWriter_fourcc(*'mp4v')
                        # Частота кадров: 30 FPS (как у камеры пользователя)
                        fps = 30.0
                        video_writer = cv2.VideoWriter(video_path, fourcc, fps, (width, height))
                        # Сохраняем ссылку на видеописатель в сессии
                        if token in stream_sessions:
                            stream_sessions[token]["video_writer"] = video_writer
                            stream_sessions[token]["video_path"] = video_path
                        print(f"Начало записи стрима {token} в {video_path}")
                        print(f"Разрешение видео: {width}x{height}, FPS: {fps}")
                        print(f"Stream ID = Video ID = {token}")
                    except Exception as e:
                        print(f"ОШИБКА при инициализации видеописателя: {e}")
                        import traceback
                        traceback.print_exc()
                        continue

                # Отображаем видеокадр в реальном времени (если включено)
                # Это показывает видеопоток в окне - полезно для мониторинга
                # Примечание: Требуется GUI окружение. Установите ENABLE_VIDEO_DISPLAY = False для серверов без GUI
                if ENABLE_VIDEO_DISPLAY:
                    try:
                        # Отображаем кадр в окне
                        # Имя окна включает Stream ID для идентификации
                        cv2.imshow(display_window_name, frame)
                        # Обрабатываем события окна OpenCV (требуется для работы imshow)
                        # Используем waitKey с таймаутом 1мс, чтобы не блокировать async цикл
                        cv2.waitKey(1)
                    except Exception as e:
                        # Если отображение не удалось (например, нет GUI), логируем предупреждение, но продолжаем
                        # Отображение будет попытаться снова на следующем кадре
                        print(f"Предупреждение: Не удалось отобразить видеокадр {frame_count}: {e}")
                        if frame_count == 1:
                            print("Совет: Установите ENABLE_VIDEO_DISPLAY = False при работе без GUI")

                # Записываем кадр в видеофайл для последующего воспроизведения/анализа
                try:
                    if video_writer is not None:
                        video_writer.write(frame)
                except Exception as e:
                    print(f"ОШИБКА при записи кадра {frame_count} в видеофайл: {e}")
                    import traceback
                    traceback.print_exc()

                # Сохраняем последний кадр для MJPEG потока
                # Кодируем кадр в JPEG формат для передачи через HTTP
                try:
                    _, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 85])
                    last_frames[token] = buffer.tobytes()
                except Exception as e:
                    print(f"Предупреждение: Не удалось сохранить кадр {frame_count} для MJPEG потока: {e}")

                # Отправляем подтверждение получения кадра (опционально)
                # Если нужно отправлять данные о каждом кадре, используйте эндпоинт /stream/broadcast/{token}
                # Здесь просто обрабатываем кадр без автоматической отправки данных
                    
            except WebSocketDisconnect:
                # Это нормальное отключение клиента
                raise
            except Exception as e:
                # Логируем любые другие ошибки, но продолжаем работу
                print(f"ОШИБКА при обработке кадра {frame_count} для стрима {token}: {e}")
                import traceback
                traceback.print_exc()
                # Продолжаем цикл, чтобы не разрывать соединение из-за ошибки в одном кадре
                continue
            
    except WebSocketDisconnect:
        # Клиент отключился - завершаем обработку и очищаем ресурсы
        print(f"WebSocket отключен для стрима {token}.")
        
        # Удаляем соединение из менеджера
        manager.disconnect(websocket, token)
        
        # Проверяем, остались ли еще соединения для этого стрима
        if not manager.active_connections.get(token):
            print(f"Последний клиент отключился от стрима {token}. Закрываем стрим.")
            
            # Освобождаем ресурсы видеописателя
            if video_writer is not None:
                try:
                    video_writer.release()
                    if video_path:
                        print(f"Стрим {token} сохранен в {video_path}")
                except Exception as e:
                    print(f"Ошибка при освобождении видеописателя для стрима {token}: {e}")
            
            # Закрываем окно отображения видео, если оно было открыто
            if ENABLE_VIDEO_DISPLAY and display_window_name:
                try:
                    cv2.destroyWindow(display_window_name)
                except Exception as e:
                    print(f"Ошибка при закрытии окна для стрима {token}: {e}")
            
            # Удаляем сессию стрима, чтобы он не был доступен для переподключения
            if token in stream_sessions:
                del stream_sessions[token]
                print(f"Сессия стрима {token} удалена.")
            
            # Удаляем последний кадр
            if token in last_frames:
                del last_frames[token]
            
            print(f"Стрим {token} полностью закрыт и очищен.")
        else:
            active_count = len(manager.active_connections.get(token, []))
            print(f"Клиент отключился, но для стрима {token} еще есть {active_count} активных соединений.")
            
        print(f"Текущие активные стримы: {list(stream_sessions.keys())}")


# ============================================================================
# ПРИМЕР ИСПОЛЬЗОВАНИЯ API
# ============================================================================
"""
Примеры использования API для работы со стримами:

1. СОЗДАНИЕ ТРАНСЛЯЦИИ (получение UUID):
   POST /stream/request
   
   Ответ:
   {
     "stream_token": "a0a0a0a0-a0a0-a0a0-a0a0-a0a0a0a0a0a0",
     "stream_id": "a0a0a0a0-a0a0-a0a0-a0a0-a0a0a0a0a0a0",
     "video_uuid": "a0a0a0a0-a0a0-a0a0-a0a0-a0a0a0a0a0a0",
     "video_id": "a0a0a0a0-a0a0-a0a0-a0a0-a0a0a0a0a0a0",
     "websocket_url": "/ws/stream/a0a0a0a0-a0a0-a0a0-a0a0-a0a0a0a0a0a0",
     "status": "active"
   }
   
   # Пример использования:
   import requests
   response = requests.post("http://localhost:8000/stream/request")
   stream_data = response.json()
   stream_id = stream_data["stream_id"]  # UUID стрима
   print(f"Создан стрим с ID: {stream_id}")

2. ОТПРАВКА ДАННЫХ В СТРИМ:
   POST /stream/broadcast/{token}
   
   Отправляет данные всем подключенным пользователям стрима через WebSocket.
   
   Формат данных (как было при генерации):
   Тело запроса (JSON):
   {
     "frames": [
       {
         "metric": 0.95,  # float от 0.0 до 1.0
         "cord": [10, 20, 100, 200]  # список из 4 целых чисел [x, y, width, height]
       },
       {
         "metric": 0.87,
         "cord": [50, 60, 150, 250]
       }
     ]
   }
   
   Ответ:
   {
     "message": "Data broadcasted successfully",
     "stream_id": "a0a0a0a0-a0a0-a0a0-a0a0-a0a0a0a0a0a0",
     "recipient_count": 2,
     "data": {
       "time": 1234567890.123,
       "frames": [
         {
           "metric": 0.95,
           "cord": [10, 20, 100, 200]
         }
       ]
     }
   }
   
   # Пример использования:
   import requests
   
   stream_id = "a0a0a0a0-a0a0-a0a0-a0a0-a0a0a0a0a0a0"
   data_to_send = {
       "frames": [
           {
               "metric": 0.95,
               "cord": [10, 20, 100, 200]
           },
           {
               "metric": 0.87,
               "cord": [50, 60, 150, 250]
           }
       ]
   }
   
   response = requests.post(
       f"http://localhost:8000/stream/broadcast/{stream_id}",
       json=data_to_send
   )
   result = response.json()
   print(f"Данные отправлены {result['recipient_count']} получателям")

3. ЗАКРЫТИЕ ТРАНСЛЯЦИИ:
   POST /stream/close/{token}
   
   Этот эндпоинт:
   - Помечает стрим как закрывающийся
   - Дожидается завершения обработки всех кадров (2 секунды)
   - Освобождает ресурсы (видеописатель, окна)
   - Помечает стрим как закрытый
   
   Ответ:
   {
     "message": "Stream closed successfully",
     "stream_id": "a0a0a0a0-a0a0-a0a0-a0a0-a0a0a0a0a0a0",
     "status": "closed",
     "closed_at": 1234567890.123
   }
   
   # Пример использования:
   import requests
   stream_id = "a0a0a0a0-a0a0-a0a0-a0a0-a0a0a0a0a0a0"
   response = requests.post(f"http://localhost:8000/stream/close/{stream_id}")
   result = response.json()
   print(f"Стрим закрыт: {result['status']}")
   # Важно: Этот эндпоинт дожидается завершения обработки всех кадров
   # перед закрытием, чтобы не потерять данные при раннем закрытии WebSocket

4. ПОЛУЧЕНИЕ СПИСКА АКТИВНЫХ СТРИМОВ:
   GET /stream/list
   
   Ответ:
   {
     "active_streams": [
       "a0a0a0a0-a0a0-a0a0-a0a0-a0a0a0a0a0a0",
       "0a0a0a0a-0a0a-0a0a-0a0a-0a0a0a0a0a0a"
     ],
     "count": 2
   }

5. ПОДКЛЮЧЕНИЕ К СТРИМУ ЧЕРЕЗ WEBSOCKET (JavaScript):
   const streamId = "a0a0a0a0-a0a0-a0a0-a0a0-a0a0a0a0a0a0";
   const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
   const wsUrl = `${protocol}//${window.location.host}/ws/stream/${streamId}`;
   const socket = new WebSocket(wsUrl);
   
   // Отправка видеокадра (base64 data URI)
   socket.send("data:image/jpeg;base64,/9j/4AAQSkZJRg...");
   
   // Получение обработанных данных
   socket.onmessage = (event) => {
     const data = JSON.parse(event.data);
     console.log("Обработанные данные:", data);
   };

6. ИСПОЛЬЗОВАНИЕ ВНУТРЕННЕЙ ФУНКЦИИ (Python):
   from routers.streaming import get_active_streams
   
   # Получить список активных стримов
   active_ids = get_active_streams()
   print(f"Активные стримы: {active_ids}")

7. ПОЛУЧЕНИЕ ПРЯМОГО ВИДЕОПОТОКА (MJPEG):
   # Метод 1: Использование в HTML
   <img src="http://localhost:8000/stream/video/{stream_id}" alt="Video Stream" />
   
   # Метод 2: Использование в JavaScript
   const streamId = "a0a0a0a0-a0a0-a0a0-a0a0-a0a0a0a0a0a0";
   const videoUrl = `http://localhost:8000/stream/video/${streamId}`;
   const img = document.createElement('img');
   img.src = videoUrl;
   document.body.appendChild(img);
   
   # Метод 3: Использование в Python (requests)
   import requests
   
   stream_id = "a0a0a0a0-a0a0-a0a0-a0a0-a0a0a0a0a0a0"
   response = requests.get(
       f"http://localhost:8000/stream/video/{stream_id}",
       stream=True
   )
   
   # Сохранение кадров из потока
   for chunk in response.iter_content(chunk_size=1024):
       if chunk:
           # Обработка JPEG кадров из MJPEG потока
           # Каждый chunk может содержать один или несколько кадров
           pass
   
   # Метод 4: Использование OpenCV для получения кадров
   import cv2
   import requests
   import numpy as np
   from io import BytesIO
   
   stream_id = "a0a0a0a0-a0a0-a0a0-a0a0-a0a0a0a0a0a0"
   url = f"http://localhost:8000/stream/video/{stream_id}"
   
   # Открываем MJPEG поток
   cap = cv2.VideoCapture(url)
   
   while True:
       ret, frame = cap.read()
       if not ret:
           break
       
       # Обработка кадра
       cv2.imshow('Stream', frame)
       if cv2.waitKey(1) & 0xFF == ord('q'):
           break
   
   cap.release()
   cv2.destroyAllWindows()

8. ПРИМЕР ПОЛНОГО ЦИКЛА РАБОТЫ:
   import requests
   
   # Шаг 1: Создать трансляцию (получить UUID)
   response = requests.post("http://localhost:8000/stream/request")
   stream_data = response.json()
   stream_id = stream_data["stream_id"]  # UUID стрима
   print(f"Создан стрим с ID: {stream_id}")
   
   # Шаг 2: Подключиться к WebSocket и отправлять кадры
   # (см. пример выше для JavaScript)
   
   # Шаг 3: Получить прямой видеопоток через HTTP
   # В браузере: <img src="http://localhost:8000/stream/video/{stream_id}" />
   # Или через OpenCV: cap = cv2.VideoCapture(f"http://localhost:8000/stream/video/{stream_id}")
   
   # Шаг 4: Проверить активные стримы
   response = requests.get("http://localhost:8000/stream/list")
   active_streams = response.json()["active_streams"]
   print(f"Активные стримы: {active_streams}")
   
   # Шаг 5: Отправить данные в стрим всем подключенным пользователям
   data_to_send = {
       "frames": [
           {
               "metric": 0.95,
               "cord": [10, 20, 100, 200]
           },
           {
               "metric": 0.87,
               "cord": [50, 60, 150, 250]
           }
       ]
   }
   response = requests.post(
       f"http://localhost:8000/stream/broadcast/{stream_id}",
       json=data_to_send
   )
   result = response.json()
   print(f"Данные отправлены {result['recipient_count']} получателям")
   
   # Шаг 6: Закрыть трансляцию (дожидается завершения обработки всех кадров)
   response = requests.post(f"http://localhost:8000/stream/close/{stream_id}")
   result = response.json()
   print(f"Стрим закрыт: {result['status']}")
   # Важно: Этот эндпоинт дожидается завершения обработки всех кадров
   # перед закрытием, чтобы не потерять данные при раннем закрытии WebSocket
   
   # Шаг 7: Видео будет сохранено в файл {stream_id}.mp4
   # и отображаться в окне OpenCV (если ENABLE_VIDEO_DISPLAY = True)
   # Также доступно через HTTP MJPEG поток по адресу /stream/video/{stream_id}
"""