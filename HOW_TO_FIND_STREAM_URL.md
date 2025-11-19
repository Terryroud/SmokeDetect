# Как найти правильный URL видео потока

## Проблема

Сайты типа **sochi.camera** используют `blob:` URL, которые работают только в браузере и не могут быть открыты напрямую через OpenCV.

## Решение: Найти реальный HLS URL (.m3u8)

### Шаг 1: Откройте Developer Tools

1. Откройте страницу с камерой в браузере (Chrome/Firefox/Edge)
2. Нажмите **F12** или **Ctrl+Shift+I** (Windows) / **Cmd+Option+I** (Mac)
3. Перейдите на вкладку **Network**

### Шаг 2: Фильтруйте запросы

1. В поле фильтра введите: **m3u8**
2. Обновите страницу (**F5**) или включите/выключите видео
3. Подождите несколько секунд

### Шаг 3: Найдите .m3u8 файл

Вы увидите запросы к файлам типа:
```
playlist.m3u8
stream.m3u8
master.m3u8
index.m3u8
```

### Шаг 4: Скопируйте полный URL

1. Нажмите на запрос правой кнопкой мыши
2. Выберите **Copy** → **Copy URL** (или **Copy as cURL**)
3. URL должен выглядеть примерно так:
```
https://cdn.example.com/streams/camera123/playlist.m3u8
https://stream-server.com/hls/camera_id/index.m3u8?token=abc123
```

### Шаг 5: Используйте найденный URL

```bash
curl -X POST "http://localhost:8000/stream/open-stream-url" \
     -H "Content-Type: application/json" \
     -d '{"url": "https://cdn.example.com/streams/camera123/playlist.m3u8"}'
```

Или через Python:
```python
import requests

data = {
    "url": "https://cdn.example.com/streams/camera123/playlist.m3u8",
    "detection_interval": 5
}

response = requests.post("http://localhost:8000/stream/open-stream-url", json=data)
print(response.json())
```

## Пример для sochi.camera

### Что вы видите в браузере:
```
blob:https://sochi.camera/bdeb1626-84ac-4f39-8609-557b335ac9da
```

### Что нужно найти:
В Network → Filter "m3u8" вы должны увидеть что-то вроде:
```
https://sochi-streams.cdnvideo.ru/sochi/camera_123/playlist.m3u8
https://cdn.sochi.camera/hls/bdeb1626-84ac-4f39-8609-557b335ac9da/index.m3u8
```

**Используйте именно этот URL!**

## Альтернатива: Используйте браузерные расширения

Если вы не можете найти .m3u8 URL вручную, попробуйте расширения:

### Chrome/Edge:
- **Video DownloadHelper**
- **Stream Detector**
- **Video Downloader Plus**

### Firefox:
- **Video DownloadHelper**
- **HLS Downloader**

Эти расширения автоматически определяют HLS потоки на странице.

## Проверка URL

Перед отправкой на сервер, проверьте URL:

### Метод 1: VLC Player
1. Откройте VLC
2. Media → Open Network Stream
3. Вставьте URL
4. Если видео воспроизводится - URL правильный

### Метод 2: FFmpeg
```bash
ffmpeg -i "https://your-stream-url.m3u8" -t 5 test.mp4
```

Если команда работает - URL правильный.

### Метод 3: curl
```bash
curl -I "https://your-stream-url.m3u8"
```

Должен вернуть `200 OK` или `302 Redirect`.

## Типичные ошибки

### ❌ Неправильно:
```
blob:https://sochi.camera/bdeb1626-84ac-4f39-8609-557b335ac9da
https://sochi.camera/bdeb1626-84ac-4f39-8609-557b335ac9da
```

### ✅ Правильно:
```
https://cdn.sochi.camera/hls/bdeb1626-84ac-4f39-8609-557b335ac9da/playlist.m3u8
rtsp://camera-server.com:554/stream
https://stream-cdn.com/live/camera123.m3u8
```

## Поддержка

Если вы все еще не можете найти URL:

1. Проверьте документацию вашего сервиса камер
2. Обратитесь в поддержку сервиса
3. Некоторые камеры предоставляют API для получения URL потоков

## Поддерживаемые форматы

✅ **HLS** (.m3u8) - HTTP Live Streaming
✅ **RTSP** - Real Time Streaming Protocol
✅ **HTTP/HTTPS** - Прямые видео файлы (.mp4, .avi, .mov)
❌ **blob:** - Временные URL браузера (не поддерживаются)
❌ **file://** - Локальные файлы браузера (не поддерживаются)
