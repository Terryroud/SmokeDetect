"""
Тестовый скрипт для проверки эндпоинта открытия стрима по URL
"""
import requests
import json
import time

def test_open_stream_url(stream_url: str, detection_interval: int = 5):
    """
    Тестирует эндпоинт /stream/open-stream-url

    Args:
        stream_url: URL видео потока (HLS, RTSP, HTTP)
        detection_interval: Интервал детекции в секундах
    """
    api_url = "http://localhost:8000/stream/open-stream-url"

    print(f"Открытие стрима: {stream_url}")
    print(f"Интервал детекции: {detection_interval} сек")

    try:
        data = {
            "url": stream_url,
            "detection_interval": detection_interval
        }

        response = requests.post(api_url, json=data)

        if response.status_code == 200:
            result = response.json()
            print(f"\n✓ Стрим успешно создан!")
            print(f"Stream ID: {result['stream_id']}")
            print(f"Статус: {result['status']}")
            print(f"WebSocket URL: {result['websocket_url']}")
            print(f"Video URL: {result['video_url']}")
            print(f"Сообщение: {result['message']}")

            print(f"\nДля просмотра результатов подключитесь к WebSocket:")
            print(f"ws://localhost:8000{result['websocket_url']}")

            print(f"\nДля просмотра видео откройте в браузере:")
            print(f"http://localhost:8000{result['video_url']}")

            print(f"\nДля остановки стрима используйте:")
            print(f"curl -X POST http://localhost:8000/stream/close/{result['stream_id']}")

            return result
        else:
            print(f"\n✗ Ошибка {response.status_code}")
            print(f"Детали: {response.text}")
            return None

    except Exception as e:
        print(f"✗ Ошибка: {e}")
        return None

def list_active_streams():
    """
    Получает список активных стримов
    """
    api_url = "http://localhost:8000/stream/list"

    try:
        response = requests.get(api_url)

        if response.status_code == 200:
            result = response.json()
            print(f"\n📊 Активные стримы: {result['count']}")
            for stream_id in result['active_streams']:
                print(f"  - {stream_id}")
            return result
        else:
            print(f"✗ Ошибка {response.status_code}")
            return None

    except Exception as e:
        print(f"✗ Ошибка: {e}")
        return None

def close_stream(stream_id: str):
    """
    Закрывает стрим
    """
    api_url = f"http://localhost:8000/stream/close/{stream_id}"

    try:
        response = requests.post(api_url)

        if response.status_code == 200:
            result = response.json()
            print(f"\n✓ Стрим закрыт!")
            print(f"Stream ID: {result['stream_id']}")
            print(f"Статус: {result['status']}")
            return result
        else:
            print(f"✗ Ошибка {response.status_code}")
            print(f"Детали: {response.text}")
            return None

    except Exception as e:
        print(f"✗ Ошибка: {e}")
        return None

if __name__ == "__main__":
    # Пример использования с HLS потоком
    # Замените на реальный URL потока

    print("=" * 60)
    print("Тест эндпоинта /stream/open-stream-url")
    print("=" * 60)

    # Пример URL (замените на реальный)
    test_url = "https://example.com/stream.m3u8"

    print(f"\n⚠️  ВНИМАНИЕ: Укажите реальный URL потока!")
    print(f"Текущий URL для теста: {test_url}")

    # Раскомментируйте для тестирования с реальным URL
    # result = test_open_stream_url(test_url, detection_interval=5)
    #
    # if result:
    #     print("\n⏳ Ждем 30 секунд...")
    #     time.sleep(30)
    #
    #     # Получаем список активных стримов
    #     list_active_streams()
    #
    #     # Закрываем стрим
    #     close_stream(result['stream_id'])

    print("\n" + "=" * 60)
    print("Для использования раскомментируйте код выше")
    print("и укажите реальный URL видео потока")
    print("=" * 60)
