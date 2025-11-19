"""
Тестовый скрипт для проверки эндпоинта детекции курения
"""
import requests

def test_detect_smoking(image_path: str):
    """
    Тестирует эндпоинт /stream/detect-smoking

    Args:
        image_path: Путь к изображению для тестирования
    """
    url = "http://localhost:8000/stream/detect-smoking"

    print(f"Отправка изображения: {image_path}")

    try:
        with open(image_path, "rb") as f:
            files = {"file": f}
            response = requests.post(url, files=files)

        if response.status_code == 200:
            result = response.json()
            print(f"\n✓ Успешно!")
            print(f"Вердикт: {result['verdict']}")
            print(f"Временная метка: {result['timestamp']}")
        else:
            print(f"\n✗ Ошибка {response.status_code}")
            print(f"Детали: {response.text}")

    except FileNotFoundError:
        print(f"✗ Файл не найден: {image_path}")
    except Exception as e:
        print(f"✗ Ошибка: {e}")

if __name__ == "__main__":
    # Пример использования:
    # Замените на путь к вашему изображению
    test_image = "test_image.jpg"
    test_detect_smoking(test_image)
