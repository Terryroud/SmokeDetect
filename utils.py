from openai import OpenAI
import requests
from bs4 import BeautifulSoup
import re

API_KEY = ""

client = OpenAI(
  base_url="https://openrouter.ai/api/v1",
  api_key=API_KEY,
)

def detect_smoking(b64_image: str):
    """
    Calls the OpenAI API to detect smoking in a base64 encoded image.
    """
    try:
        response = client.chat.completions.create(
            model="nvidia/nemotron-nano-12b-v2-vl:free",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": "Is someone smoking a cigarette or vape in this photo? Just answer Yes or No."
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/png;base64,{b64_image}"
                            }
                        }
                    ]
                }
            ],
            extra_body={"reasoning": {"enabled": False}, "temperature" : 0}
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"Error calling OpenAI API: {e}")
        return None

def extract_hls_url_from_page(url):
    """
    Extracts HLS (.m3u8) URLs from a given web page.

    Args:
        url (str): The URL of the web page to scrape.

    Returns:
        str: The first HLS URL found on the page, or None.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL: {e}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')
    hls_urls = []

    # 1. Search for .m3u8 in script tags (often embedded in JavaScript)
    for script in soup.find_all('script'):
        if script.string:
            # This regex looks for http or https URLs ending with .m3u8
            found_urls = re.findall(r'https?://[^\\s"\'_]+\.m3u8', script.string)
            hls_urls.extend(found_urls)
    
    # 2. Search for .m3u8 in video and source tags
    for video_tag in soup.find_all('video'):
        if 'src' in video_tag.attrs and '.m3u8' in video_tag['src']:
            hls_urls.append(video_tag['src'])
        for source_tag in video_tag.find_all('source'):
            if 'src' in source_tag.attrs and '.m3u8' in source_tag['src']:
                hls_urls.append(source_tag['src'])

    # 3. Search for .m3u8 in any tag's attributes (e.g., 'href', 'data-src', etc.)
    for tag in soup.find_all(True): # find all tags
        for attr, value in tag.attrs.items():
            if isinstance(value, str) and '.m3u8' in value:
                if value.startswith('http://') or value.startswith('https://'):
                    hls_urls.append(value)
    
    if hls_urls:
        return hls_urls[0]
    return None
