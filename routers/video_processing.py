import asyncio
import base64
import os
import tempfile
import uuid
from collections import deque
from typing import Deque, Dict

import cv2
import numpy as np
from fastapi import APIRouter, BackgroundTasks, File, HTTPException, UploadFile

from utils import detect_smoking

router = APIRouter()

# In-memory storage for job status and results
job_results: Dict[str, Dict] = {}


async def process_video_smoking_detection(video_path: str):
    """
    Processes a video to detect smoking by sampling frames and using a sliding window.
    1. Samples one frame every 5 seconds.
    2. Runs detection on all sampled frames.
    3. Applies a sliding window of 5 samples.
    4. Returns "Yes" if over 50% of the windows are positive.

    Args:
        video_path: The path to the video file.

    Returns:
        A verdict ("Yes" or "No") based on the smoking detection analysis.
    """
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        raise HTTPException(status_code=400, detail="Could not open video file.")

    fps = cap.get(cv2.CAP_PROP_FPS)
    if not fps or fps == 0:
        fps = 30  # Assume 30 fps if not available

    frame_interval = int(fps * 5)  # Sample a frame every 5 seconds
    frame_count = 0
    sampled_frames = []

    while True:
        ret, frame = cap.read()
        if not ret:
            break

        if frame_count % frame_interval == 0:
            sampled_frames.append(frame)
        
        frame_count += 1

    cap.release()

    if not sampled_frames:
        return "No" # Video is shorter than 5 seconds

    # Run detection on all sampled frames concurrently
    detection_tasks = []
    for frame in sampled_frames:
        _, buffer = cv2.imencode('.png', frame)
        b64_image = base64.b64encode(buffer).decode('utf-8')
        detection_tasks.append(asyncio.to_thread(detect_smoking, b64_image))
    
    api_verdicts = await asyncio.gather(*detection_tasks)

    # Normalize verdicts to "Yes" or "No"
    frame_verdicts = []
    for result in api_verdicts:
        if result and "yes" in result.strip().lower():
            frame_verdicts.append("Yes")
        else:
            frame_verdicts.append("No")

    # Apply a sliding window of 5 to the frame verdicts
    if len(frame_verdicts) < 5:
        # Not enough samples for a full window, if any sample is "Yes", then "Yes"
        return "Yes" if "Yes" in frame_verdicts else "No"

    window_size = 5
    verdicts_window: Deque[str] = deque(maxlen=window_size)
    window_verdicts = []

    for verdict in frame_verdicts:
        verdicts_window.append(verdict)
        if len(verdicts_window) == window_size:
            yes_count = verdicts_window.count("Yes")
            # If more than half the frames in the window are "Yes"
            if yes_count / window_size > 0.5:
                window_verdicts.append("Yes")
            else:
                window_verdicts.append("No")
    
    if not window_verdicts:
        return "No"

    # Final verdict: if more than 50% of the windows are positive
    positive_windows = window_verdicts.count("Yes")
    if positive_windows / len(window_verdicts) > 0.5:
        return "Yes"
    else:
        return "No"


async def process_video_and_store_result(job_id: str, video_path: str):
    """
    Wrapper function to run in the background, process video, and store the result.
    """
    try:
        verdict = await process_video_smoking_detection(video_path)
        job_results[job_id] = {"status": "completed", "verdict": verdict}
    except Exception as e:
        job_results[job_id] = {"status": "failed", "error": str(e)}
    finally:
        if os.path.exists(video_path):
            os.unlink(video_path)


@router.post("/video/detect-smoking", tags=["Video Processing"], status_code=202)
async def detect_smoking_in_video(
    background_tasks: BackgroundTasks, file: UploadFile = File(...)
):
    """
    Uploads a video and starts smoking detection in the background.

    This endpoint returns a job ID immediately. Use the
    `/video/detect-smoking/result/{job_id}` endpoint to check the status and
    get the result.
    """
    job_id = str(uuid.uuid4())
    job_results[job_id] = {"status": "processing"}

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as tmp:
            contents = await file.read()
            tmp.write(contents)
            video_path = tmp.name

        background_tasks.add_task(process_video_and_store_result, job_id, video_path)

        return {
            "message": "Video processing started in the background.",
            "job_id": job_id,
            "status_url": f"/video/detect-smoking/result/{job_id}",
        }

    except Exception as e:
        job_results[job_id] = {"status": "failed", "error": str(e)}
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/video/detect-smoking/result/{job_id}", tags=["Video Processing"])
async def get_detection_result(job_id: str):
    """
    Retrieves the result of a smoking detection job.
    """
    job = job_results.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job ID not found.")
    
    return job
