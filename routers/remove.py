from fastapi import APIRouter
import os
from services.removal import clean_video_metadata_table

router = APIRouter()

@router.post("/clean-video-meta")
def clean_video_metadata():
    try:
        result = clean_video_metadata_table()
        return {"status": "success", "message": result}
    except Exception as e:
        return {"status": "error", "detail": str(e)}
