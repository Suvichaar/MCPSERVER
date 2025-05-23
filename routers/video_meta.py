from fastapi import APIRouter
import os 
from services.videosheetadd import assign_video_metadata
from dotenv import load_dotenv

router = APIRouter()

@router.post("/video-meta")
def video_meta():
    """
    Endpoint to assign video metadata to distribution data.
    """
    load_dotenv()
    return assign_video_metadata()
    # ✅ Call the function and return its result
    # return {
    #     "message": "Video metadata assigned successfully.",
    #     "status": "success"
    # } 