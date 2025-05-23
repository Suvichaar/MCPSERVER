import os
from fastapi import APIRouter
from services.image_downloader import download_and_upload_author_images

router = APIRouter()

@router.post("/batch-author-images")
def trigger_batch_image_upload():
    try:
        result = download_and_upload_author_images()
        return {"status": "success", **result}
    except Exception as e:
        return {"status": "error", "detail": str(e)}
