from fastapi import APIRouter
import os
from services.azure_image_batch import generate_and_upload_image_alt_batch

router = APIRouter()
@router.post("/batch-image-alt")
def trigger_batch_image_alt_upload():
    try:
        result = generate_and_upload_image_alt_batch()
        return {"status": "success", **result}
    except Exception as e:
        return {"status": "error", "detail": str(e)}
# Compare this snippet from routers/image_router.py:
# import os
# from fastapi import APIRouter
# from services.image_downloader import download_and_upload_author_images
#
# router = APIRouter()             
#
# @router.post("/batch-author-images")      