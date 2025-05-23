import os
from fastapi import APIRouter
from services.resizer import generate_resized_urls

router = APIRouter()
@router.post("/generate-resized-urls")
def trigger_resized_url_generation():
    try:
        result = generate_resized_urls()
        return {"status": "success", **result}
    except Exception as e:
        return {"status": "error", "detail": str(e)}
