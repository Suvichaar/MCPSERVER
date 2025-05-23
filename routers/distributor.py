import os
from fastapi import APIRouter
from dotenv import load_dotenv
from services.distribute import distribute_urls

router =  APIRouter()

@router.post("/distribute-urls")
def distribute_urls_endpoint():
    try:
        result = distribute_urls()
        return {"status": "success", **result}
    except Exception as e:
        return {"status": "error", "detail": str(e)}
# Compare this snippet from routers/image_router.py:
# import os
