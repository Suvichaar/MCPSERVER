from fastapi import APIRouter, HTTPException
from services.azure_batch import generate_and_upload_batch

router = APIRouter()

@router.post("/submit-batch")
def submit_azure_batch():
    try:
        return generate_and_upload_batch()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))