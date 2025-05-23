from fastapi import APIRouter
from services.fetch import fetch_and_store_pending_batches

router = APIRouter()

@router.get("/download_jsonl_data")
def trigger_batch_data_download():
    return fetch_and_store_pending_batches()