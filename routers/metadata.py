import os
import psycopg2
from fastapi import APIRouter
from dotenv import load_dotenv
from services.metadata_generator import generate_meta_data

router = APIRouter()
@router.post("/generate-metadata")
def generate_metadata():
    """
    Endpoint to trigger metadata generation.
    """
    try:
        result = generate_meta_data()
        return {"status": "success", **result}
    except Exception as e:
        return {"status": "error", "detail": str(e)}
    # return {"status": "error", "detail": str(e)}
    # return {"status": "error", "detail": str(e)}
    # return {"status": "error", "detail": str(e)}
    # return {"status": "error", "detail": str(e)}
