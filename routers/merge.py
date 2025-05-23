import os
import psycopg2
from fastapi import APIRouter
from dotenv import load_dotenv
from services.merge_handler import merge_textual_data

router = APIRouter()

@router.post("/merge-textual-data")
def textual_data():
    return merge_textual_data()  # ✅ Call the function and return its result