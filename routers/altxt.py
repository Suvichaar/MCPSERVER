# routers/alttxt.py
from fastapi import APIRouter
from services.alttxtmatch import match_alttxt_and_store

router = APIRouter()

@router.post("/match-alt-text")
def run_alttxt_matching():
    result = match_alttxt_and_store()
    return result