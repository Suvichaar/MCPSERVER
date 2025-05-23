from fastapi import FastAPI

# ✅ Import all routers
from routers import (
    quotes,
    structure,
    image_router,
    azure,
    track,
    track_images,
    merge,
    altxt ,
    image_resizer,
    distributor,
    metadata,
    video_meta,
    remove,
    rotate,
    reorder
)

app = FastAPI(title="Quote MCP Server")

# ✅ Register all routers with organized prefixes and tags
app.include_router(quotes.router, prefix="/quotes", tags=["Quotes"])
app.include_router(structure.router, prefix="/generate", tags=["Structure"])
app.include_router(image_router.router, prefix="/images", tags=["Image Download"])
app.include_router(azure.router, prefix="/azure", tags=["Azure Batch Text Submit"])
app.include_router(track_images.router, prefix="/track", tags=["Azure Batch Image Submit"])
app.include_router(track.router, prefix="/azure", tags=["Azure Batch  Tracker"])
app.include_router(merge.router, prefix="/merge_text", tags=["Merge Textual Data"])
app.include_router(altxt.router, prefix="/match", tags=["ALT Text Matcher"])  
app.include_router(image_resizer.router, prefix="/resizer", tags=["Image Resizer"])
app.include_router(distributor.router, prefix="/distribute", tags=["Resized url distribute"])
app.include_router(video_meta.router, prefix="/videometa", tags=["Video Metadata"])  
app.include_router(remove.router, prefix="/modify_column", tags=["Column Modification"])  
app.include_router(metadata.router, prefix="/metadata", tags=["Metadata Generator"])
app.include_router(rotate.router, prefix="/rotate", tags=["Rotate Meta Data"])
app.include_router(reorder.router, prefix="/reorder", tags=["Final Quote Fancy Data"])
@app.get("/")
def root():
    return {"message": "MCP Server is running"}

