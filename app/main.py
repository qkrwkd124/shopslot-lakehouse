from fastapi import FastAPI

from app.api.router import router


app = FastAPI(title="ShopSlot Operational API", version="0.1.0")
app.include_router(router)
