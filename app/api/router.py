from fastapi import APIRouter


router = APIRouter()


@router.get("/health", tags=["operations"])
def health() -> dict[str, str]:
    return {"status": "ok"}
