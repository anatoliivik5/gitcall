# routers
from fastapi import APIRouter, Query
from app.usecase.audio_from_bee_to_bx import FromBeeToBx
from fastapi.responses import JSONResponse

# uvicorn app.main:app --host 0.0.0.0 --port 8000
router = APIRouter()


@router.post("/process-data/")
async def process_data(
    date_from: str = Query(..., alias="date_from"),
    date_to: str = Query(..., alias="date_to"),
    userId: int = Query(...),
    phone_client: str = Query(..., alias="phone_client"),
    duration_call_minut: int = Query(..., alias="duration_call_minut"),
    companyId: int = Query(..., alias="companyId")
):
    domain = "april-garant.bitrix24.ru"
    case = FromBeeToBx()
    result = case.prepare_response(domain, date_from, date_to, userId, phone_client, duration_call_minut, companyId)
    return JSONResponse(result)

