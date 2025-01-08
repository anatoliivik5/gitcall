# bx_api:
import requests
from fastapi import HTTPException
from app.entities.dto import SendToTimelineCallsLinksDTO
from config import settings


class BxAPI:
    def __init__(self):
        self.headers = {
             "Content-Type": "application/json",
             "X-HELPER-API-KEY": settings.bitrix_api_key
         }
        self.bitrix_url = settings.bitrix_url

    def send_to_bitrix(self, data: SendToTimelineCallsLinksDTO):
        # Подготовка данных для отправки
        payload = {
            "domain": data.domain,
            "links": data.links,
            "companyId": data.companyId,
            "message": data.message
        }
        try:
            # Отправка POST-запроса
            response = requests.post(self.bitrix_url, json=payload, headers=self.headers)
            response.raise_for_status()

            # Проверка статуса ответа
            if response.status_code == 200:
                print(f"Успешно отправлено в Bitrix24: {response.json()}")
            else:
                print(f"Ошибка при отправке в Bitrix24. Код ответа: {response.status_code}")
                print(f"Ответ от сервера: {response.text}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Ошибка при отправке данных в Bitrix24. Код ответа: {response.status_code}, Ответ: {response.text}"
                )
        except requests.exceptions.RequestException as e:
            print(f"Ошибка при запросе в Bitrix24: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Ошибка при запросе в Bitrix24: {str(e)}"
            )


bx_api = BxAPI()
