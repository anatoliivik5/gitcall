from app.entities.dto import CallComeDataDTO
from app.service.beeline_service import BeelineCallsService
from app.service.bx_service import BxService


class FromBeeToBx:

    def prepare_response(self, domain,  date_from, date_to, userId, phone_client, duration_call_minut, companyId):
        """
        Подготовка данных для отправки в Bitrix24.

        :param beeline_url: URL для API Beeline
        :param headers: Заголовки для запросов
        :param params: Параметры для запросов
        :param phone_client: Номер телефона клиента
        :param duration_call: Продолжительность звонка в миллисекундах
        :param duration_call_minut: Продолжительность звонка в минутах
        :param companyId: Идентификатор компании
        :param date_from: Дата начала периода
        :param date_to: Дата окончания периода
        :param userId: Внутренний номер пользователя
        :return: Сформированный ответ для Bitrix24

        Parameters
        ----------
        list_of_dict_30
        """
        come_data = CallComeDataDTO(
            user_id=userId,
            date_from=date_from,
            date_to=date_to,
            duration_call_minut=duration_call_minut,
            phone_client=phone_client,
            companyId=companyId,
        )
        beeline_service = BeelineCallsService(come_data)
        beeline_result = beeline_service.get_calls_data()
        bx_service = BxService(domain, data=beeline_result, come_data=come_data)
        result = bx_service.process()

        return result



