# bx_service
from app.client.bx_api import BxAPI, SendToTimelineCallsLinksDTO
from app.service.beeline_service import BeelineCallsService, BeelineCallsServiceResultDTO
from app.usecase.audio_from_bee_to_bx import CallComeDataDTO


class BxService:
	def __init__(self, domain, data: BeelineCallsServiceResultDTO, come_data: CallComeDataDTO):
		self.domain = domain
		self.calls_data = data
		self.come_data = come_data

	def process(self):
		data_result = self.prepare_response()
		bx_client = BxAPI()
		send_result = bx_client.send_to_bitrix(data=data_result)
		return send_result

	def prepare_response(self) -> SendToTimelineCallsLinksDTO:
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
		calls_data = self.calls_data
		c_data = self.come_data

		# Проверяем, есть ли данные
		if not calls_data.list_of_dict_30 or not calls_data.list_id_call or not calls_data.result_df:
			# Если данные отсутствуют
			links = []
			message = (
				f"Нет звонков c параметрами: более {c_data.duration_call_minut} мин., "
				f"на номер {c_data.phone_client}, период с {c_data.date_from} по {c_data.date_to}, "
				f"внутренний номер {c_data.user_id}"
			)
			result = SendToTimelineCallsLinksDTO(
				domain=self.domain,
				links=links,
				companyId=c_data.companyId,
				message=message
			)
			return result

		links = [
			{
				"name": f"Дата, время: {row['date']}, продолжительность: {row['duration']}",
				"value": row['url']
			}
			for row in calls_data.result_df  # Итерируемся по списку

		]

		message = f"Звонки на номер {c_data.phone_client} за период с {c_data.date_from} по {c_data.date_to} включительно."

		result = SendToTimelineCallsLinksDTO(
			domain=self.domain,
			links=links,
			companyId=c_data.companyId,
			message=message
		)
		return result

