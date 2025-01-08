# beeline_service
from datetime import datetime, timedelta
import pandas as pd
from app.client.beeline_api import beeline_api, BeelineAPI
from app.entities.dto import BeelineCallsServiceResultDTO
from app.usecase.audio_from_bee_to_bx import CallComeDataDTO


class BeelineCallsService:
	beeline_api: BeelineAPI

	def __init__(
			self,
			data: CallComeDataDTO
	):

		self.userId = data.user_id
		self.dateFrom = datetime.strptime(data.date_from, "%d.%m.%Y").strftime("%Y-%m-%dT00:00:00Z")
		self.dateTo = datetime.strptime(data.date_to, "%d.%m.%Y").strftime("%Y-%m-%dT23:59:59Z")
		self.duration_call = data.duration_call_minut * 60 * 1000
		self.duration_call_minut = data.duration_call_minut
		self.phone_client = data.phone_client.split(",")[0].strip()[-10:]
		self.companyId = data.companyId
		self.beeline_api = beeline_api

	def get_calls_data(self) -> BeelineCallsServiceResultDTO:
		params = {
			"userId": self.userId,
			"dateFrom": self.dateFrom,
			"dateTo": self.dateTo,
			"companyId": self.companyId
		}

		# Получение данных о звонках
		call_data = self.beeline_api.fetch_call_data(params)
		# Обработка данных звонков
		list_of_dict_30, list_id_call = self.process_call_data(call_data)
		call_references = self.fetch_call_references(list_id_call)
		result_df = self.format_result_data(list_of_dict_30, call_references)
		if result_df is None:
			result_df = []
		else:
			result_df = result_df.to_dict('records')
		result = BeelineCallsServiceResultDTO(
			list_of_dict_30=list_of_dict_30,
			list_id_call=list_id_call,
			result_df=result_df
		)
		return result

	def process_call_data(self, list_of_dict):
		list_of_dict_30 = []
		list_id_call = []
		count_phone_client = 0

		# Фильтр звонков по номеру и длительности
		for i in list_of_dict:
			if i['duration'] > self.duration_call and i['phone'][-10:] == self.phone_client:
				i['companyId'] = self.companyId
				list_of_dict_30.append(i)
				list_id_call.append(i['id'])
				count_phone_client += 1
		return list_of_dict_30, list_id_call

	def fetch_call_references(self, list_id_call):
		list_of_dict_call = []
		for id_num in list_id_call:
			call_dict = self.beeline_api.fetch_call_references(id_num=id_num)
			if call_dict:
				list_of_dict_call.append(call_dict)
			else:
				raise
		return pd.DataFrame(list_of_dict_call)

	def format_result_data(self, list_of_dict_30, data_list_of_dict_call):
		if data_list_of_dict_call.empty:
			result_df = None
		else:
			for data in list_of_dict_30:
				milliseconds = data['date']
				milli_duration = data['duration']
				date_time = datetime.fromtimestamp(milliseconds / 1000.0)
				delta = timedelta(milliseconds=milli_duration)
				hours, remainder = divmod(delta.seconds, 3600)
				minutes, seconds = divmod(remainder, 60)
				formatted_date_time = date_time.strftime('%Y-%m-%d %H:%M:%S')
				formatted_duration = '{:02}:{:02}:{:02}'.format(hours, minutes, seconds)
				data['duration'] = formatted_duration
				data['date'] = formatted_date_time
				data['externalId'] = data['abonent']['extension']
				data['abonent'] = data['abonent']['firstName']
			data_dict_30 = pd.DataFrame(list_of_dict_30)
			data_url = data_list_of_dict_call[['fileSize', 'url']]
			data_ab = data_dict_30[['fileSize', 'externalId', 'phone', 'companyId', 'date', 'abonent', 'duration']]
			result_df = pd.concat([data_ab.set_index('fileSize'), data_url.set_index('fileSize')], axis=1).reset_index()

		return result_df
