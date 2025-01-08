# beeline_api
import requests
from config import settings


class BeelineAPI:
    def __init__(self):
        self.headers = {"X-MPBX-API-AUTH-TOKEN": settings.beeline_api_auth_token}
        self.base_url = settings.beeline_api_url

    def fetch_call_data(self, params):
        list_of_dict = []

        while True:
            response = requests.get(self.base_url, headers=self.headers, params=params)
            if response.status_code == 200:
                data_dict = response.json()
                list_of_dict.extend(data_dict)
                if len(data_dict) < 100:
                    break
                else:
                    params['id'] = data_dict[-1]['id']
            else:
                raise Exception(f"Beeline API Error: {response.status_code}")
        return list_of_dict

    def fetch_call_references(self, id_num: int):
        endpoint_url = f"{self.base_url}/{id_num}/reference?"

        response_call = requests.get(endpoint_url, headers=self.headers)

        result = None
        if response_call.status_code == 200:
            result = response_call.json()
            return result
        else:
            Exception(f"Beeline Reference API Error: ")
            return result


# Создаем глобальный экземпляр
beeline_api = BeelineAPI()
