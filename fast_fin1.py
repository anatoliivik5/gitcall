from time import sleep
from fastapi import FastAPI
from pydantic import BaseModel
import pandas as pd
from datetime import datetime, timedelta
import requests
import logging


# uvicorn fast_fin1:app --host 0.0.0.0 --port 8000
app = FastAPI()

# Устанавливаем базовый уровень логирования на INFO
logging.basicConfig(level=logging.INFO)

# Модель для данных, которые клиент отправляет на сервер
class Payload(BaseModel):
    date_from: str  # Формат даты: 'YYYY-MM-DD'
    date_to: str
    userId: int
    phone_client: str
    duration_call_minut: int

@app.post("/process-data/")
async def process_data(payload: Payload):
    # Получаем параметры из запроса
    date_from = payload.date_from
    date_to = payload.date_to
    # Преобразуем дату в нужный формат ISO 8601
    date_from = datetime.strptime(date_from, "%d.%m.%Y").strftime("%Y-%m-%dT00:00:00Z")
    date_to = datetime.strptime(date_to, "%d.%m.%Y").strftime("%Y-%m-%dT23:59:59Z")
    userId = payload.userId
    phone_client = payload.phone_client
    # Удаляем всё, что идет после первого разделителя запятой в номере телефона
    phone_client = phone_client.split(",")[0].strip()
    duration_call_minut = payload.duration_call_minut
    userId = int(userId)
    duration_call_minut = int(duration_call_minut)


    # Создаем DataFrame на основе параметров
    url = 'https://cloudpbx.beeline.ru/apis/portal/records'
    headers = {"/////..."}
    list_of_dict = []
    params = {
        'userId': userId,
        'dateFrom': date_from,
        'dateTo': date_to,
        'id': 0,  # начальное значение id
        'pageSize': 100  # количество записей на странице
    }

    # список для хранения результатов для текущего пользователя
    user_records = []

    # Цикл для получения всех записей для текущего пользователя
    while True:
        # Отправка GET-запроса
        response = requests.get(url, headers=headers, params=params)
        sleep(1)
        # Проверка статуса ответа
        if response.status_code == 200:
            # Преобразование JSON-ответа в словарь
            data_dict = response.json()
            user_records.extend(data_dict)

            # Проверяем, есть ли еще страницы результатов
            if len(data_dict) < 100:
                break  # Если записей меньше 100, это последняя страница
            else:
                # Обновляем id для следующего запроса
                params['id'] = data_dict[-1]['id']  # последний id плюс один для следующего запроса

        else:
            # Обработка ошибки
            print(f'Error: {response.status_code}')

    # Добавляем результаты для текущего пользователя в список
    list_of_dict.append(user_records)
    list_of_dict = sum(list_of_dict, [])
    list_of_dict_30 = []
    list_of_dict_all = []
    list_id_call = []
    list_id_call_all = []
    count_phone_client = 0
    calls_more_minute = 0
    duration_call = duration_call_minut * 60 * 1000  # Переводим в миллисекунды
    # Собираем данные по номеру телефона клиента и длительности разговора.
    # Если номер телефона клиента не найден в звонках, отбор только по длительности.
    for i in list_of_dict:
        if i['duration'] > duration_call and i['phone'] == phone_client:
            list_of_dict_30.append(i)
            list_id_call.append(i['id'])
            count_phone_client += 1
        elif i['duration'] > duration_call and i['phone'] != phone_client:
            list_of_dict_all.append(i)
            list_id_call_all.append(i['id'])
            calls_more_minute += 1

    if count_phone_client > 0:
        print(f'Кол-во звонков на номер {phone_client} = {count_phone_client}')
    else:
        print(
            f'Номер {phone_client} в списке звонков отсутствует. Общее кол-во звонков более {duration_call_minut} минут = {calls_more_minute}')
    if len(list_of_dict_30) == 0:
        list_of_dict_30, list_id_call = list_of_dict_all, list_id_call_all


    list_of_dict_call = []
    for id_num in list_id_call:
        url = 'https://cloudpbx.beeline.ru/apis/portal/records' + '/' + id_num + '/reference?'
        params1 = {'recordId': id_num}
        # Отправка GET-запроса
        response_call = requests.get(url, headers=headers, params=params1)
        call_dict = response_call.json()
        # print(len(call_dict), call_dict)
        list_of_dict_call.append(call_dict)
    data_list_of_dict_call = pd.DataFrame(list_of_dict_call)

    for data in list_of_dict_30:
        # Извлекаем значение в миллисекундах из словаря Дата и Продолжительность
        milliseconds = data['date']
        milli_duration = data['duration']

        # Преобразуем миллисекунды в объект datetime
        date_time = datetime.fromtimestamp(milliseconds / 1000.0)
        # Преобразуем миллисекунды в объект timedelta
        delta = timedelta(milliseconds=milli_duration)
        # Получаем часы, минуты и секунды из объекта timedelta
        hours, remainder = divmod(delta.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)

        # Форматируем объект datetime в нужный строковый формат
        formatted_date_time = date_time.strftime('%Y-%m-%d %H:%M:%S')
        # Форматируем результат в строку
        formatted_duration = '{:02}:{:02}:{:02}'.format(hours, minutes, seconds)
        # Заменяем значения в словаре время
        data['duration'] = formatted_duration
        data['date'] = formatted_date_time
        # Заменяем значения в словаре параметры внутренний номер и имя
        data['externalId'] = data['abonent']['extension']
        data['abonent'] = data['abonent']['firstName']

    data_dict_30 = pd.DataFrame(list_of_dict_30)
    data_url = data_list_of_dict_call[['fileSize', 'url']]
    data_ab = data_dict_30[['fileSize', 'externalId', 'phone', 'date', 'abonent', 'duration']]

    # Конкатенация по колонке 'fileSize'
    result_df = pd.concat([data_ab.set_index('fileSize'), data_url.set_index('fileSize')], axis=1).reset_index()

    # Присваиваем новые имена колонкам работаем с новым дата-фреймом
    result_df = result_df.rename(
        columns={'date': 'Дата', 'externalId': 'Внутренний', 'abonent': 'Менеджер', 'phone': 'Номер телефона',
                 'duration': 'Продолжительность', 'url': 'Ссылка для скачивания'})
    # Преобразование DataFrame в JSON
    json_data = result_df.to_json(orient='records', force_ascii=False)
    # print(type(json_data), json_data)
    return json_data
