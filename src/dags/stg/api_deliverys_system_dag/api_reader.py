from urllib.parse import quote_plus as quote
from airflow.models import Variable

import requests

class ApiConnect:
    def __init__(self,
                 entity: str,
                 sort: str,
                 limit: str,
                 offset: str
                 ) -> None:

        self.entity = entity
        self.sort = sort
        self.limit = limit
        self.offset = offset


    def url(self) -> str:
        return f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/{self.entity}'
 

    def client(self):
        nickname = Variable.get('X-Nickname', deserialize_json=False)
        cohort = Variable.get('X-Cohort', deserialize_json=False)
        apy_key = Variable.get('X-API-KEY', deserialize_json=False)


        headers = {
            'X-Nickname': nickname,
            'X-Cohort': cohort,
            'X-API-KEY': apy_key,
        }
    
        response = requests.get(self.url(), headers=headers, params={
            'sort_field': str(self.sort),
            'sort_direction': 'asc',
            'limit': str(self.limit),
            'offset': str(self.offset),
        })

        if response.status_code == 200:
            dict_list_res = response.json()
        else:
            print("Errorrrrrrrrrrrrrrr:", response.status_code, response.reason)

        return dict_list_res
    