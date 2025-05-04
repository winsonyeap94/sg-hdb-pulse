import json
import os

import requests
from dotenv import load_dotenv

load_dotenv()


class OneMapAPI:

    def __init__(self):
        self.token = self.__get_token()

    def __get_token(self):
        """
        Get the token for the OneMap API
        """
        url = "https://www.onemap.gov.sg/api/auth/post/getToken"
        payload = {
            "email": os.environ['ONEMAP_EMAIL'],
            "password": os.environ['ONEMAP_EMAIL_PASSWORD']
        }
        response = requests.request("POST", url, json=payload)
        token = json.loads(response.text)['access_token']
        return token
        
    def get_address_details(self, search_query):
        """
        Get the address details for a given search query.

        Example results:
        {
            "SEARCHVAL": "640 ROWELL ROAD SINGAPORE 200640",
            "BLK_NO": "640",
            "ROAD_NAME": "ROWELL ROAD",
            "BUILDING": "NIL",
            "ADDRESS": "640 ROWELL ROAD SINGAPORE 200640",
            "POSTAL": "200640",
            "X": "30381.1007417506",
            "Y": "32195.1006872542",
            "LATITUDE": "1.30743547948389",
            "LONGITUDE": "103.854713903431"
        }
        """
        search_query = search_query.replace(" ", "%20")
        url = f"https://www.onemap.gov.sg/api/common/elastic/search?searchVal={search_query}&returnGeom=Y&getAddrDetails=Y&pageNum=1"
        headers = {"Authorization": f"Bearer {self.token}"}
        response = requests.get(url, headers=headers)
        response_json = json.loads(response.text)
        address_details = response_json['results'][0]
        return address_details


if __name__ == "__main__":

    one_map_api = OneMapAPI()
    address_details = one_map_api.get_address_details("ANG MO KIO AVE 10")
    print(address_details)
