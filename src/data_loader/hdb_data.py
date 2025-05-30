import os
import sys
from datetime import datetime

import pandas as pd
import requests
from tqdm import tqdm

sys.path.append(os.path.dirname(__file__))
from onemap_api import OneMapAPI

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from database import Database


class HDBData:

    def __init__(self):
        self.onemap_api = OneMapAPI()
        self.db = Database(db_path="hdb_data.db")

    def get_resale_price_data(self):
        """
        Loads the HDB resale price data from database.
        """
        data_df = self.db.read_table("SELECT * FROM HDB_RESALE_PRICE")
        return data_df
    
    def get_rental_price_data(self):
        """
        Loads the HDB rental price data from database.
        """
        data_df = self.db.read_table("SELECT * FROM HDB_RENTAL_PRICE")
        return data_df
    
    def update_resale_price_data(self):
        """
        Updates the HDB resale price data in the database.

        References:
        1) https://data.gov.sg/datasets?query=&page=1&sort=updatedAt&topics=housing&resultId=d_8b84c4ee58e3cfc0ece0d773c8ca6abc
        2) https://guide.data.gov.sg/developer-guide/dataset-apis

        NOTE: We do not update the data because there is no specific "key" to the dataset. There can be multiple resale prices within the same month.
        """
        # First, initiate the download of the dataset. The response will contain a URL to download the data.
        dataset_id = "d_8b84c4ee58e3cfc0ece0d773c8ca6abc"
        url = f"https://api-open.data.gov.sg/v1/public/api/datasets/{dataset_id}/initiate-download"
        response = requests.get(url)
        data = response.json()

        # Next, download the data from the URL.
        data_df = pd.read_csv(data['data']['url'])

        # Add a new column to the dataframe with the current date.
        data_df['update_dt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Insert the new data into the database.
        self.db.bulk_insert_df("HDB_RESALE_PRICE", data_df, if_exists="replace")

    def update_hdb_address_details(self):
        """
        Updates the HDB address details in the database.
        """
        # Load existing data from the database.
        existing_data = self.db.read_table("SELECT * FROM HDB_ADDRESS_DETAILS")
        hdb_resale_price_df = self.db.read_table("SELECT DISTINCT TOWN, STREET_NAME FROM HDB_RESALE_PRICE")
        hdb_rental_price_df = self.db.read_table("SELECT DISTINCT TOWN, STREET_NAME FROM HDB_RENTAL_PRICE")
        unique_address_details_df = pd.concat([hdb_resale_price_df, hdb_rental_price_df])\
            .drop_duplicates(subset=['TOWN', 'STREET_NAME'])
        
        # Load new data from the data.gov.sg API.
        # NOTE: We only query OneMap API for the address details if the address is not already in the database.
        existing_data_idx = existing_data['TOWN'] + existing_data['STREET_NAME']
        unique_address_details_df['IDX'] = unique_address_details_df['TOWN'] + unique_address_details_df['STREET_NAME']
        unique_address_details_df = unique_address_details_df.loc[~unique_address_details_df['IDX'].isin(existing_data_idx), :].copy()
        if len(unique_address_details_df) == 0:
            print("No new address details to update.")
            return
        
        new_address_details = []
        for _, row in tqdm(unique_address_details_df.iterrows(), total=len(unique_address_details_df)):
            address_details = self.onemap_api.get_address_details(row['STREET_NAME'])
            address_details['TOWN'] = row['TOWN']
            address_details['STREET_NAME'] = row['STREET_NAME']
            new_address_details.append(address_details)
        new_address_details_df = pd.DataFrame(new_address_details)\
            .drop(columns=['SEARCHVAL'])\
            .assign(UPDATE_DT=datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        # Insert the new data into the database.
        self.db.bulk_insert_df("HDB_ADDRESS_DETAILS", new_address_details_df, if_exists="append")

    def update_rental_price_data(self):
        """
        Updates the HDB rental price data in the database.

        References:
        1) https://data.gov.sg/datasets?query=&page=1&sort=updatedAt&topics=housing&resultId=d_c9f57187485a850908655db0e8cfe651
        2) https://guide.data.gov.sg/developer-guide/dataset-apis
        """
        # First, initiate the download of the dataset. The response will contain a URL to download the data.
        dataset_id = "d_c9f57187485a850908655db0e8cfe651"
        url = f"https://api-open.data.gov.sg/v1/public/api/datasets/{dataset_id}/initiate-download"
        response = requests.get(url)
        data = response.json()

        # Next, download the data from the URL.
        data_df = pd.read_csv(data['data']['url'])

        # Add a new column to the dataframe with the current date.
        data_df['update_dt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Insert the new data into the database.
        self.db.bulk_insert_df("HDB_RENTAL_PRICE", data_df, if_exists="replace")


if __name__ == "__main__":
    
    hdb_data = HDBData()
    # hdb_data.update_resale_price_data()
    # hdb_data.update_rental_price_data()
    hdb_data.update_hdb_address_details()
