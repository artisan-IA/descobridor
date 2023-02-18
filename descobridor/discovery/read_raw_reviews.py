import re
from typing import Any, Dict
import numpy as np
import requests
from datetime import date
import time
from bs4 import BeautifulSoup

from truby.db_connection import MongoConnection, CosmosConnection
from descobridor.discovery import serp_api as sa
from descobridor.discovery import review_parser as rp
from descobridor.discovery.constants import G_REVIEW_HEADER


def extract_reviews_page(link: str):
     session = requests.Session()
     response = session.get(link)
     soup = BeautifulSoup(response.text, "html.parser")
     return soup.prettify('utf-8')


def format_query_next_page(data_id: str, next_page_token: str):
     head = f"{G_REVIEW_HEADER}:{data_id}"
     tail = f",sort_by:newestFirst,next_page_token:{next_page_token},associated_topic:,_fmt:pc"
     return f"{head}{tail}"

def _next_page_token(raw_page):
     return re.findall('data-next-page-token="(\w+\=+)', raw_page)[0]

def binary_page_to_str(raw_page: bytes):
     return raw_page.decode('utf-8')

def store_page(
     place_id: str,
     data_id: str,      
     name: str,
     page_number: int,
     page_str: str, 
     ):
     next_page_token = _next_page_token(page_str)
     record = {
          'place_id': place_id,
          'data_id': data_id,
          'name': name,
          'scrape_ds': str(date.today()),
          'page_number': page_number,
          'next_page_token': next_page_token,
          'content': page_str
     }
     with CosmosConnection("raw_reviews") as conn:
          conn.collection.insert_one(record)
     return record
     

def update_gmaps_entry_is_reviewed(gmaps_entry: Dict[str, Any]):
     with MongoConnection("gmaps_places_output") as conn:
          conn.collection.update_one(
               {'place_id': gmaps_entry['place_id']},
               {"$set": {'reviews_extracted': True, 'reviwes_extraction_ds': str(date.today())}}
          )
          
def find_latest_available_review_date(place_id: str):
     return None


def store_reviews(reviwes):
     # TODO FIX
     with MongoConnection("reviews") as conn:
          conn.collection.insert_many(reviwes)
     
     
def extract_all_reviews(gmaps_entry: Dict[str, Any]):
     data_id = gmaps_entry['data_id']
     if not data_id:
          serp_output = sa.serp_search_place(
               gmaps_entry['place_id'], 
               gmaps_entry['name'],
               gmaps_entry['coords']
               )
          data_id = serp_output['place_results']['data_id']
     
     last_review_available = find_latest_available_review_date(gmaps_entry['place_id'])
     # start the review extraction
     next_page_token = ''
     page_number = 0
     while page_number < np.random.randint(70, 100):
          print(f'reading page {page_number}')
          link = format_query_next_page(data_id, next_page_token)
          raw_data = extract_reviews_page(link)
          page_str = binary_page_to_str(raw_data)
          print(f"page {page_number} read")
          try:
               next_page_token = _next_page_token(page_str)
          except IndexError:
               next_page_token = None
               break
          else:
               print(f"storing page {page_number}")
               record = store_page(
                    place_id=gmaps_entry['place_id'],
                    data_id=data_id,
                    name=gmaps_entry['name'],
                    page_number=page_number,
                    page_str=page_str
               )
               print(f"stored page {page_number}")
               reviews = rp.get_all_reviews(record)
               store_reviews(reviews)
               if reviews.review_date.max() < last_review_available:
                    break
               page_number += 1
               wait = max(2, np.random.gamma(6, 2))
               print(f'sleeping for {wait} s')
               time.sleep(wait)

     update_gmaps_entry_is_reviewed(gmaps_entry)
     print('finished')
     