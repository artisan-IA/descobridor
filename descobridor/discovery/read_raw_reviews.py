import re
from typing import Any, Dict, Tuple
import numpy as np
import requests
from datetime import date, datetime
import pandas as pd 
import time
from bs4 import BeautifulSoup

from truby.db_connection import MongoConnection, CosmosConnection
from descobridor.discovery import review_parser as rp
from descobridor.discovery.constants import (
    TOO_MANY_PAGES, 
    REVIEWS_TOO_OLD_MONTHS,
    GMAPS_NEXT_PAGE_TOKEN
    )


def get_language_related_g_header(country_domain: str, language: str):
    return f"https://www.google.{country_domain}/async/reviewDialog?hl={language}&async=feature_id"


def get_review_page_from_google(link: str) -> str:
    session = requests.Session()
    response = session.get(link)
    soup = BeautifulSoup(response.text, "html.parser")
    return soup.prettify('utf-8')


def get_next_page_token(page_str: str) -> str:
    return re.findall(GMAPS_NEXT_PAGE_TOKEN, page_str)[0]


def format_query_page(
    data_id: str, 
    next_page_token: str,
    country_domain: str,
    language: str
    ) -> str:
    """
    a google review pages consists of:
    a general header: a function language and country
    data_id: fixed for a given place
    next_page_token: it's "" for the first page, and extracted from previos page for the rest
    a generic tail.
    """
    g_header = get_language_related_g_header(country_domain, language)
    head = f"{g_header}:{data_id}"
    tail = f",sort_by:newestFirst,next_page_token:{next_page_token},associated_topic:,_fmt:pc"
    return f"{head}{tail}"


def binary_page_to_str(raw_google_output: bytes):
    return raw_google_output.decode('utf-8')
     

def update_places_is_reviewed(request: Dict[str, Any]):
    with MongoConnection("gmaps_places_output") as conn:
        conn.collection.update_one(
            {'place_id': request['place_id']},
            {"$set": {'reviews_extracted': True, 'reviwes_extraction_ds': str(date.today())}}
        )
        
        
# TODO must have! # Ay no!
def find_latest_available_review_date(place_id: str):
    return None


def store_reviews(reviwes):
    # TODO FIX but why?? seems alright
    with MongoConnection("reviews") as conn:
        conn.collection.insert_many(reviwes)
        

def make_page_record(
    place_id: str,
    data_id: str,      
    name: str,
    page_number: int,
    page_str: str, 
    next_page_token: str
    ):
    return {
        'place_id': place_id,
        'data_id': data_id,
        'name': name,
        'scrape_ds': str(date.today()),
        'page_number': page_number,
        'next_page_token': next_page_token,
        'content': page_str
    }
    
def store_page(record: Dict[str, Any]):
    with CosmosConnection("raw_reviews") as conn:
        conn.collection.insert_one(record)
    return record
     
     
def process_page(request: Dict[str, Any], page_number: int, next_page_token: str) -> Tuple(Dict, str):
    link = format_query_page(request['data_id'], next_page_token, 
                                 request['country_domain'], request['language'])
    raw_google_output = get_review_page_from_google(link)
    page_str = binary_page_to_str(raw_google_output)
    print(f"page {page_number} read")
    try:
        next_page_token = get_next_page_token(page_str)
    except IndexError:
        next_page_token = None
        
    page_record = make_page_record(
                place_id=request['place_id'],
                data_id=request['data_id'],
                name=request['name'],
                page_number=page_number,
                page_str=page_str,
                next_page_token=next_page_token
            )
    return page_record, next_page_token


def assert_data_id_present(request: Dict[str, Any]) -> bool:
    if not request['data_id']:
        raise IndexError(f"no data_id key for {request['name']}. It should not be in this queue.")
    return True


def is_stop_condition(reviews, next_page_token: str, last_review_available) -> bool:
    reviews_age = datetime.now() - pd.to_datetime(reviews.review_date.min())
    return (
        (next_page_token is None) 
        or (reviews.review_date.max() < last_review_available)
        or reviews_age > pd.Timedelta(REVIEWS_TOO_OLD_MONTHS, unit='M')
    )

     
# this blasted function is too long
def extract_all_reviews(request: Dict[str, Any]) -> None:
    """
    :param request: a dictionary with the following keys:
        place_id: str, data_id: str
    """
    assert_data_id_present(request)
    
    last_review_available = find_latest_available_review_date(request['place_id'])
    # start the review extraction
    next_page_token = ''
    page_number = 0
    while page_number < TOO_MANY_PAGES:
        print(f'reading page {page_number}')
        page_record, next_page_token = process_page(request, page_number, next_page_token)
        reviews = rp.get_all_reviews(page_record)
        print(f"storing page {page_number}")
        store_page(page_record)
        store_reviews(reviews)
        print(f"stored page {page_number}")

        if is_stop_condition(reviews, next_page_token, last_review_available):
            break
        
        page_number += 1
        wait = max(2, np.random.gamma(6, 2))
        print(f'sleeping for {wait} s')
        time.sleep(wait)

    update_places_is_reviewed(request)
    print('finished')
