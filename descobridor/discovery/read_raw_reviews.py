import re
from typing import Any, Dict, Tuple
import numpy as np
import requests
from datetime import date, datetime
import pandas as pd 
import time
from bs4 import BeautifulSoup

from truby.db_connection import MongoConnection, CosmosConnection, RedisConnection
from descobridor.discovery import review_parser as rp
from descobridor.discovery.constants import (
    TOO_MANY_PAGES, 
    REVIEWS_TOO_OLD_MONTHS,
    GMAPS_NEXT_PAGE_TOKEN,
    PAGE_STATUS_EXPIRATION,
    RAW_PAGE_EXPIRATION_S
    )
from descobridor.the_logger import logger


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


def binary_page_to_str(raw_google_output: bytes) -> str:
    return raw_google_output.decode('utf-8')
     

def update_places_is_reviewed(request: Dict[str, Any]) -> None:
    with MongoConnection("places") as conn:
        conn.collection.update_one(
            {'place_id': request['place_id']},
            {"$set": {f"review_extr_ds_{request['language']}": str(date.today())}}
        )


def store_reviews(reviews):
    """Store reviews in mongo, omitting duplicated reviews
    dumplications come from 2 people haveing the same name.
    cases are reasonably rare, so we don't bother."""
    mongo = MongoConnection("reviews")
    mongo.df_to_collection_omit_duplicated(reviews)
        

def make_page_record(
    place_id: str,
    data_id: str,      
    name: str,
    language: str,
    page_number: int,
    page_str: str, 
    next_page_token: str
    ) -> Dict[str, Any]:
    return {
        'place_id': place_id,
        'data_id': data_id,
        'name': name,
        'scrape_ds': str(date.today()),
        'scrape_language': language,
        'page_number': page_number,
        'next_page_token': next_page_token,
        'content': page_str
    }
    
    
def store_page(record: Dict[str, Any]) -> None:
    with CosmosConnection("raw_reviews") as conn:
        conn.collection.insert_one(record, ttl=RAW_PAGE_EXPIRATION_S)
    return record
     
     
def process_page(request: Dict[str, Any], page_number: int, next_page_token: str) -> Tuple[Dict, str]:
    """
    having a request, a page number and a next_page_token,
    we get a page from google, and extract reviews from it.
    """
    link = format_query_page(request['data_id'], next_page_token, 
                                 request['country_domain'], request['language'])
    raw_google_output = get_review_page_from_google(link)
    page_str = binary_page_to_str(raw_google_output)
    logger.info(f"page {page_number} read")
    try:
        next_page_token = get_next_page_token(page_str)
    except IndexError:
        next_page_token = None
        
    page_record = make_page_record(
                place_id=request['place_id'],
                data_id=request['data_id'],
                name=request['name'],
                language=request['language'],
                page_number=page_number,
                page_str=page_str,
                next_page_token=next_page_token
            )
    return page_record, next_page_token


def assert_data_id_present(request: Dict[str, Any]) -> bool:
    if not request['data_id']:
        raise IndexError(f"no data_id key for {request['name']}. It should not be in this queue.")
    return True


def is_stop_condition(reviews, next_page_token: str, last_scraped: datetime) -> bool:
    reviews_age = datetime.now() - pd.to_datetime(reviews.review_date.min())
    logger.info(f"reviews_age: {reviews_age}")
    return (
        (next_page_token is None) 
        or (pd.to_datetime(reviews.review_date.max()) < last_scraped)
        or reviews_age > pd.Timedelta(REVIEWS_TOO_OLD_MONTHS*30, unit='d')
    )


def get_last_scraped(request: Dict[str, Any]) -> datetime:
    last_scraped = pd.to_datetime(request['last_scraped'])
    if last_scraped is None:
        return datetime(2015, 1, 1, 0, 0)
    else:
        return last_scraped.normalize()
    
    
def _successful_page_key(request: Dict[str, Any]) -> str:
    return f"{request['place_id']}_{request['language']}_page"
    
def successful_page_to_redis(request: Dict[str, Any], page_number: int):
    """
    stores the numbero of the page that was successfully scraped.
    PAGE_STATUS_EXPIRATION is the time in seconds that the key will be stored in redis.
    It has to be quite a bit, so that in case of failure we could return to this place.
    """
    with RedisConnection() as redis:
        redis.connection.set(
            _successful_page_key(request), 
            page_number,
            ex=PAGE_STATUS_EXPIRATION
            )
        
def get_successful_page_from_redis(request: Dict[str, Any]) -> int:
    with RedisConnection() as redis:
        page = redis.connection.get(_successful_page_key(request))
    if page is None:
        return 0
    else:
        return int(page)
    

def get_next_page_token_from_cosmos(request: Dict[str, Any], page_number) -> str:
    if page_number == 0:
        return ''
    
    with CosmosConnection("raw_reviews") as conn:
        record = conn.collection.find_one(
            {"page_number": page_number, "place_id": request['place_id']}
        )
    if record is None:
        return ''
    else:
        return record['next_page_token']
    

def get_page_num_and_page_token(request: Dict[str, Any]) -> Tuple[int, str]:
    """
    sometimes we have to restart the extraction from a certain page.
    sometimes we start from the beginning.
    This functions determines the page number and the next_page_token
    from where we start.
    """
    page_number = get_successful_page_from_redis(request)
    next_page_token = get_next_page_token_from_cosmos(request, page_number)
    if page_number == 0:
        return 0, ''
    else:
        # + 1 because we want to start from the next page after successful page
        return page_number + 1, next_page_token
     
     
# this blasted function is too long
def extract_all_reviews(request: Dict[str, Any]) -> None:
    """
    :param request: a dictionary with the following keys:
        place_id: str, 
        data_id: str
        country_domain: str
        language: str,
        name: str
        last_scraped: str
    """
    assert_data_id_present(request)
    last_scraped = get_last_scraped(request)
    # start the review extraction
    page_number, next_page_token = get_page_num_and_page_token(request)
    while page_number < TOO_MANY_PAGES:
        logger.info(f'reading {request["name"]} page {page_number}')
        page_record, next_page_token = process_page(request, page_number, next_page_token)
        reviews = rp.get_page_reviews(page_record, request['language'])
        if not reviews.empty:
            logger.info(f"storing page and reviews for {page_number}")
            store_page(page_record)
            logger.info(f"stored page {page_number}")
            store_reviews(reviews)
            logger.info(f"stored reviews for {page_number}")
            successful_page_to_redis(request, page_number)
        else:
            logger.warning(f"no reviews found for {request['name']}")
            break

        if is_stop_condition(reviews, next_page_token, last_scraped):
            logger.info(f"stop condition met for {request['name']}")
            break
        
        page_number += 1
        wait = max(np.random.beta(2, 2) * 2.5 + 2.5, np.random.gamma(4, 2))
        logger.info(f'sleeping for {wait} s')
        time.sleep(wait)

    update_places_is_reviewed(request)
    logger.info(f' [v] finished with {request["name"]}')
