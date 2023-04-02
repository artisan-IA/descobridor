from typing import List, Dict, Any
from bs4 import BeautifulSoup
import re
import pandas as pd
import hashlib
from joblib import Parallel, delayed
from dotenv import load_dotenv

from descobridor.helpers import get_localized_parser
from descobridor.discovery.review_age import ReviewAge
from descobridor.the_logger import logger


load_dotenv()


def get_soup(row):
    soup = BeautifulSoup(row, "html.parser")
    return soup


def get_trans_orig_status(text, translated_tag, original_tag):
    text_parts = [el.text.strip() for el in text.children if len(el.text.strip()) > 0]
    tranlated_status = [
        (translated_tag in part, part == translated_tag)  for part in text_parts]

    original_status = [
        (original_tag in part, part == original_tag)  for part in text_parts]
    
    return text_parts, original_status, tranlated_status

def seek_translated(text_parts, translated_status, translated_tag):
    original_review = ""
    translated_review = ""
    for i in range(len(text_parts)):
        if translated_status[i][0]:
            if translated_status[i][1]:
                translated_review = text_parts[i + 1]
                original_review = text_parts[i - 1]
                break
            else:
                translated_review = text_parts[i].replace(translated_tag, "").strip()
                original_review = ""
                break
    return translated_review, original_review


def seek_original(text_parts, original_status, original_tag):
    original_review = ""
    for i in range(len(text_parts)):
        if original_status[i][0]:
            # sometimes original is empty, though translated is not
            if original_status[i][1] and len(original_status) > i + 1:
                original_review = text_parts[i + 1]
                break
            else:
                original_review = text_parts[i].replace(original_tag, "").strip()
                break
    return original_review

def seek_any(text_parts):
    original_review = ""
    for i in range(len(text_parts)):
        if text_parts[i] != "":
            original_review = text_parts[i]
            break
    return original_review

def get_target_n_original_from_text(text, original_tag, translated_tag):
    text_parts, original_status, translated_status = \
        get_trans_orig_status(text, translated_tag, original_tag)
    
    translated_review, original_review = seek_translated(
        text_parts, 
        translated_status, 
        translated_tag
    )
    if translated_review != "" and original_review != "":
        return original_review, translated_review
    elif translated_review != "" and original_review == "":
        original_review = seek_original(
            text_parts, 
            original_status, 
            original_tag
        )
        return original_review, translated_review
    elif translated_review == "" and original_review == "":
        original_review = seek_any(text_parts)
        return original_review, original_review
    
def get_full_review(text, original_tag, translated_tag, full_review_class, sing_lang_review_class):
    full_review = text.findChild("span", class_=full_review_class) 
    if not full_review:
        review_container = text.findChild("span", class_=sing_lang_review_class)
        if review_container:
            review_group = list(review_container.children)[1]
            original = review_group.contents[0].strip()
            return original, original
        else:
            return "", ""
    else:
        return get_target_n_original_from_text(full_review, original_tag, translated_tag)
    

#get reviews ands mark and add to a list
def soup_to_reviews(soup: BeautifulSoup, language) -> List[List[str]]:
    loc_parser = get_localized_parser(language)
    texts = soup.find_all("div", class_=loc_parser["review_class"])
    logger.debug(f"Found {len(texts)} reviews")
    reviews = [
        get_full_review(
            text, 
            loc_parser["original_tag"],
            loc_parser["translated_tag"], 
            loc_parser["full_review_class"],
            loc_parser["sing_lang_review_class"]
        )
        for text in texts
    ]
    logger.debug(f"Extracted {len(reviews)} reviews")
    reviews_df = pd.DataFrame(reviews, columns=["review_original", "review_target_language"])
    reviews_df["language"] = language
    return reviews_df


#get lis with users name
def get_name_list(soup: BeautifulSoup) -> pd.Series:    
    """
    user name is in div with class TSUbDb
    """
    names_text = soup.find_all("div", class_="TSUbDb")
    list_of_texts = []        
    for name in names_text:              
        list_of_texts.append(" ".join(name.text.split("\n")).replace(",", "").replace("|", "").strip())   
    return pd.Series(list_of_texts, name="reviewer_name")


#get time of review and add to list 
def get_times(soup: BeautifulSoup) -> pd.Series:
    texts = soup.find_all("div", class_="PuaHbe")
    list_of_texts = []        
    for text in texts:         
        line = " ".join(text.text.split("\n")).split()
        clean_line = (" ".join(line).strip())
        list_of_texts.append(clean_line)   
    return pd.Series(list_of_texts, name="time")


#get stars of the review
def get_stars(soup: BeautifulSoup, stars_class) -> List[int]:
    """
    returns stars given in a review
    """
    texts = soup.find_all("span", class_=stars_class)
    list_of_ratings = [None for _ in range(len(texts))]     
    for (i, text) in enumerate(texts):      
        str_line = str(text)
        pattern = re.compile(r"\d{1}")
        line=pattern.findall(str_line)[0].strip()
        list_of_ratings[i] = float(line)
    return pd.Series(list_of_ratings, name="stars")             


def add_food_service_atmosphere(review: Dict[str, Any], loc: Dict[str, str]):
    review = review.copy()
    if loc["food"] not in review:
        review["food"] = [None]
    if loc["service"] not in review:
        review["service"] = [None]
    if loc["athmosphere"] not in review:
        review["atmosphere"] = [None]
    return {key.lower(): value for key, value in review.items()}


def filter_review_keys(review: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "review": review["review"],
        "food": review["food"][0],
        "service": review["service"][0],
        "atmosphere": review["atmosphere"][0],
    }


#get all reviews to a final dict
def parse_the_page(page_record: Dict[str, Any], language: str) -> pd.DataFrame:
    loc_parser = get_localized_parser(language)
    content = page_record["content"]
    soup = get_soup(content)  
    reviews = soup_to_reviews(soup, language)
    names = get_name_list(soup)
    time = get_times(soup)
    stars= get_stars(soup, loc_parser["stars_class"])
    
    restaurant_name = pd.Series([page_record["name"]] * len(reviews),  name="restaurant_name")
    scrape_ds = pd.Series([page_record["scrape_ds"]] * len(reviews), name="scrape_ds")
    place_ids = pd.Series([page_record["place_id"]] * len(reviews), name="place_id")
    data_ids = pd.Series([page_record["data_id"]] * len(reviews), name="data_id")
    return pd.concat([
        reviews,
        names,
        time, 
        stars,
        restaurant_name,
        scrape_ds,
        place_ids,
        data_ids
    ], axis=1)
    

def add_review_age(review_df: pd.DataFrame, language: str) -> pd.DataFrame:
    review_df = review_df.copy()
    review_df["review_age"] = review_df.apply(
        lambda df: ReviewAge(df["scrape_ds"], df["time"], language), axis=1)
    review_df['age_precision'] = review_df['review_age'].apply(lambda x: x.precision)
    review_df['review_date'] = review_df['review_age'].apply(lambda x: x.review_date)
    return review_df.drop(columns=["review_age"])


def add_unique_review_id(review_df: pd.DataFrame) -> pd.DataFrame:
    review_df = review_df.copy()
    review_df["unique_review_id"] = (
        review_df["place_id"] + "_" + review_df["reviewer_name"] + "_" + review_df["language"].iat[0]
    ).apply(lambda x: hashlib.new('ripemd160', x.encode()).hexdigest())
    return review_df


def get_page_reviews(page_record: Dict[str, Any], language: str) -> pd.DataFrame:
    review_df = parse_the_page(page_record, language)
    if review_df.empty:
        return review_df
    else:
        review_df = add_review_age(review_df, language)
        review_df = add_unique_review_id(review_df)
        return review_df


#scrape all reviews from a given df
def extract_all_files_reviews(df: pd.DataFrame, language: str) -> pd.DataFrame:
    r = Parallel(n_jobs=4)(
        delayed(get_page_reviews)(row, language) for (i, row) in df.iterrows()
        )
    return pd.concat(r).reset_index(drop=True)
