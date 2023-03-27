# -*- coding: utf-8 -*-


from typing import List, Dict, Any
from bs4 import BeautifulSoup
from more_itertools import locate
import re
import os
import pandas as pd
import hashlib
from dotenv import load_dotenv

from descobridor.helpers import get_localization, get_localized_parser
from descobridor.discovery.review_age import ReviewAge
from joblib import Parallel, delayed


load_dotenv()


def get_soup(row):
    soup = BeautifulSoup(row, "html.parser")
    return soup


def _clean_text_from_spaces(all_texts: List[BeautifulSoup]) -> List[List[str]]:
    texts=[]
    for text in all_texts:
        # single text could be a review or a mark
        # source code has a lot of \n and similiar stuff, so we need to remove them
        texts.append(
            [" ".join(text.span.text.split("\n")).replace(",", "").replace("|", "").strip()]
            )
    return texts
   

#get reviews ands mark and add to a list
def get_review_text_and_marks(soup: BeautifulSoup) -> List[List[str]]:
    all_texts=soup.find_all("div", class_="Jtu6Td")  # class where revies and ratings are  
    texts = _clean_text_from_spaces(all_texts)
    
    list_of_lists_and_words=[]
    for sentences in texts:
        for sentence in sentences:        
            new = sentence.split() # we need words separately
            list_of_lists_and_words.append(new)
            break
            
    return list_of_lists_and_words


def split_translated_original(text, language):
    """
    """
    if not text or not isinstance(text, str):
        return '', ''
    
    if language == "es":
        trans_ind = "(Traducido por Google)"
        orig_ind = "(Original)"
        trans_ind_ing = "(Translated by Google)"
        more = "Más"
    elif language == "en":
        trans_ind = "(Translated by Google)"
        orig_ind = "(Original)"
        more = "More"
    else:
        raise NotImplementedError("Only ES, EN is implemented")
    text = text.rstrip(more)
    sep = text[:30]
    repetition_index = text.lower().find(sep.lower(),1)
    if repetition_index != 0:
        text = text[:repetition_index]
    if len(re.findall(trans_ind, text)) > 0:
        text_target = text.partition(orig_ind)[0].replace(trans_ind, '').strip()
        text_other_lang = (
            text.partition(orig_ind)[2]
            .replace(orig_ind, '')
            .partition(trans_ind)[0]
            .strip()
        )
        return text_target, text_other_lang
    if len(re.findall(trans_ind_ing, text)) > 0:
        text_other_lang = text.partition(trans_ind_ing)[0].replace(trans_ind_ing, '').strip()
        text_target = (
            text.partition(trans_ind_ing)[2]
            .replace(trans_ind_ing, '')
            .partition(trans_ind_ing)[0]
            .strip()
        )
        return text_target, text_other_lang
    else:
      return text, ''


#create list of dicts with review and marks
def add_to_list_of_review_dict(list_of_sentences_and_words: List[List[str]]):   
    """
    """ 
    list_of_reviews=[]   
    index=[]
    for sentence in list_of_sentences_and_words:
        dict_of_reviews={}
        # normally mark is right after colon.
        # befre the colon is information for what (ambience, food, and so on) was given
        if ":" in sentence:
            ind= list(locate(sentence, lambda x: x == ':'))
            index.append(ind)
            length=len(ind)
            dict_of_reviews["review"] = [" ".join(sentence[:ind[0]-1]).strip()]            
            for i in range(length):
                dict_of_reviews[sentence[ind[i]-1]] = sentence[ind[i]+1:ind[i]+2]
            list_of_reviews.append(dict_of_reviews)
        else:
            dict_of_reviews["review"] = " ".join(sentence).strip()
            list_of_reviews.append(dict_of_reviews)            
    return list_of_reviews


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
        clean_line = (" ".join(line).replace("Nuevo", "").strip())
        list_of_texts.append(clean_line)   
    return pd.Series(list_of_texts, name="time")


#get stars of the review
def get_stars(soup: BeautifulSoup, stars_class) -> List[int]:
    """
    returns stars given in a review
    """
    texts = soup.find_all("span", class_=stars_class)
    print(texts) 
    list_of_ratings = [None for _ in range(len(texts))]     
    for (i, text) in enumerate(texts):      
        str_line = str(text)
        print(str_line)
        pattern = re.compile(r"\d{1}")
        line=pattern.findall(str_line)[0].strip()
        print(line)
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
        

def soup_to_reviews(soup, language):
    loc = get_localization(os.environ["country"])
    texts=get_review_text_and_marks(soup)
    reviews = add_to_list_of_review_dict(texts)
    reviews = [add_food_service_atmosphere(review, loc) for review in reviews]
    reviews_df = pd.DataFrame([filter_review_keys(review) for review in reviews])
    reviews_df["target_original"] = reviews_df["review"].apply(
        lambda x: split_translated_original(x, language)
    )
    reviews_df["review_target_language"] = reviews_df["target_original"].apply(lambda x: x[0])
    reviews_df["review_original"] = reviews_df["target_original"].apply(lambda x: x[1])
    reviews_df["language"] = language
    return reviews_df.drop(columns=["target_original"])


#get all reviews to a final dict
def parse_the_page(row, language):
    loc_parser = get_localized_parser(language)
    content = row["content"]
    soup = get_soup(content)  
    reviews = soup_to_reviews(soup, language)
    names = get_name_list(soup)
    time = get_times(soup)
    stars= get_stars(soup, loc_parser["stars_class"])
    
    restaurant_name = pd.Series([row["name"]] * len(reviews),  name="restaurant_name")
    scrape_ds = pd.Series([row["scrape_ds"]] * len(reviews), name="scrape_ds")
    place_ids = pd.Series([row["place_id"]] * len(reviews), name="place_id")
    data_ids = pd.Series([row["data_id"]] * len(reviews), name="data_id")
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


def get_page_reviews(row, language):
    review_df = parse_the_page(row, language)
    review_df = add_review_age(review_df, language)
    
    review_df.to_csv("review_df.csv")
    return review_df
    

def add_review_age(review_df, language):
    review_df = review_df.copy()
    print(review_df.head(1))
    review_df["review_age"] = review_df.apply(
        lambda df: ReviewAge(df["scrape_ds"], df["time"], language), axis=1)
    review_df['age_precision'] = review_df['review_age'].apply(lambda x: x.precision)
    review_df['review_date'] = review_df['review_age'].apply(lambda x: x.review_date)
    return review_df.drop(columns=["review_age"])


def add_unique_review_id(review_df):
    review_df = review_df.copy()
    review_df["unique_review_id"] = (
        review_df["place_id"] + "_" + review_df["reviewer_name"]
    ).apply(lambda x: hashlib.new('ripemd160', x.encode()).hexdigest())
    return review_df



#scrape all reviews from a given df
def extract_all_files_reviews(df, language):
    r = Parallel(n_jobs=4)(
        delayed(get_page_reviews)(row, language) for (i, row) in df.iterrows()
        )
    return pd.concat(r).reset_index(drop=True)
