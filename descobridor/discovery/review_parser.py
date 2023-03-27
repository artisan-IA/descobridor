from typing import List, Dict, Any
from bs4 import BeautifulSoup
import re
import pandas as pd
import hashlib
from dotenv import load_dotenv

from descobridor.helpers import get_localized_parser
from descobridor.discovery.review_age import ReviewAge
from joblib import Parallel, delayed


load_dotenv()


def get_soup(row):
    soup = BeautifulSoup(row, "html.parser")
    return soup


def get_trans_orig_matrix(text, translated_tag, original_tag):
    text_parts = [el.text.strip() for el in text.children if len(el.text.strip()) > 0]
    has_tranlated = [translated_tag in part for part in text_parts]
    is_translated_tag = [part == translated_tag for part in text_parts]

    has_original = [original_tag in part for part in text_parts]
    is_original_tag = [part == original_tag for part in text_parts]
    return text_parts, has_tranlated, is_translated_tag, has_original, is_original_tag

def seek_translated(text_parts, has_tranlated, is_translated_tag, translated_tag):
    original_review = ""
    translated_review = ""
    for i in range(len(text_parts)):
        if has_tranlated[i]:
            if is_translated_tag[i]:
                translated_review = text_parts[i + 1]
                original_review = text_parts[i - 1]
                break
            else:
                translated_review = text_parts[i].replace(translated_tag, "").strip()
                original_review = ""
                break
    return translated_review, original_review


def seek_original(text_parts, has_original, is_original_tag, original_tag):
    original_review = ""
    for i in range(len(text_parts)):
        if has_original[i]:
            if is_original_tag[i]:
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
    text_parts, has_tranlated, is_translated_tag, has_original, is_original_tag = \
        get_trans_orig_matrix(text, translated_tag, original_tag)
    
    translated_review, original_review = seek_translated(
        text_parts, 
        has_tranlated, 
        is_translated_tag, 
        translated_tag
    )
    if translated_review != "" and original_review != "":
        return original_review, translated_review
    elif translated_review != "" and original_review == "":
        original_review = seek_original(
            text_parts, 
            has_original, 
            is_original_tag, 
            original_tag
        )
        return original_review, translated_review
    elif translated_review == "" and original_review == "":
        original_review = seek_any(text_parts)
        return original_review, original_review
    
def get_full_review(text, original_tag, translated_tag, full_review_class):
    full_review = text.findChild("span", class_=full_review_class) 
    if not full_review:
        review_container = text.findChild("span", class_="f5axBf")
        if review_container:
            review_group = list(review_container.children)[1]
            original = review_group.contents[0].strip()
            return original, original
        else:
            return "", ""
    else:
        return get_target_n_original_from_text(full_review, original_tag, translated_tag)
    

#get reviews ands mark and add to a list
def get_review_text_and_translation(
    soup: BeautifulSoup, 
    full_review_class: str,
    original_tag: str,
    translated_tag: str
    ) -> List[List[str]]:
    texts = soup.find_all("div", class_="Jtu6Td")
    reviews = [
        get_full_review(text, original_tag, translated_tag, full_review_class)
        for text in texts
    ]
    print(len(reviews))
    return pd.DataFrame(reviews, columns=["review_original", "review_target_language"])


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
    loc_parser = get_localized_parser(language)
    reviews_df = get_review_text_and_translation(
        soup, 
        loc_parser["full_review_class"],
        loc_parser["original_text"],
        loc_parser["translated_by_google"]
        )
    reviews_df["language"] = language
    return reviews_df


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
