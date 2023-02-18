from bs4 import BeautifulSoup
from more_itertools import locate
import re
from typing import Dict, List
from collections import defaultdict
import pandas as pd


def get_review_text_and_ratings(soup: BeautifulSoup) -> List[List[str]]:
    """
    every soup contains several reviews.
    this function gets reviews as list of words ands ratings 
    adds them to a list
    """
    all_texts=soup.find_all("div", class_="Jtu6Td")    
    texts=[]
    list_of_sentences_and_words=[]
    for text in all_texts:
        texts.append([" ".join(text.span.text.split("\n")).replace(",", "").replace("|", "").strip()])    
    for sentences in texts:
        for sentence in sentences:            
            new = sentence.split()
            list_of_sentences_and_words.append(new)
            
    return list_of_sentences_and_words


def get_page_reviews(soup: BeautifulSoup) -> List[Dict[str, str]]:    
    """
    Creates list of dicts with review and marks.
    It appends reviews and if present detailed ratings
    eg ambient: 5, food: 4
    """
    texts = get_review_text_and_ratings(soup)
    list_of_reviews=[]   
    index=[]
    for sentence in texts:
        dict_of_reviews={}
        if ":" in sentence:
            ind= list(locate(sentence, lambda x: x == ':'))
            index.append(ind)
            lenth=len(ind)

            dict_of_reviews["review"] = [" ".join(sentence[:ind[0]-1]).strip()]            
            for i in range(lenth):
                dict_of_reviews[sentence[ind[i]-1]] = sentence[ind[i]+1:ind[i]+2]
        else:
            dict_of_reviews["review"] = [" ".join(sentence).strip()]
        
        list_of_reviews.append(dict_of_reviews)
            
    return list_of_reviews


def get_reviewer_names(soup) -> List[str]:    
    """
    get name of reviewer and add to list
    """
    names_text = soup.find_all("div", class_="TSUbDb")
    list_of_texts = []        
    for name in names_text:              
        list_of_texts.append(" ".join(name.text.split("\n")).replace(",", "").replace("|", "").strip())
    
    return list_of_texts


def get_review_times(soup: BeautifulSoup) -> List[str]:
    """
    gets time of review and add to list 
    time of review is in format provided by google: human-readable string
    """
    texts = soup.find_all("div", class_="PuaHbe")
    list_of_texts = []        
    for text in texts:         
        line = " ".join(text.text.split("\n")).split()
        clean_line = (" ".join(line).replace("Nuevo", "").strip())
        list_of_texts.append(clean_line)   
    return list_of_texts   


def get_stars(soup: BeautifulSoup) -> List[str]:
    """
    returns a list of stars given by users (number of stars)
    """
    texts = soup.find_all("span", class_="Fam1ne EBe2gf")    
    list_of_texts = []        
    for text in texts:      
        str_line = str(text)        
        pattern = re.compile(r"\d{1}")
        line=pattern.findall(str_line)[0].strip()
        list_of_texts.append(line)   
    return list_of_texts                 
    
    
def get_all_reviews(html_content: str, place_name: str, scrape_ds: str):
    """
    A function applied to a row of a dataframe.
    That row contains a page of reviews together with place name and scrape datestring.
    returns:
        all reviews on that page (every review is a dict with review and ratings), 
        names of reviewers, 
        times of reviews, 
        stars, 
        place_names, 
        scrape_ds'
    """
    soup = BeautifulSoup(html_content, "html.parser")
    reviews = get_page_reviews(soup)     
    names = get_reviewer_names(soup)
    times = get_review_times(soup)
    stars= get_stars(soup)
    place_names = [place_name] * len(stars)
    scrape_dss = [scrape_ds] * len(stars)
    return reviews, names, times, stars, place_names, scrape_dss


def scrape_all_files_reviews(reviews_df: pd.DataFrame) -> pd.DataFrame:
    """
    takes a dataframe with clumn 'content' each row of which contains a page of reviews
    it transforms this content into texts of reviwes, stars and so on.
    Then it attaches place name and scrape_ds to each. 
    returns a dataframe with all reviews
    """
    list_of_ind_reviwes = defaultdict(list)
    for row in reviews_df.itertuples():
        reviews, names, times, stars, place_names, scrape_dss = get_all_reviews(
            row.content, row.name, row.scrape_ds
            )
        list_of_ind_reviwes['review'].extend(reviews)
        list_of_ind_reviwes['reviewer_name'].extend(names)
        list_of_ind_reviwes['stars'].extend(stars)
        list_of_ind_reviwes['review_time'].extend(times)
        list_of_ind_reviwes['place_name'].extend(place_names)
        list_of_ind_reviwes['scrape_ds'].extend(scrape_dss)
        
    return pd.DataFrame(list_of_ind_reviwes)
