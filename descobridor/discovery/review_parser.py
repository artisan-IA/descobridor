# -*- coding: utf-8 -*-


from typing import List
from bs4 import BeautifulSoup
from more_itertools import locate
import re
import pandas as pd
import pickle
import hashlib
from descobridor.discovery.review_age import ReviewAge
# from review_age import ReviewAge
from joblib import Parallel, delayed



def get_soup(row):
    soup = BeautifulSoup(row, "html.parser")
    return soup

def _clean_text_from_spaces(all_texts):
    texts=[]
    for text in all_texts:
        # single text could be a review or a mark
        # source code has a lot of \n and similiar stuff, so we need to remove them
        texts.append(
            [" ".join(text.span.text.split("\n")).replace(",", "").replace("|", "").strip()]
            )
    return texts
   
#get reviews ands mark and add to a list
def get_review_text_and_marks(soup):
    all_texts=soup.find_all("div", class_="Jtu6Td")  # class where revies and ratings are  
    texts = _clean_text_from_spaces(all_texts)
    
    list_of_lists_and_words=[]
    for sentences in texts:
        for sentence in sentences:        
            new = sentence.split() # we need words separately
            list_of_lists_and_words.append(new)
            break
            
    return list_of_lists_and_words

#create list of dicts with review and marks
def add_to_list_of_review_dict(list_of_sentences_and_words: List[List[str]]):   
    """
    TODO: no main rating is scraped
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
def get_name_list(soup):    
    names_text = soup.find_all("div", class_="TSUbDb")
    list_of_texts = []        
    for name in names_text:              
        list_of_texts.append(" ".join(name.text.split("\n")).replace(",", "").replace("|", "").strip())   
    return list_of_texts

#get time of review and add to list 
def get_time_list(soup):
    texts = soup.find_all("div", class_="PuaHbe")
    list_of_texts = []        
    for text in texts:         
        line = " ".join(text.text.split("\n")).split()
        clean_line = (" ".join(line).replace("Nuevo", "").strip())
        list_of_texts.append(clean_line)   
    return list_of_texts   

#get stars of the review
def get_stars(soup):
    texts = soup.find_all("span", class_="Fam1ne EBe2gf")    
    list_of_texts = []        
    for text in texts:      
        str_line = str(text)        
        pattern = re.compile(r"\d{1}")
        line=pattern.findall(str_line)[0].strip()
        list_of_texts.append(line)   
    return list_of_texts                 

#add list of taken info to the main dict
def merge_list_int_list_of_dicts(a_list_to_add, list_of_dicts, key_name) -> List[dict]:
    """
    we have a list of dicts 
    and a list of values 
    We want for every item in the list add a value to the corresponding dict
    """
    new_list_of_dict=[]
    for (dict1, item) in zip(list_of_dicts, a_list_to_add):
        dict1[key_name] = item
        new_list_of_dict.append(dict1)                
    return new_list_of_dict

#get all reviews to a final dict
def get_all_reviews(row, language):
    file_name = row["content"]
    
    soup = get_soup(file_name)  
    texts=get_review_text_and_marks(soup)
    reviews=add_to_list_of_review_dict(texts)     
    names = get_name_list(soup)
    time = get_time_list(soup)
    stars= get_stars(soup)
    
    restaurant_name = [row["name"]] * len(reviews)
    scrape_ds = [row["scrape_ds"]] * len(reviews)
    place_ids = [row["place_id"]] * len(reviews)
    
    dict_name =merge_list_int_list_of_dicts(names, reviews, "reviewer_name")
    dict_time = merge_list_int_list_of_dicts(time, dict_name, "time")
    dict_rname = merge_list_int_list_of_dicts(restaurant_name, dict_time, "restaurant_name")
    dict_scrape_ds = merge_list_int_list_of_dicts(scrape_ds, dict_rname, "scrape_ds")
    dict_place_ids = merge_list_int_list_of_dicts(place_ids, dict_scrape_ds, "place_id")
    dict_list_stars =merge_list_int_list_of_dicts(stars, dict_scrape_ds, "stars")
    review_df = (
        pd.DataFrame(add_transalated_original_to_dict(dict_list_stars, language))
        .pipe(add_review_age, language)
        .pipe(add_unique_review_id)
    )
    return review_df
    

def add_review_age(review_df, language):
    review_df = review_df.copy()
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


# def split_translated_original(text, language):
#     """
#     """
#     if not text or not isinstance(text, str):
#         return '', ''
    
#     if language == "ES":
#         trans_ind = "\(Traducido por Google\)"
#         orig_ind = "\(Original\)"
#     else:
#         raise NotImplementedError("Only ES is implemented")
#     if len(re.findall(trans_ind, text)) > 0:
#         _, start_trans = re.search(trans_ind, text).span()
#         end_trans, start_orig = re.search(orig_ind, text[start_trans:]).span()
#         end_trans, start_orig = end_trans + start_trans, start_orig + start_trans
#         repeat_trans = re.search(trans_ind, text[start_orig:])
#         if repeat_trans:
#             end_orig, _ = repeat_trans.span()
#             end_orig += start_orig
#         else:
#             end_orig = len(text)
#         return text[start_trans:end_trans], text[start_orig:end_orig]
#     else:
#         return text, ''
    
def split_translated_original(text, language):
    """
    """
    if not text or not isinstance(text, str):
        return '', ''
    
    if language == "ES":
        trans_ind = "(Traducido por Google)"
        orig_ind = "(Original)"
        trans_ind_ing = "(Translated by Google)"
    else:
        raise NotImplementedError("Only ES is implemented")
    text = text.rstrip('Más')
    sep = text[:30]
    print('sep: ', sep)
    repetition_index = text.lower().find(sep.lower(),1)
    if repetition_index != 0:
        text = text[:repetition_index]
        print('text: ', text)
    if len(re.findall(trans_ind, text)) > 0:
        text_target = text.partition(orig_ind)[0].replace(trans_ind, '').strip()
        text_other_lang = text.partition(orig_ind)[2].replace(orig_ind, '').partition(trans_ind)[0].strip()
        return text_target, text_other_lang
    if len(re.findall(trans_ind_ing, text)) > 0:
        text_other_lang = text.partition(trans_ind_ing)[0].replace(trans_ind_ing, '').strip()
        # text_target = text.partition(trans_ind_ing)[2].replace(trans_ind_ing, '').partition(trans_ind_ing)[0].strip()
        text_target = text.partition(trans_ind_ing)[2].replace(trans_ind_ing, '').partition(trans_ind_ing)[0].strip().replace(text_other_lang, '').strip()
        return text_target, text_other_lang
    else:
      return text, ''

    
def add_transalated_original_to_dict(dict_list, language):
    for a_dict in dict_list:
        a_dict["review_target_language"], a_dict["review_original"] = split_translated_original(a_dict["review"], language)
        a_dict["language"] = language
    return dict_list


#scrape all reviews from a given df
def extract_all_files_reviews(df, language):
    r = Parallel(n_jobs=4)(
        delayed(get_all_reviews)(row, language) for (i, row) in df.iterrows()
        )
    return pd.concat(r)

def save_to_pickle(list_of_final_dict):        
    with open('all_raw_reviews.pkl', 'wb') as f:
        pickle.dump(list_of_final_dict, f)



# if __name__ == "__main__":
#     # for testing and such
#     file_name = 'raw_reviews_sample_500.csv'
#     # file_name = "all_raw_reviews.csv"
#     #file_name= "doner_kebab.csv"
#     df = pd.read_csv(file_name)
#     list_of_final_dict= extract_all_files_reviews(df, 'ES')
#     # save_to_pickle(list_of_final_dict)
#     # list_of_final_dict.to_csv("reviews_sample_partial.csv")   
#     print(list_of_final_dict)
#     print('yay')



