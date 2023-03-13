from typing import Any, Dict, Optional, Tuple, Union
import h3
import os
import pandas as pd
from datetime import date, datetime
import googlemaps
import time
from dotenv import load_dotenv
from truby.db_connection import MongoConnection
from descobridor.discovery.constants import (
     SUB_HEX_SIZE,
)


load_dotenv()


def scan_hex(
        hex_name: str,  
        query: str, 
        n_pages: Optional[int]=2
    ) -> pd.DataFrame:
    """uses googlemapes Places API to search for places
    in a given hex
    Args:
        hex_name (str): which hex we are searching
        sub_hex_size (int): we are going to split our search into
        several searches over subhexes. Since google sorting is a little chaotic, 
        we aim for sub-hexes in a hope to find more places
        query (str): what we are searching. As finicky as google search, of course
        n_pages (Optional[int], optional): _description_. Defaults to 2.
        less relevant (accourding to google) places aren't on the first page
        even if their in our selected radius.

    Returns:
        pd.DataFrame: raw dataframe of google results.
    """
    gmaps = googlemaps.Client(key=os.environ['GAPI'])
    sub_hexes = h3.h3_to_children(hex_name, SUB_HEX_SIZE)
    radius = int(h3.edge_length(SUB_HEX_SIZE) * 1000)

    all_places = []
    for hind in sub_hexes:
        first_page = gmaps.places(
            location=h3.h3_to_geo(hind), 
            query=query,
            radius=radius)
        first_page_df = pd.DataFrame(
            first_page['results']
        )
        first_page_df.loc[:, f'hex{SUB_HEX_SIZE}'] = hind
        next_page_token = first_page['next_page_token']
        all_places.append(first_page_df)
        
        for p_number in range(n_pages - 1):
            time.sleep(5)
            next_page = gmaps.places(
                page_token=next_page_token
            )
            next_page_df = pd.DataFrame(
                next_page['results']
            )
            next_page_df.loc[:, f'search_hex{SUB_HEX_SIZE}'] = hind
            try:
                next_page_token = next_page['next_page_token']
                all_places.append(next_page_df)
            except KeyError:
                print(f'no more pages for {hind}, current page {p_number}')
                break
        
        print(f'done with {hind}')

    all_places = pd.concat(all_places).reset_index(drop=True)
    all_places = _add_location_columns(all_places, hex_name)
    all_places = _add_query_boilerplate(all_places, query, None)
    all_places = all_places.drop_duplicates('place_id')
    all_places.set_index(all_places.place_id, inplace=True)
    return all_places


def store_scan_results(data: pd.DataFrame, google_query: str) -> None:
    """
    stores the results of a scan in the database
    Args:
        data (pd.DataFrame): all_places dataframe, the output of scan_hex
        google_query (str): the query we used to search for the places
    """
    update_query={
    "$addToSet": 
        {"all_queries": google_query}
        }

    conn = MongoConnection(collection='gmaps_places_output')
    conn.insert_update_duplicated(
        data=data, 
        orient='column',
        update_query=update_query
    )


def _add_location_columns(all_places: pd.DataFrame, search_hex7) -> pd.DataFrame:
    """
    a function to declutter scan_hex
    adds location related columns to the dataframe
    """
    all_places = all_places.copy()
    all_places.loc[:, 'coords'] = all_places.geometry.apply(_get_lat_long)
    all_places.drop(columns='geometry', inplace=True)
    all_places.loc[:, f'loc_hex{SUB_HEX_SIZE}'] = all_places.coords.apply(
        lambda x: h3.geo_to_h3(*x, SUB_HEX_SIZE))
    all_places.loc[:, 'search_hex7'] = search_hex7
    all_places.loc[:, 'loc_hex7'] = all_places.coords.apply(lambda x: h3.geo_to_h3(*x, 7))
        
    return all_places


def _add_query_boilerplate(all_places: pd.DataFrame, query: str, data_id: str) -> pd.DataFrame:
    """
    a function to declutter scan_hex
    adds query related columns to the dataframe, as well as some mandatory columns
    """
    all_places = all_places.copy()
    all_places.loc[:, 'query'] = query
    all_places.loc[:, 'query_ds'] = str(date.today())
    all_places.loc[:, 'reviews_extracted_local_lang'] = False
    all_places.loc[:, 'review_extr_ds_local_lang'] = None
    all_places.loc[:, 'reviews_extracted_en'] = False
    all_places.loc[:, 'review_extr_ds_en'] = None
    all_places.loc[:, 'data_id'] = data_id
    all_places.loc[:, "added_at"] = datetime.now()
    all_places['all_queries'] = [[query]] * len(all_places)
    return all_places
     
     
def _get_lat_long(geometry: Dict[str, Dict[str, float]]):
    """
    extracts lat-lng tuple from google geometry dict
    """
    return geometry['location']['lat'], geometry['location']['lng']


def places_output_schema():
    """
    probably not the best place for this, but it's the only place it's used
    """
    return {
            'formatted_address',
            'name',
            'place_id',
            'types_en',
            'loc_hex7',
            'loc_hex9',
            'coords',
            'search_hex7',
            'search_hex9',
            'search_query',
            'query_ds',
            'reviews_extracted_local_lang',
            'review_extr_ds_local_lang',
            'reviews_extracted_en',
            'review_extr_ds_en',
            'permanent_closed',
            'data_id',
            'all_queries',
            'priority',
            'unserpable',
            "added_at"
    }
    
    
def find_place_id(place_name: str, place_coord: Tuple[float, float], data_id: str) -> Union[dict, None]:
    """
    SERP ARI sometimes returns extra places on top of the searched ones
    These places don't have a place_id, 
    __This function searches for place_id and place details using place name and coordinates__
    This function can return None if no place_id is found
    Otherwise returns output of gmaps.place API which is similar to gmaps.places API
    """
    gmaps = googlemaps.Client(key=os.environ['GAPI'])
    lat, lon = place_coord
    place_id_results = gmaps.find_place(
        input=place_name, 
        location_bias=f"point:{lat},{lon}", 
        input_type="textquery"
        )
    try:
        place_id = place_id_results['candidates'][0]['place_id']
    except IndexError:
        print('no place id found for', place_name)
    else:
        place_details = gmaps.place(place_id=place_id, language='es')['result']
        if place_details['name'] == place_name:
            place_details['data_id'] = data_id
            return place_details
        else:
            print("place name in SERP doesn't match place found by google")
          
          
def format_places_df(place_dict: Dict[str, Any], data_id: str) -> pd.DataFrame:
    """
    returns: a dataframe, matching the schema of gmaps_places_output 
    ready to be inserted into the database
    """
    # TODO change to copying the dict
    handle_place_types(place_dict)
    add_default_priority(place_dict)
    add_default_unserpable(place_dict)
    place_details_df = pd.DataFrame(dict( # noqa C402
        (k, [v]) for k, v in place_dict.items() 
        if (k in places_output_schema() or k == 'geometry')
        ))
    place_details_df = _add_location_columns(place_details_df, None)
    place_details_df = _add_query_boilerplate(place_details_df, query=None, data_id=data_id)
    return place_details_df


def handle_place_types(place_dict: Dict[str, Any]) -> Dict[str, Any]:
    if 'types' in place_dict:
        place_dict['types_en'] = place_dict['types']
        place_dict.pop('types')
    else:
        place_dict['types_en'] = []


def add_default_priority(place_dict: Dict[str, Any]) -> Dict[str, Any]:
    if 'priority' not in place_dict:
        place_dict['priority'] = 1


def add_default_unserpable(place_dict: Dict[str, Any]) -> Dict[str, Any]:
    if 'unserpable' not in place_dict:
        place_dict['unserpable'] = False
    

