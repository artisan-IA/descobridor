from datetime import date, datetime
from typing import Any, Dict, Optional, Tuple
from serpapi import GoogleSearch
import os
import pandas as pd
from dotenv import load_dotenv

from truby.db_connection import MongoConnection
import src.gmaps_tools.hex_scan as hs


load_dotenv()

def serp_search_no_cache(place_name: str, place_coord: Tuple[float, float]) -> Dict[str, Any]:
     """
     uses SERP API to get data_id of a given place
     Always talks to SERP API, never uses cache
     """
     params = {
          "api_key": os.environ["SERP"],
          "device": "desktop",
          "type": "search",
          "engine": "google_maps",
          "google_domain": "google.es",
          "hl": "es",
          "ll": f"@{place_coord[0]},{place_coord[1]},15z",
          "q": place_name
     }

     print('using SERP')
     search = GoogleSearch(params)
     serp_output = search.get_dict()
     serp_output = format_serp_output(params, serp_output)
     
     # SERP API returns surpluss of results.
     # We use them to update gmaps_places_output with data_id's and new entries
     for entry in serp_output['place_results']:
          link_back_to_place_id(entry)
     return serp_output

def serp_search_place(
     place_id: str, 
     place_name: str, 
     place_coord: Tuple[float, float],
     use_cache: Optional[bool] = True
     ) -> Dict[str, Any]:
     """
     searches for data_id of a given place
     data_id can be easily found in SERP API.
     SERP API is expensive and returns supruss of results.
     So we cache them and do not requery ever.
     """
     if use_cache:
          cached_output = read_serp_cache(place_id)
          if cached_output:
               print('using cached')
               return cached_output
     
     serp_output = serp_search_no_cache(place_name, place_coord)
     
     try:
          cache_serp_output(serp_output)
     except: # noqa E722
          print(f"could not save {place_name=}")
     return read_serp_cache(place_id)


def link_back_to_place_id(serp_entry: Dict[str, Any]) -> None:
     """
     When SERP API returns a surpluss of results, they are useless,
     because they don't have a place_id.
     This function searches for place_id and forms a proper gmaps_places_output entry
     It uploads these to gmaps_places_output 
     inserting if new or updating if place_id already exists.
     """
     coords = (serp_entry['gps_coordinates']['latitude'], 
               serp_entry['gps_coordinates']['longitude'])
     place_details_dict = hs.find_place_id(serp_entry['title'], coords)
     # it is theoretically possible that place_id is not found
     if place_details_dict:
          place_id = place_details_dict['place_id']
          with MongoConnection('places') as conn:
               result = conn.collection.update_one(
                    {'place_id': place_id}, 
                    {'$set': {'data_id': serp_entry['data_id']}}
               )
          if result.raw_result['n'] == 0: # no entry found
               place_details = hs.format_place_details(place_details_dict, serp_entry['data_id'])
               record = place_details.loc[0].to_dict()
               with MongoConnection('gmaps_places_output') as conn:
                    conn.collection.insert_one(record)
               print(f"new entry for {serp_entry['title']}")
          else:
               print(f"updated {serp_entry['title']} with data_id {serp_entry['data_id']}")
     

### HEPLERS ###


def convert_local_results_to_place_results(serp_output: Dict[str, Any], params: Dict[str, Any]):
     local_results = serp_output['local_results']
     place_results = {
          'search_metadata': [serp_output['search_metadata']] * len(local_results),
          'search_parameters': [serp_output['search_parameters']] * len(local_results),
          'search_information': [serp_output['search_information']] * len(local_results),
          'place_results': [],
          'local_results': [],
          'query_params': [f"{params['q']}_{params['ll']}"] * len(local_results)
     }
     for entry in local_results:
          # at the end, we might be having several entries for the same place
          place_results['local_results'].append(f"ll_{entry['position']}")
          place_results['place_results'].append(entry)
          
     return place_results


def get_all_places_from_place_results(serp_output: Dict[str, Any], params: Dict[str, Any]):
     # there are places with data_id in people_also_search_for results. Extracting them here.
     people_also_search_for = serp_output['place_results']['people_also_search_for'][0]['local_results']
     # forming dictionary to store all places. + 1 is for the place_results field
     place_results = {
          'search_metadata': [serp_output['search_metadata']] * (len(people_also_search_for) + 1),
          'search_parameters': [serp_output['search_parameters']] * (len(people_also_search_for) + 1),
          'search_information': [serp_output['search_information']] * (len(people_also_search_for) + 1),
          'place_results': [],
          'local_results': [],
          'query_params': [f"{params['q']}_{params['ll']}"] * (len(people_also_search_for) + 1)
     }
     # appending the place results with data_id from the place_results field
     place_results['place_results'].append(serp_output['place_results'])
     place_results['local_results'].append('pr')
     
     # appending the place results with data_id from the people_also_search_for field
     for entry in people_also_search_for:
          place_results['place_results'].append(entry)
          place_results['local_results'].append(f"pasf_{entry['position']}")
     return place_results
     

def format_serp_output(params: Dict[str, Any], serp_output):
     if 'place_results' in serp_output:
          place_results = get_all_places_from_place_results(serp_output, params)
          formatted = pd.DataFrame(place_results)
     elif 'local_results' in serp_output:
          formatted = pd.DataFrame(convert_local_results_to_place_results(serp_output, params))
     else:
          raise ValueError(f"Unexpected SERP output: {serp_output}")

     formatted['query_ds'] = date.today().strftime("%Y-%m-%d")
     formatted['query_dt'] = datetime.now()
     return formatted

def cache_serp_output(serp_output: pd.DataFrame):
     conn = MongoConnection("serp_cache")
     conn.df_to_collection(serp_output)
     
     
def read_serp_cache(place_id: str):
     with MongoConnection("serp_cache") as conn:
          cached = list(conn.collection.find({'place_results.place_id': place_id}))

     # can be multiple results, because SERP can return references to the same places
     # in different fields. We only need data_id, so taking any of them is fine.
     if len(cached) > 0:
          return cached[0]
     
     

     
     




