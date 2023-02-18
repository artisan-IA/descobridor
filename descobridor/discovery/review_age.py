from datetime import datetime, timedelta, date
from dataclasses import dataclass


class ReviewAge:
     """
     Class that operates with a review age string
     eg. "Hace 2 semanas" with age in days and with a date of the review.
     It is used in review extration 
     as well as in infrastructure of scraping and re-scraping.
     """
     def __init__(self, scrape_ds: str, string_repr: str, language:str):
          self.scrape_ds = scrape_ds
          self.string_repr = string_repr
          self.language = language
          self.scrape_datetime = datetime.strptime(scrape_ds, '%Y-%m-%d').replace(
               hour=0, 
               minute=0,
               second=0, 
               microsecond=0
               )
          self.days_before_scrape = self.string_to_days(string_repr, language)
          self.review_date = self.scrape_datetime - timedelta(days=self.days_before_scrape)
          self.age_in_days = (datetime.today() - self.review_date).days
          self.precision = self.get_precision()
     
     @staticmethod
     def string_to_days(string: str, language: str) -> int:
          if language == 'ES':
               return ReviewAge.str_es_to_days_before_scrape(string)
          elif language == "EN":
               return ReviewAge.str_en_to_days_before_scrape(string)
     
     def __eq__(self, __o: "ReviewAge") -> bool:
          self.review_date == __o.review_date
          
     def __lt__(self, __o: "ReviewAge") -> bool:
          self.review_date < __o.review_date
          
     def __gt__(self, __o: "ReviewAge") -> bool:
          self.review_date > __o.review_date
          
     def __le__(self, __o: "ReviewAge") -> bool:
          self.review_date <= __o.review_date
          
     def __ge__(self, __o: "ReviewAge") -> bool:
          self.review_date >= __o.review_date
          
     def __str__(self) -> str:
          return self.review_date.strftime('%Y-%m-%d')
     
     def __repr__(self) -> str:
          return f'ReviewAge({self.review_date.strftime("%Y-%m-%d")})'
     
     def get_precision(self) -> str:
          """
          Returns the precision of the review date
          """
          if self.days_before_scrape == 0:
               return 0
          elif self.days_before_scrape < 7:
               return 1
          elif self.days_before_scrape < 30:
               return 7
          elif self.days_before_scrape < 365:
               return 30
          else:
               return 365
     
     @staticmethod
     def str_es_to_days_before_scrape(review_age_str: str) -> int:
          """
          Extract the age of the review in days from the string
          """
          age_list = review_age_str.lower().split(' ')
          assert age_list[0] == 'hace'
          if age_list[2] in ('minutos', 'minuta', 'secundos', 'hora', 'horas'):
               return 0
          elif age_list[2] in ('día', 'días'):
               if age_list[1] == 'un':
                    return 1
               return int(age_list[1])
          elif age_list[2] in ('semana', 'semanas'):
               if age_list[1] == 'una':
                    return 1
               return int(age_list[1]) * 7
          elif age_list[2] in ('mes', 'meses'):
               if age_list[1] == 'un':
                    return 30
               return int(age_list[1]) * 30
          elif age_list[2] in ('año', 'años'):
               if age_list[1] == 'un':
                    return 365
               return int(age_list[1]) * 365
          else:
               raise ValueError(f"Unknown age unit: {age_list[2]}")
     