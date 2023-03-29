import yaml
from typing import Dict


def get_localization(country) -> Dict[str, str]:
    """
    :returns: dict with keys: country, language, domain
    """
    loc_config = yaml.safe_load(open("localization.yaml"))
    return {
        'country': country, 
        'language': loc_config[country]["language"]["language"],
        'domain': loc_config[country]["domain"],
        'food': loc_config[country]["language"]["food"],
        'service': loc_config[country]["language"]["service"],
        'athmosphere': loc_config[country]["language"]["athmosphere"]
    }
    
def get_localized_parser(language):
    loc_config = yaml.safe_load(open("localization.yaml"))
    return loc_config["review_parser"][language]
