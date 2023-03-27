import yaml
from typing import Dict


def get_localization(country) -> Dict[str, str]:
    """
    :returns: dict with keys: country, language, domain
    """
    loc_config = yaml.safe_load(open("localization.yaml"))
    return {
        'country': country, 
        'language': loc_config[country]["language"],
        'domain': loc_config[country]["domain"]
    }
