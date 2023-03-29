DIRECT_EXCHANGE = 'direct_exchange'
TOPIC_EXCHANGE = 'topic_exchange'
COUNTRY = 'es'

SERP_QUEUE_NAME = 'serp_queue'
SERP_BATCH_SIZE = 1
SERP_QUEUE_MAX_LENGTH = 20
SERP_QUEUE_MAX_PRIORITY = 10

GMAPS_SCRAPE_QUEUE_MAX_LENGTH = 10
GMAPS_SCRAPE_QUEUE_MAX_PRIORITY = 10
GMAPS_SCRAPE_BATCH_SIZE = 1
GMAPS_SCRAPE_KEY = 'gmaps_scrape'
GMAPS_SCRAPE_FREQ_D = 28

VPN_COUNTRIES = ("es", "pt")
VPN_WAIT_TIME_S = 3600 * 24 # how long to wait until we can use VPN again
EXPIRE_CURR_VPN_S = 3600 # how long current VPN is valid
VPN_NOTHING_WORKS_SLEEP_S = 3600 * 24 # if no VPN is available, suspend all operations for this time
CURRENT_VPN_SUFFIX = 'current_vpn'

GMAPS_SCRAPER_INTERFACE = {
    "place_id", "data_id", "name", "language", "country_domain", "priority", "last_scraped"
}
