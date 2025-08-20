import requests
import logging

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_URL = "https://api.jolpi.ca/ergast/f1"

def get_data(endpoint: str):
    """Generic function to fetch data from an API endpoint."""
    try:
        response = requests.get(f"{BASE_URL}/{endpoint}.json?limit=1000")
        response.raise_for_status()  # Raises an HTTPError for bad responses (4XX or 5XX)
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from {endpoint}: {e}")
        return None

def get_total_rounds(season: int) -> int:
    """Gets the total number of rounds for a given season."""
    data = get_data(f"{season}")
    if data and 'MRData' in data and 'RaceTable' in data['MRData'] and 'Races' in data['MRData']['RaceTable']:
        return len(data['MRData']['RaceTable']['Races'])
    return 0

def get_driver_standings(season: int):
    """Fetches driver standings for a given season."""
    logging.info(f"Fetching driver standings for {season}...")
    data = get_data(f"{season}/driverStandings")
    return data['MRData']['StandingsTable']['StandingsLists'][0]['DriverStandings'] if data and data['MRData']['StandingsTable']['StandingsLists'] else []

def get_constructor_standings(season: int):
    """Fetches constructor standings for a given season."""
    logging.info(f"Fetching constructor standings for {season}...")
    data = get_data(f"{season}/constructorStandings")
    return data['MRData']['StandingsTable']['StandingsLists'][0]['ConstructorStandings'] if data and data['MRData']['StandingsTable']['StandingsLists'] else []

def get_races_data(season: int, data_key: str):
    """
    A generic function to get the full race object list for a given data type
    (results, qualifying, pitstops).
    """
    all_races = []
    total_rounds = get_total_rounds(season)
    if total_rounds == 0:
        return []

    for r in range(1, total_rounds + 1):
        logging.info(f"Fetching {data_key} for {season} round {r}...")
        endpoint = f"{season}/{r}/{data_key}"
        data = get_data(endpoint)
        if data and data['MRData']['RaceTable']['Races']:
            all_races.extend(data['MRData']['RaceTable']['Races'])
            
    return all_races