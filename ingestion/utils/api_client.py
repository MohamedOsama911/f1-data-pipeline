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

def get_race_results(season: int, round: int):
    """Fetches race results for a specific season and round."""
    logging.info(f"Fetching race results for {season} round {round}...")
    data = get_data(f"{season}/{round}/results")
    return data['MRData']['RaceTable']['Races'][0]['Results'] if data and data['MRData']['RaceTable']['Races'] else []

def get_qualifying_results(season: int, round: int):
    """Fetches qualifying results for a specific season and round."""
    logging.info(f"Fetching qualifying results for {season} round {round}...")
    data = get_data(f"{season}/{round}/qualifying")
    return data['MRData']['RaceTable']['Races'][0]['QualifyingResults'] if data and data['MRData']['RaceTable']['Races'] else []

def get_pit_stops(season: int, round: int):
    """Fetches pit stop data for a specific season and round."""
    logging.info(f"Fetching pit stops for {season} round {round}...")
    data = get_data(f"{season}/{round}/pitstops")
    return data['MRData']['RaceTable']['Races'][0]['PitStops'] if data and data['MRData']['RaceTable']['Races'] else []

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

