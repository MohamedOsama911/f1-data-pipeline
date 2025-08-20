# ingestion/utils/api_client.py
import requests
import logging

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_URL = "https://api.jolpi.ca/ergast/f1"

def get_data(endpoint: str):
    """Generic function to fetch data from an API endpoint."""
    try:
        response = requests.get(f"{BASE_URL}/{endpoint}.json?limit=1000")
        response.raise_for_status()
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

def get_all_season_results(season: int):
    """Fetches all race result objects for an entire season."""
    logging.info(f"Fetching all race results for the {season} season...")
    all_races = []
    total_rounds = get_total_rounds(season)
    if total_rounds == 0: return []
    
    for r in range(1, total_rounds + 1):
        data = get_data(f"{season}/{r}/results")
        if data and data['MRData']['RaceTable']['Races']:
            all_races.extend(data['MRData']['RaceTable']['Races'])
    return all_races

def get_all_season_qualifying(season: int):
    """Fetches all qualifying result objects for an entire season."""
    logging.info(f"Fetching all qualifying results for the {season} season...")
    all_races = []
    total_rounds = get_total_rounds(season)
    if total_rounds == 0: return []

    for r in range(1, total_rounds + 1):
        data = get_data(f"{season}/{r}/qualifying")
        if data and data['MRData']['RaceTable']['Races']:
            all_races.extend(data['MRData']['RaceTable']['Races'])
    return all_races

def get_all_season_pit_stops(season: int):
    """Fetches all pit stops for an entire season, injecting the round number."""
    logging.info(f"Fetching all pit stops for the {season} season...")
    all_pit_stops = []
    total_rounds = get_total_rounds(season)
    if total_rounds == 0: return []

    for r in range(1, total_rounds + 1):
        data = get_data(f"{season}/{r}/pitstops")
        if data and data['MRData']['RaceTable']['Races']:
            pit_stops_for_round = data['MRData']['RaceTable']['Races'][0].get('PitStops', [])
            # Crucial step: add the round number to each pit stop record
            for pit_stop in pit_stops_for_round:
                pit_stop['round'] = r
            all_pit_stops.extend(pit_stops_for_round)
    return all_pit_stops

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