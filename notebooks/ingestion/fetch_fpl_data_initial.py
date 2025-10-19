# import requests
# import json

# FPL_BASE_URL = "https://fantasy.premierleague.com/api/"

# # Only include endpoints that return top-level JSON directly
# ENDPOINTS = {
#     "bootstrap_static": "bootstrap-static/",
#     "fixtures": "fixtures/",
#     "event_status": "event-status/"
# }

# def fetch_and_save(endpoint_name, endpoint_path):
#     url = FPL_BASE_URL + endpoint_path
#     response = requests.get(url)
#     response.raise_for_status()
#     data = response.json()

#     with open(f"{endpoint_name}.json", "w") as f:
#         json.dump(data, f, indent=2)

# for name, path in ENDPOINTS.items():
#         fetch_and_save(name, path)

import requests
import json
import os
import time

FPL_BASE_URL = "https://fantasy.premierleague.com/api/"
SEASON_FOLDER = "2025_26_gw_07"  # Your initial load folder

# Endpoints
ENDPOINTS = {
    "bootstrap_static": "bootstrap-static/",
    "fixtures": "fixtures/",
    "event_status": "event-status/"
}

def fetch_and_save(endpoint_name, endpoint_path):
    url = FPL_BASE_URL + endpoint_path
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    os.makedirs(SEASON_FOLDER, exist_ok=True)
    with open(f"{SEASON_FOLDER}/{endpoint_name}.json", "w") as f:
        json.dump(data, f, indent=2)
    return data

#Pull static data
bootstrap_data = fetch_and_save("bootstrap_static", ENDPOINTS["bootstrap_static"])
fixtures_data = fetch_and_save("fixtures", ENDPOINTS["fixtures"])
event_status_data = fetch_and_save("event_status", ENDPOINTS["event_status"])

#Get all player IDs
players = bootstrap_data['elements']
player_ids = [player['id'] for player in players]


#Fetch full history and past_history for each player
all_player_data = []

for pid in player_ids:
    url = f"{FPL_BASE_URL}element-summary/{pid}/"
    try:
        response = requests.get(url)
        response.raise_for_status()
        player_data = response.json()

        all_player_data.append({
            'id': pid,
            'history': player_data.get('history', []),
            'past_history': player_data.get('history_past', [])
        })

        print(f"Fetched full history for player {pid}")
        time.sleep(0.5) 

    except Exception as e:
        print(f"Error fetching player {pid}: {e}")

#Save full player history to 1 file
with open(f"{SEASON_FOLDER}/player_full_history.json", "w") as f:
    json.dump(all_player_data, f, indent=2)

print(f"Saved full player history to {SEASON_FOLDER}/player_full_history.json")