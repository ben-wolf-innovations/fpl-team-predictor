import requests
import json
import os
import time

# Config
FPL_BASE_URL = "https://fantasy.premierleague.com/api/"
PROTOCOL = "HIST"  # Options: "HIST" or "LATEST"
SEASON_START_YEAR = 2025
SEASON_END_YEAR = 26

# Step 1: Fetch bootstrap-static
bootstrap_url = FPL_BASE_URL + "/bootstrap-static"
bootstrap_response = requests.get(bootstrap_url)
bootstrap_response.raise_for_status()
bootstrap_data = bootstrap_response.json()

# Step 2: Determine latest finished gameweek
events = bootstrap_data.get("events", [])
finished_events = [e for e in events if e.get("finished")]
latest_event_id = max(e["id"] for e in finished_events) if finished_events else 0
gw_str = f"{latest_event_id:02d}"

# Step 3: Define folder path
SEASON_FOLDER = f"data/{SEASON_START_YEAR}_{SEASON_END_YEAR}/gw_{gw_str}"
os.makedirs(SEASON_FOLDER, exist_ok=True)

# Step 4: Save bootstrap-static
with open(f"{SEASON_FOLDER}/bootstrap_static.json", "w") as f:
    json.dump(bootstrap_data, f, indent=2)

# Step 5: Fetch and save fixtures with correct future param
future_param = 1 if PROTOCOL == "LATEST" else 0
fixtures_url = FPL_BASE_URL + f"/fixtures?future={future_param}"
fixtures_response = requests.get(fixtures_url)
fixtures_response.raise_for_status()
fixtures_data = fixtures_response.json()

with open(f"{SEASON_FOLDER}/fixtures.json", "w") as f:
    json.dump(fixtures_data, f, indent=2)

# # Step 6: Get all player IDs
# players = bootstrap_data.get("elements", [])
# player_ids = [player["id"] for player in players]

# # Step 7: Fetch player history
# all_player_data = []

# for pid in player_ids:
#     url = f"{FPL_BASE_URL}element-summary/{pid}/"
#     try:
#         response = requests.get(url)
#         response.raise_for_status()
#         player_data = response.json()

#         history = player_data.get("history", [])

#         if PROTOCOL == "LATEST":
#             history = [h for h in history if h["round"] == latest_event_id]
#             player_record = {
#                 "id": pid,
#                 "history": history
#             }
#         else:
#             past_history = player_data.get("history_past", [])
#             player_record = {
#                 "id": pid,
#                 "history": history,
#                 "past_history": past_history
#             }

#         all_player_data.append(player_record)
#         print(f"Fetched history for player {pid}")
#         time.sleep(0.3)

#     except Exception as e:
#         print(f"Error fetching player {pid}: {e}")

# # Step 8: Save player data
# if PROTOCOL == "HIST":
#     filename = f"all_players_stats_{SEASON_START_YEAR}_{SEASON_END_YEAR}_gw_{gw_str}.json"
# else:
#     filename = f"player_stats_{SEASON_START_YEAR}_{SEASON_END_YEAR}_gw_{gw_str}.json"

# with open(f"{SEASON_FOLDER}/{filename}", "w") as f:
#     json.dump(all_player_data, f, indent=2)

# print(f"Saved player data to {SEASON_FOLDER}/{filename}")