import requests
import json

FPL_BASE_URL = "https://fantasy.premierleague.com/api/"

# Only include endpoints that return top-level JSON directly
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

    with open(f"{endpoint_name}.json", "w") as f:
        json.dump(data, f, indent=2)

for name, path in ENDPOINTS.items():
        fetch_and_save(name, path)