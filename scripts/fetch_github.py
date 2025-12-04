import os
import requests
import time
import json
from datetime import datetime

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
HEADERS = {"Authorization": f"token{GITHUB_TOKEN}"}

def fetch_commits(owner: str, repo: str, per_page=100, max_pages=10):
    """Fetch commits form a Github repository."""
    all_commits = []
    for page in range(1, max_pages + 1):
        url = f"https://api.github.com/repos/{owner}/{repo}/commits"
        params = {"per_page": per_page, "page": page}
        response = requests.get(url, headers=HEADERS, params=params)
        if response.status_code != 200:
            print(f"Error fetching page {page}: {response.text}")
            break
        data = response.json()
        if not data:
            break
        all_commits.extend(data)
        time.sleep(1)
    os.makedirs("data/raw", exist_ok=True)
    with open(f"data/raw{repo}_commits.json", "w") as f:
        json.dump(all_commits, f, indent=2)
    print(f"Saved {len(all_commits)} commits for {repo}")
    return all_commits

def fetch_events(owner: str, repo: str, per_page=100, max_pages=10):
    """Fetch recent events from Github"""
    all_events = []
    for page in range(1, max_pages + 1):
        url = f"https://api.github.com/repos/{owner}/{repo}/events"
        params = {"per_page": per_page, "page": page}
        response = requests.get(url, headers=HEADERS, params=params)
        if response.status_code != 200:
            print(f"Error fetching page {page}: {response.text}")
            break
        data = response.json()
        if not data:
            break
        all_events.extend(data)
        time.sleep(1)
    os.makedirs("data/raw", exist_ok=True)
    with open(f"data/raw/{repo}_events.json", "w") as f:
        json.dump(all_events, f, indent=2)
    print(f"Saved {len(all_events)} events for {repo}")
    return all_events

if __name__ == "__main__":
    repos = ["pandas-dev/pandas", "numpy/numpy", "matplotlib/matplotlib", "scikit-learn/scikit-learn", "seaborn/seaborn"]
    for r in repos: 
        owner, repo = r.split("/")
        fetch_commits(owner, repo)
        fetch_events(owner, repo)
