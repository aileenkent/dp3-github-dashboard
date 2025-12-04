import duckdb
import pandas as pd
import json
import glob

def load_commits_to_duckdb():
    """Load raw commits JSON into DuckDB for analysis."""
    con = duckdb.connect(database="data/commits.db")
    commit_files = glob.glob("data/raw/*_commits.json")
    for file in commit_files:
        with open(file) as f:
            data = json.load(f)
        records = []
        for c in data:
            try:
                author = c.get("commit", {}).get("author", {}).get("name")
                date = c.get("commit", {}).get("author", {}).get("date")
                additions = c.get("stats", {}).get("additions", 0)
                deletions = c.get("stats", {}).get("deletions", 0)
                repo_name = file.split("/")[-1].replace("_commits.json","")
                records.append((repo_name, author, date, additions, deletions))
            except Exception as e:
                continue
        df = pd.DataFrame(records, columns=["repo","author","date","additions","deletions"])
        # Convert date to datetime
        df["date"] = pd.to_datetime(df["date"])
        # Write to DuckDB
        con.execute("""
            CREATE TABLE IF NOT EXISTS commits AS SELECT * FROM df
            ON CONFLICT DO NOTHING
        """)
        con.execute("INSERT INTO commits SELECT * FROM df")
    print("Loaded commits into DuckDB.")
    con.close()

if __name__ == "__main__":
    load_commits_to_duckdb()
