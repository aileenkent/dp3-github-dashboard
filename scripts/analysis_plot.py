import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def plot_commit_activity():
    con = duckdb.connect(database="data/commits.db")
    df = con.execute("SELECT * FROM commits").fetchdf()
    
    # Aggregate commits per week
    df['week'] = df['date'].dt.to_period('W').apply(lambda r: r.start_time)
    weekly = df.groupby(['repo','week']).size().reset_index(name='commit_count')
    
    plt.figure(figsize=(12,6))
    sns.lineplot(data=weekly, x='week', y='commit_count', hue='repo', marker='o')
    plt.title("Weekly Commit Activity per Repository")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("data/weekly_commits.png")
    plt.show()
    con.close()

def plot_top_contributors():
    con = duckdb.connect(database="data/commits.db")
    df = con.execute("SELECT * FROM commits").fetchdf()
    
    top_authors = df.groupby(['repo','author']).size().reset_index(name='commits')
    top_authors = top_authors.sort_values(['repo','commits'], ascending=[True, False])
    
    for repo in top_authors['repo'].unique():
        plt.figure(figsize=(8,4))
        subset = top_authors[top_authors['repo']==repo].head(10)
        sns.barplot(data=subset, x='commits', y='author')
        plt.title(f"Top Contributors for {repo}")
        plt.tight_layout()
        plt.savefig(f"data/top_contributors_{repo}.png")
        plt.show()
    con.close()

if __name__ == "__main__":
    plot_commit_activity()
    plot_top_contributors()
