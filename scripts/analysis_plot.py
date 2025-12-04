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

def plot_contribution_heatmap(df):
    """Heatmap of commit intensity per author over weeks"""
    df['week'] = df['date'].dt.to_period('W').apply(lambda r: r.start_time)
    for repo in df['repo'].unique():
        subset = df[df['repo']==repo]
        heat = subset.groupby(['author','week']).size().unstack(fill_value=0)
        plt.figure(figsize=(12,6))
        sns.heatmap(heat, cmap='Blues', linewidths=.5)
        plt.title(f"Weekly Commit Heatmap for {repo}")
        plt.ylabel("Author")
        plt.xlabel("Week")
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        safe_repo = repo.replace("/", "_")
        plt.savefig(f"data/heatmap_{safe_repo}.png")
        plt.close()

def plot_commit_vs_contributors(df):
    """Comparative scatter plot: total commits vs number of contributors per repo"""
    summary = df.groupby('repo').agg(
        total_commits=('author','count'),
        contributors=('author','nunique')
    ).reset_index()
    
    plt.figure(figsize=(8,6))
    sns.scatterplot(data=summary, x='contributors', y='total_commits', hue='repo', size='total_commits', legend=False, s=100)
    plt.title("Total Commits vs Number of Contributors per Repo")
    plt.xlabel("Number of Contributors")
    plt.ylabel("Total Commits")
    plt.tight_layout()
    plt.savefig("data/commits_vs_contributors.png")
    plt.close()

if __name__ == "__main__":
    plot_commit_activity()
    plot_top_contributors()
