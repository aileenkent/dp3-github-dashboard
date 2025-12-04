from prefect import task, Flow
import subprocess

@task
def fetch_data():
    subprocess.run(["python", "scripts/fetch_github.py"], check=True)

@task
def transform_data():
    subprocess.run(["python", "scripts/clean_transform.py"], check=True)

@task
def analyze():
    subprocess.run(["python", "scripts/analysis_plot.py"], check=True)

with Flow("GitHub Dashboard Pipeline") as flow:
    fetch = fetch_data()
    transform = transform_data(upstream_tasks=[fetch])
    analyze_task = analyze(upstream_tasks=[transform])

if __name__ == "__main__":
    flow.run()
