
Web Scraping, Transformation, DVC Versioning, and Git Push Pipeline

This repository contains an Airflow DAG script for automating a data pipeline that involves web scraping, data transformation, DVC (Data Version Control) integration, and synchronization with a Git repository. The pipeline fetches data from specified sources, processes it, versions the transformed data and metadata using DVC, and pushes the changes to a remote Git repository.

Usage:

Environment Setup:

Ensure Python is installed.
Install required libraries:
Copy code
pip install apache-airflow beautifulsoup4 requests
pip install dvc
Clone Repository:

bash
Copy code
git clone https://github.com/mtalhastar/mlops_assignment2.git
cd mlops_assignment2
Initialize DVC:

csharp
Copy code
dvc init
Configure Remote DVC Repository:

csharp
Copy code
dvc remote add origin <URL>
Update DAG Script:

Customize sources in mlops_dag.py.
Run Airflow Scheduler:

Copy code
airflow scheduler
Access Airflow Web Interface:

Open http://localhost:8080 in a web browser.
Trigger mlops_dag manually or let it run based on schedule.
Monitor Execution:

Check logs in Airflow UI.
