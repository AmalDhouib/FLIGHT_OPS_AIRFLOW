import requests
import json 
from datetime import datetime
# Import Path from pathlib to handle file and folder paths easily
from pathlib import Path
URL = "https://opensky-network.org/api/states/all"

def run_bronze_ingestion(**context):
      
    response = requests.get(URL,timeout=30)
    # Raise an exception if the HTTP request returned an error status
    response.raise_for_status()
    data = response.json()
    # Get current UTC date and time formatted as a string
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    path = Path(f"/opt/airflow/data/bronze/flights_{timestamp}.json")
    with open(path ,"w") as f:
        json.dump(data,f)
    context["ti"].xcom_push(key="bronze_file",value=str(path))
    
