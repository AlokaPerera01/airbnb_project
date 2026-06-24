import os
import requests
import logging
import time
import json
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def download_file(url, dest_path, max_retries=3):
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Downloading {url} (attempt {attempt}/{max_retries})")
            response = requests.get(url, timeout=60)
            if response.status_code == 200:
                with open(dest_path, "wb") as f:
                    f.write(response.content)
                logger.info(f"Saved to {dest_path}")
                return True
            else:
                logger.warning(f"HTTP {response.status_code} for {url}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Attempt {attempt} failed: {e}")
            if attempt < max_retries:
                logger.info("Retrying in 5 seconds...")
                time.sleep(5)
    logger.error(f"All {max_retries} attempts failed for {url}")
    return False


def run_pipeline(city_path, snapshot_date, base_folder):
    start_time = datetime.now()
    logger.info(f"Pipeline started for {city_path} at {start_time}")

    dest_folder = os.path.join(base_folder, city_path.split("/")[-1])
    os.makedirs(dest_folder, exist_ok=True)

    base_url = f"http://data.insideairbnb.com/{city_path}/{snapshot_date}"
    files = {
        "listings.csv.gz": f"{base_url}/data/listings.csv.gz",
        "calendar.csv.gz": f"{base_url}/data/calendar.csv.gz",
        "reviews.csv.gz": f"{base_url}/data/reviews.csv.gz",
        "listings_summary.csv": f"{base_url}/visualisations/listings.csv",
        "reviews_summary.csv": f"{base_url}/visualisations/reviews.csv",
        "neighbourhoods.csv": f"{base_url}/visualisations/neighbourhoods.csv",
        "neighbourhoods.geojson": f"{base_url}/visualisations/neighbourhoods.geojson",
    }

    results = {}
    for filename, url in files.items():
        dest_path = os.path.join(dest_folder, filename)
        if os.path.exists(dest_path):
            logger.info(f"Skipping {filename} - already exists")
            results[filename] = "skipped"
            continue
        success = download_file(url, dest_path)
        results[filename] = "success" if success else "failed"

    end_time = datetime.now()
    duration = (end_time - start_time).seconds

    metadata = {
        "city": city_path.split("/")[-1],
        "snapshot_date": snapshot_date,
        "run_timestamp": start_time.isoformat(),
        "duration_seconds": duration,
        "files": results,
    }

    metadata_path = os.path.join(base_folder, "..", "processed", "pipeline_metadata.json")
    if os.path.exists(metadata_path):
        with open(metadata_path, "r") as f:
            all_metadata = json.load(f)
    else:
        all_metadata = []
    all_metadata.append(metadata)
    with open(metadata_path, "w") as f:
        json.dump(all_metadata, f, indent=2)

    logger.info(f"Pipeline completed in {duration} seconds")
    logger.info(f"Results: {results}")
    return metadata


if __name__ == "__main__":
    run_pipeline(
        city_path="thailand/central-thailand/bangkok",
        snapshot_date="2025-09-27",
        base_folder="data/raw"
    )
