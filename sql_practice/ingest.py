# Python script to ingest data from API
# API:

import requests
import pandas as pd
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Build API client class
class CMSProviderAPIClient:
    """Fetches CMS provider data from API."""

    SQL_API_URL = "https://data.cms.gov/provider-data/api/1/datastore/sql"
    DISTRIBUTION_ID = "a106bb7d-22a0-5be5-be84-af58b992c236"

    def __init__(self, batch_size=500, timeout=30):
        self.batch_size = batch_size
        self.timeout = timeout
        self.session = requests.Session()

    # Fetch data
    def fetch_data(self, limit=None):
        """Fetch data from the API with pagination."""
        logger.info(f"Starting fetch from {self.SQL_API_URL}")

        all_data = []
        offset = 0

        while True:
            # Build the SQL query as a STRING
            sql_query = f"[SELECT * FROM {self.DISTRIBUTION_ID}][LIMIT {self.batch_size} OFFSET {offset}]"

            # Build params dict with the query STRING
            params = {"query": sql_query}

            logger.info(f"fetching batch at offset {offset}")

            try:
                resp = self.session.get(
                    self.SQL_API_URL, params=params, timeout=self.timeout
                )
                resp.raise_for_status()

            except requests.RequestException as e:
                logger.error(f"API request failed: {e}")
                raise

            batch = resp.json()  # Parse JSON response

            if not batch or len(batch) == 0:  # Empty response = end of data
                logger.info("No more data to fetch")
                break

            all_data.extend(batch)  # Add this batch to list
            offset += len(batch)

            logger.info(f"Fetched {len(batch)} records (total: {len(all_data)})")

            # Stop early if limit hit
            if limit and len(all_data) >= limit:
                logger.info(f"Reached limit of {limit} records")
                all_data = all_data[:limit]
                break

            # Stop if batch was smaller than batch_size (end of data)
            if len(batch) < self.batch_size:
                logger.info("Batch smaller than batch_size, reached end")
                break

        df = pd.DataFrame(all_data)
        logger.info(f"Converted to DataFrame: {df.shape}")
        return df

    def validate_schema(self, df):
        """Check that the data looks reasonable."""
        logger.info("Validating schema...")
        
        if len(df) == 0:
            logger.error("DataFrame is empty!")
            return False
        
        logger.info(f"Shape: {df.shape}")
        logger.info(f"Columns: {list(df.columns)}")
        
        # Check for columns with >90% nulls
        null_rates = df.isnull().sum() / len(df)
        bad_cols = null_rates[null_rates > 0.9]
        
        if not bad_cols.empty:
            logger.warning(f"Columns with >90% nulls: {list(bad_cols.index)}")
        
        logger.info("Schema validation passed")
        return True

def main():
    """Main pipeline: fetch, validate, save."""

    output_dir = Path("./data")
    output_dir.mkdir(exist_ok=True)
    output_file = output_dir / "cms_provider_data.csv"

    try:
        # Initialize client
        client = CMSProviderAPIClient(batch_size=500)

        # Fetch data (limit to 1000 for testing)
        logger.info("\n=== FETCHING DATA ===")
        df = client.fetch_data()

        # Validate
        logger.info("\n=== VALIDATING DATA ===")
        if not client.validate_schema(df):
            raise ValueError("Schema validation failed")

        # Show summary
        logger.info(f"\n === DATA SUMMARY ===")
        logger.info(f"Shape: {df.shape}")
        logger.info(f"Columns: {list(df.columns)}")
        logger.info(f"\nFirst few rows:\n{df.head()}")
        logger.info(f"\nData types:\n{df.dtypes}")
        logger.info(f"\nNull counts:\n{df.isnull().sum()}")

        # Save to CSV
        logger.info("\n=== SAVING TO CSV ===")
        df.to_csv(output_file, index=False)
        logger.info(f"Saved {len(df)} records to {output_file}")
        logger.info(f"File size: {output_file.stat().st_size / 1024:.1f} KB")

        return df

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    df = main()
