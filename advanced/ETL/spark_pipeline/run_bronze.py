"""
Entry point for the Bronze ingestion pipeline.
"""

import logging

from advanced.config import load_settings
from advanced.config.spark_session import create_spark_session

from advanced.ETL.spark_pipeline.bronze.bronze_ingestion import (
    BronzeIngestion,
)


def main() -> None:

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(name)s | "
            "%(message)s"
        ),
    )

    # 1. Load application configuration.
    settings = load_settings()

    # 2. Create Spark session.
    spark = create_spark_session()

    try:

        # 3. Initialize Bronze ingestion.
        bronze = BronzeIngestion(
            spark=spark,
            settings=settings,
        )

        # 4. Execute the pipeline.
        bronze.run()

    finally:

        # 5. Release Spark resources.
        spark.stop()


if __name__ == "__main__":
    main()