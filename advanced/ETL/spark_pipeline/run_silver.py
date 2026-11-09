"""
Entry point for the Silver transformation pipeline.
"""

import logging

from advanced.config.spark_session import create_spark_session

from advanced.ETL.spark_pipeline.silver.silver_transformation import (
    SilverTransformation,
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

    spark = create_spark_session()

    try:

        silver = SilverTransformation(
            spark=spark,
            source_database="bronze",
            target_database="silver",
        )

        silver.run()

    finally:

        spark.stop()


if __name__ == "__main__":
    main()