
"""
Gold Pipeline Entry Point
=========================

Runs the complete Silver-to-Gold transformation.

Usage:
    python -m advanced.ETL.spark_pipeline.run_gold
"""

import logging

from advanced.config.spark_session import create_spark_session

from advanced.ETL.spark_pipeline.gold.gold_transformation import (
    GoldTransformation,
)


logger = logging.getLogger(__name__)


def main() -> None:
    """Initialize Spark and execute Gold transformations."""

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | %(levelname)s | "
            "%(name)s | %(message)s"
        ),
    )

    spark = None

    try:
        logger.info("=" * 60)
        logger.info("INITIALIZING GOLD PIPELINE")
        logger.info("=" * 60)

        spark = create_spark_session()

        gold = GoldTransformation(
            spark=spark,
            source_database="silver",
            target_database="gold",
        )

        gold.run()

        logger.info(
            "Gold pipeline executed successfully."
        )

    except Exception:
        logger.exception(
            "Gold pipeline execution failed."
        )
        raise

    finally:
        if spark is not None:
            logger.info("Stopping Spark session.")
            spark.stop()

        logger.info("Gold pipeline execution finished.")


if __name__ == "__main__":
    main()
