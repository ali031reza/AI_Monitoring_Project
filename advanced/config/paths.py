
from dataclasses import dataclass
from pathlib import Path
import re


@dataclass(frozen=True)
class ProjectPaths:
    """
    Centralized filesystem paths for the advanced application.

    This class defines paths only.
    It does not create directories or manage files.
    """

    advanced_dir: Path

    # ========================================================
    # PROJECT DIRECTORY DISCOVERY
    # ========================================================

    @classmethod
    def discover(cls) -> "ProjectPaths":
        """Discover the advanced directory from this module."""

        advanced_dir = Path(__file__).resolve().parents[1]

        return cls(advanced_dir=advanced_dir)

    # ========================================================
    # REFERENCE DATA
    # ========================================================

    @property
    def starter_sources_dir(self) -> Path:
        return self.advanced_dir / "Starter_Sources"

    @property
    def cameras_csv(self) -> Path:
        return self.starter_sources_dir / "cameras.csv"

    @property
    def employees_csv(self) -> Path:
        return self.starter_sources_dir / "employees.csv"

    @property
    def restricted_access_csv(self) -> Path:
        return self.starter_sources_dir / "restricted_access.csv"

    # ========================================================
    # GENERATED DATA
    # ========================================================

    @property
    def data_dir(self) -> Path:
        return self.advanced_dir / "data"

    def person_detections_csv(self, worker_id: str) -> Path:
        """Return the output CSV path for a person detector worker."""

        if (
            not isinstance(worker_id, str)
            or not re.fullmatch(r"[A-Za-z0-9_-]+", worker_id)
        ):
            raise ValueError(
                f"Invalid person detector worker ID: {worker_id!r}"
            )

        return self.data_dir / f"persons_detected_{worker_id}.csv"

    @property
    def plate_detections_csv(self) -> Path:
        return self.data_dir / "plates_detected.csv"

    @property
    def preview_dir(self) -> Path:
        return self.advanced_dir / "preview"

    # ========================================================
    # SPARK AND ETL
    # ========================================================

    @property
    def etl_dir(self) -> Path:
        return self.advanced_dir / "ETL"

    @property
    def warehouse_dir(self) -> Path:
        return self.etl_dir / "warehouse"

    @property
    def metastore_dir(self) -> Path:
        return self.etl_dir / "metastore_db"

    @property
    def spark_temp_dir(self) -> Path:
        return self.etl_dir / ".spark-temp"
