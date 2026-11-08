
"""
Configuration package for the AI Monitoring application.

Provides a centralized public interface for:

- Application configuration dataclasses
- Configuration loading and validation
- Project filaesystem paths

Configuration is loaded explicitly through load_settings().
"""

# ============================================================
# PROJECT PATHS
# ============================================================

from .paths import ProjectPaths


# ============================================================
# CONFIGURATION DATACLASSES
# ============================================================

from .settings import (
    ApplicationSettings,
    KafkaSettings,
    CameraSettings,
    SimulationSettings,
    PersonDetectionSettings,
    EmployeeMovementSettings,
    EmployeeShiftSettings,
    ParkingSettings,
)


# ============================================================
# CONFIGURATION LOADER
# ============================================================

from .settings import load_settings


# ============================================================
# PUBLIC API
# ============================================================

__all__ = [
    # Main configuration
    "ApplicationSettings",
    "load_settings",

    # Configuration groups
    "KafkaSettings",
    "CameraSettings",
    "SimulationSettings",

    # Simulation subgroups
    "PersonDetectionSettings",
    "EmployeeMovementSettings",
    "EmployeeShiftSettings",
    "ParkingSettings",

    # Project paths
    "ProjectPaths",
]
