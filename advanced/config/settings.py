
import os
from pathlib import Path


# ============================================================
# 1. PROJECT PATHS
# ============================================================

ADVANCED_DIR = Path(__file__).resolve().parents[1]

# Reference data
STARTER_SOURCES_DIR = ADVANCED_DIR / "Starter_Sources"

CAMERAS_CSV = STARTER_SOURCES_DIR / "cameras.csv"
EMPLOYEES_CSV = STARTER_SOURCES_DIR / "employees.csv"
RESTRICTED_ACCESS_CSV = (
    STARTER_SOURCES_DIR / "restricted_access.csv"
)

# Generated detection files
DATA_DIR = ADVANCED_DIR / "data"

PERSON_DETECTOR_WORKER_ID = os.getenv(
    "PERSON_DETECTOR_WORKER_ID",
    "worker1",
)

PERSON_DETECTIONS_CSV = (
    DATA_DIR /
    f"persons_detected_{PERSON_DETECTOR_WORKER_ID}.csv"
)
PLATE_DETECTIONS_CSV = DATA_DIR / "plates_detected.csv"

# Generated image previews
PREVIEW_DIR = ADVANCED_DIR / "preview"


# ============================================================
# 2. KAFKA CONNECTION SETTINGS
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "127.0.0.1:9092",
)

KAFKA_ADDRESS_FAMILY = os.getenv(
    "KAFKA_ADDRESS_FAMILY",
    "v4",
)


# ============================================================
# 3. KAFKA TOPICS
# ============================================================

# Cameras 001-007: Person detection
PERSON_FRAMES_TOPIC = os.getenv(
    "KAFKA_PERSON_FRAMES_TOPIC",
    "camera.person.frames.dev",
)

# Cameras 008-010: License plate detection
PLATE_FRAMES_TOPIC = os.getenv(
    "KAFKA_PLATE_FRAMES_TOPIC",
    "camera.plate.frames.dev",
)


# ============================================================
# 4. KAFKA CONSUMER GROUPS
# ============================================================

PERSON_DETECTOR_GROUP_ID = os.getenv(
    "PERSON_DETECTOR_GROUP_ID",
    "person-detectors-dev",
)

PLATE_DETECTOR_GROUP_ID = os.getenv(
    "PLATE_DETECTOR_GROUP_ID",
    "plate-detectors-dev",
)


# ============================================================
# 5. CAMERA PRODUCTION SETTINGS
# ============================================================

# Delay between camera production rounds
FRAME_INTERVAL_SECONDS = float(
    os.getenv("FRAME_INTERVAL_SECONDS", "1.0")
)

# Synthetic image dimensions
FRAME_WIDTH = int(
    os.getenv("FRAME_WIDTH", "640")
)

FRAME_HEIGHT = int(
    os.getenv("FRAME_HEIGHT", "360")
)

# JPEG compression quality (1-100)
JPEG_QUALITY = int(
    os.getenv("JPEG_QUALITY", "80")
)


# ============================================================
# 6. PERSON DETECTION SIMULATION
# ============================================================

# Fixed seed for reproducible employee movements
SIMULATION_SEED = int(
    os.getenv("SIMULATION_SEED", "20261027")
)

# Probability of detecting a person who is present
PERSON_PRESENCE_PROBABILITY = float(
    os.getenv(
        "PERSON_PRESENCE_PROBABILITY",
        "0.25",
    )
)

# Visibility is stable within this time window
PERSON_MOVEMENT_WINDOW_SECONDS = int(
    os.getenv(
        "PERSON_MOVEMENT_WINDOW_SECONDS",
        "30",
    )
)

# Maximum detected persons in a single frame
MAX_PERSONS_PER_FRAME = int(
    os.getenv("MAX_PERSONS_PER_FRAME", "4")
)


# ============================================================
# 7. EMPLOYEE MOVEMENT SETTINGS
# ============================================================

# Minimum time an employee stays in a zone
MIN_ZONE_DWELL_SECONDS = int(
    os.getenv("MIN_ZONE_DWELL_SECONDS", "120")
)

# Maximum time an employee stays in a zone
MAX_ZONE_DWELL_SECONDS = int(
    os.getenv("MAX_ZONE_DWELL_SECONDS", "300")
)

# Minimum travel time between two zones
MIN_ZONE_TRAVEL_SECONDS = int(
    os.getenv("MIN_ZONE_TRAVEL_SECONDS", "30")
)

# Maximum travel time between two zones
MAX_ZONE_TRAVEL_SECONDS = int(
    os.getenv("MAX_ZONE_TRAVEL_SECONDS", "90")
)


# ============================================================
# 8. EMPLOYEE SHIFT SETTINGS
# ============================================================

# Possible daily shift start hours (UTC)
SHIFT_START_HOURS = tuple(
    int(hour.strip())
    for hour in os.getenv(
        "SHIFT_START_HOURS",
        "0,8,16",
    ).split(",")
)

# Shift duration: 7 hours
SHIFT_LENGTH_SECONDS = int(
    os.getenv("SHIFT_LENGTH_SECONDS", "25200")
)

# Arrival variation: up to 30 minutes
ARRIVAL_SPREAD_SECONDS = int(
    os.getenv("ARRIVAL_SPREAD_SECONDS", "1800")
)


# ============================================================
# 9. BUILDING ZONE CONNECTIONS
# ============================================================

# Zone A: General access / entrance
# Zone B: General access
# Zone C: Restricted
# Zone D: Restricted
# Zone E: Parking
#
# All connections are bidirectional.
#
# A connects to B, C, D, E.
# B connects to A, C, D.
# C connects to A and B.
# D connects to A and B.
# E connects only to A.
#
# There is no direct connection between C and D.

ZONE_CONNECTIONS = {
    "A": ("B", "C", "D", "E"),
    "B": ("A", "C", "D"),
    "C": ("A", "B"),
    "D": ("A", "B"),
    "E": ("A",),
}


# Parking simulation
PARKING_TRANSIT_SECONDS = int(
    os.getenv("PARKING_TRANSIT_SECONDS", "180")
)

PARKING_DETECTION_WINDOW_SECONDS = int(
    os.getenv("PARKING_DETECTION_WINDOW_SECONDS", "30")
)