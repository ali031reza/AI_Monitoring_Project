# AI Monitoring Streamlit UI

## Run

Run from the repository root (the directory containing `advanced/`):

```powershell
python -m streamlit run advanced/ui/app.py
```

## Spark runtime on Windows

The Spark session uses the same local warehouse and Derby metastore paths
as the project's ETL notebooks:

- `advanced/ETL/warehouse`
- `advanced/ETL/metastore_db`
- `advanced/ETL/.spark-temp`

Before starting Streamlit, set the Java and Hadoop environment variables in
the PowerShell window if they are not already configured. Use the paths
installed on your machine; the following match the paths recorded in the
project's notebooks:

```powershell
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.20.101-hotspot"
$env:HADOOP_HOME = "C:\hadoop\hadoop-3.5.0"
$env:PYSPARK_PYTHON = (Resolve-Path .\.venv\Scripts\python.exe).Path
```

Then run Streamlit:

```powershell
.\.venv\Scripts\python.exe -m streamlit run advanced/ui/app.py
```

The Windows Hadoop directory is expected to contain `bin\winutils.exe`,
`bin\hadoop.dll`, and `bin\hdfs.dll`, matching the checks in the ETL
notebooks.

## Important: Derby metastore lock

The notebook and Streamlit app are separate Python processes. The project's
embedded Derby metastore normally cannot be opened concurrently by both
processes. Stop the notebook's SparkSession (`spark.stop()`) and close its
kernel before starting Streamlit if both use the same `metastore_db`.

## Data flow

`Streamlit -> DashboardService -> gold.monitoring_detection_details`

The dashboard uses bounded Spark aggregations and collects only small
results into Pandas:

- one KPI row
- hourly aggregates
- zone aggregates
- at most 20 recent records

The SparkSession and service are cached with `st.cache_resource`.
Dashboard results are cached for 25 seconds and the dashboard fragment
refreshes every 30 seconds.

The UI refresh does not rerun Kafka consumers or the ETL pipeline. It
reflects the most recently persisted Gold tables.

## Metric definitions

- Person observations: individual rows where `detection_source = 'person'`.
- Plate observations: individual rows where `detection_source = 'plate'`.
- Distinct employees observed: distinct non-null employee IDs from person
  rows only; plate matches are excluded.
- Cameras with activity: distinct camera IDs in the selected time window.
- Enabled cameras: configuration values from `Starter_Sources/cameras.csv`,
  not a live health/heartbeat signal.

A plate-to-employee match is an association and is not evidence that the
employee was physically present. Raw detections are not automatically
classified as security alerts.


## Kafka Pipeline page

The **Kafka Pipeline** page is a live local-process control panel. It does
not connect to Spark or the Gold tables.

From the repository root, start the UI with:

```powershell
python -m streamlit run advanced/ui/app.py
```

Before starting a run:

1. Start the Kafka broker configured by `KAFKA_BOOTSTRAP_SERVERS`.
2. Confirm that `Camera_sample/TEST.mp4` exists and is readable.
3. Open **Kafka Pipeline**, choose the camera count, FPS, person workers
   and plate workers, then select **Start pipeline**.

The controller starts detector consumers first, starts a committed-offset
lag monitor, and starts the camera producer last. It provisions missing
topics and increases topic partition counts when needed. Kafka partition
counts cannot be decreased; a later run with fewer workers retains the
existing partitions.

Each run writes its configuration, metadata, process logs and JSONL
metrics under `advanced/experiment_results/<run_id>/`. These are local
experiment artifacts and are intentionally excluded from Git.

The page reports measured application metrics: producer message and byte
throughput, delivery latency, producer queue depth, per-worker receive and
processing rates, processing/commit latency, and committed lag by
partition. It does not report broker host I/O or CPU/RAM/GPU utilization.
The detector workers use mock simulation with a 30 ms delay; no YOLO
inference is executed.

The existing Overview, Person Monitoring, Access Review and related pages
remain backed by the Gold/Spark layer. The Kafka Pipeline page can be used
without opening a Spark session.
