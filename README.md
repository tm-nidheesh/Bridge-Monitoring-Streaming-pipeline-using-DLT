# Bridge-Monitoring-Streaming-pipeline-using-DLT
A hands-on demo of a production-grade streaming ETL pipeline using Databricks Delta Live Tables (DLT). We simulate IoT sensors on major bridges, ingest three raw streams (temperature, vibration, tilt), enrich them with static metadata, and compute 10-minute windowed metrics via watermarks, window aggregations, stream-static joins, and stream-stream joins.
<br>

## Repository Overview
* data_generator.ipynb: continuously emits synthetic sensor readings into Delta paths, one minute apart, with a small random timestamp lag to mimic real-world delays.
* 1_bronze_processing.ipynb: three streaming tables (1_bronze.bridge_temperature, 1_bronze.bridge_vibration, 1_bronze.bridge_tilt) that read raw Delta files as soon as they arrive.
* 2_silver_processing.ipynb<br>
  Silver layer:<br>
  * 2_silver.bridge_metadata: static lookup of five bridges.
  * Three enriched streaming tables that join each Bronze stream to the static metadata and enforce data-quality expectations.
* 3_gold_processing.ipynb
Gold layer:
  * Reads the three silver streams with a 2-minute watermark.
  * Computes 10-minute tumbling aggregates:
     * avg_temperature
     * max_vibration
     * max_tilt_angle
  * Joins them by (bridge_id, window_start, window_end) into 3_gold.bridge_metrics.
 
## Unity Catalog Overview<br>
* Create a managed catalog called bridge_monitoring.
* Create schemas in the catßalog called 00_landing, 01_bronze, 02_silver and 03_gold.
* Create a managed volume in the 00_landing schema called streaming.
* In the streaming volume create three subdirectories called bridge_temperature, bridge_vibration and bridge_tilt.

## Step 1: Simulate sensor data<br>
* Open data_generator.ipynb.
* Provide your Delta paths in the streams list.
* Run the notebook: it will spin up three background generators that append new data every minute, with a random 0–60 s timestamp lag.

 <img width="515" alt="data generator" src="https://github.com/user-attachments/assets/3b80c758-b8eb-44b3-b73c-10b78f5678a2" />
 
## Step 2: Data Ingestion to Bronze<br>
* Create a new DLT pipeline in Databricks, attaching 1_bronze_processing.ipynb as a notebook source.
* Configure the pipeline to use your Unity Catalog schema (e.g. bridge_monitoring.bronze).
* Run—three streaming tables will appear, capturing raw temperature, vibration, and tilt events.

<img width="521" alt="bronze" src="https://github.com/user-attachments/assets/042bc62d-db37-4a9d-9932-5f13969d69f8" />

## Step 3: Silver layer<br>
* Add 2_silver_processing.ipynb to the same pipeline.
* Ensure the schema names are bridge_monitoring.silver.
* Run—DLT will materialize:
  * bridge_metadata (static)
  * Three enriched streams with @dlt.expect_or_drop checks and stream–static joins.
 
## Step 4: Gold layer<br>
* Add 3_gold_processing.ipynb to your pipeline.
* Verify the target schema bridge_monitoring.gold.
* Run—DLT will:
  * Apply 2-min watermarks
  * Compute 10-min tumbling avg/max metrics
  * Perform stream–stream joins on window bounds
  * Publish bridge_metrics for downstream analytics.
  
<img width="959" alt="DLT pipeline run" src="https://github.com/user-attachments/assets/7896c4e3-7c37-4da0-914e-51489af17fc2" />
