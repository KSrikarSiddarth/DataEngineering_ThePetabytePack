MobilityPulse SetupF run - collected from VM1 on 2026-11-22

serving\   SQLite serving DB used by the dashboard and API (real-data run, Jan-Mar 2024, plus stream results exported 11:47)
results\   project results folder: measurements.json, dq reports, stream CSVs (progress/batches/latency), benchmarks, concepts
logs\      mp-demo3.log (synthetic demo, cluster), mp-pipeline.log (real-data pipeline, cluster), mp-export.log (export with stream data)

Not included: raw downloads (re-downloadable, 159 MB), the lake in MinIO on VM2 (s3a://lake/mobilitypulse), settings file with the MinIO password.
