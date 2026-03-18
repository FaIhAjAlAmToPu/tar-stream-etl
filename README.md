# TarStreamETL 🚀

**TarStreamETL** is a high-performance, thread-safe Python engine designed to process massive TAR archives (100GB+) via network streams. 

It was specifically engineered to bypass local disk constraints by using a **"Sliding Window"** extraction strategy, allowing you to transform data in parallel without ever needing to store the full archive locally.

## 🌟 Key Features
* **Zero-Footprint Streaming:** Processes TAR members directly from a URL stream—no pre-downloading required.
* **Threaded Architecture:** Parallelizes extraction (Producer) and transformation (Consumer) for maximum throughput.
* **Memory-Safe Buffering:** Uses a `threading.Semaphore` to limit the number of files on disk at any given time.
* **Atomic Hand-offs:** Implements a `.tmp` rename strategy to ensure the consumer never touches a partially extracted file.
* **Research Ready:** Default logic optimized for converting **DARPA OpTC** JSON records into flattened **Parquet** files via DuckDB.

---

## 🛠 Usage

### 1. Installation
You can install the package directly from GitHub:
```bash
pip install git+https://github.com/FaIhAjAlAmToPu/tar-stream-etl.git
```

### 2. Basic Example
```python
from tar_stream_etl import TarStreamETL

# Configuration
URL = "https://your-data-source.com/massive_archive.tar"
OUTPUT = "./processed_data"

# Initialize and Run
# max_files=3 means only 3 files will exist in the temp buffer at once
etl = TarStreamETL(url=URL, output_path=OUTPUT, max_files=3)
etl.run()
```

### 3. Custom Transformation
If you aren't using the OpTC dataset, you can "plug in" your own logic:
```python
def my_custom_logic(file_path, output_dir, db_con):
    # Your transformation code here (e.g., CSV to Parquet)
    pass

etl = TarStreamETL(url=URL, transform_func=my_custom_logic)
etl.run()
```

---

## 📓 Kaggle Integration

This tool was originally developed and verified within **Kaggle Kernels** to solve the **disk size limit** issue. During development for the `Infinite-Horizon-PIDS` project, **TarStreamETL** successfully processed over **200GB** of raw DARPA OpTC telemetry by maintaining a sliding buffer of only **3 files (~1.5GB)** at any time.

**Kaggle Utility Script Usage:**
1.  Add it as a Utility Script.
2.  Import via `from tar_stream_etl import TarStreamETL`.
3.  Set `output_path="/kaggle/tmp/output"` and `download_dir="/kaggle/tmp"` (instead of `/kaggle/working/` for more storage).

---

## 🏗 Technical Architecture

The engine operates using two main threads:
1.  **The Producer:** Streams the TAR file, extracts a member to a `.tmp` file, and renames it to its original name only when extraction is 100% complete.
2.  **The Consumer:** Monitors the buffer directory, ignores `.tmp` files, transforms "ready" files to Parquet, and deletes the source to free up a slot for the Producer.

---

## 📄 License
MIT License - Feel free to use this for your own cybersecurity research!

---