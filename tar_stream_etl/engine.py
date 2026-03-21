# %% [code]
import os, time, shutil, gzip, threading, tarfile, requests, zlib
import pyarrow.json as paj, duckdb

class TarStreamETL:
    """
    A high-performance ETL engine for processing massive TAR archives 
    via network streams without exhausting local disk space.
    
    Attributes:
        url (str): Source URL of the .tar archive.
        output_path (str): Where the transformed files (e.g., Parquet) will be saved.
        download_dir (str): Temporary buffer for extracted files.
        max_files (int): The "Sliding Window" size (max files on disk at once).
        transform_func (callable): The user-defined function for processing data.
        file_ready_func (callable): The user-defined function for checking if a particular file is ready to be transformed/consumed by consumer thread.
        flush_func (callable): The "Safety Valve" function triggered when local storage is nearly full. Defaults to `self.kaggle_flush` for automated dataset updates.
        output_size_limit_gb (float): The maximum allowed size (in GiB) for the output directory before a flush is triggered to prevent disk-full errors.
        flush_kwargs (dict): A dynamic dictionary of keyword arguments (e.g., metadata, API keys) passed via the constructor to be used by the flush_func.
        flush_count (int): No of times we flushed output dir.
        con (duckdb.PyConnection): A shared, in-memory DuckDB connection optimized for high-speed data transformation and Parquet conversion.
        flush_lock (threading.Lock): A synchronization primitive that ensures only one flush operation (e.g., Kaggle upload) can occur at a time.
        buffer_limit (threading.Semaphore): A counting semaphore that regulates the Producer thread's extraction rate based on max_files.
        finish_func (callable): The "Closing Bell" function triggered after both threads have joined. It handles the final upload of any remaining data in the output directory that didn't trigger a full "Safety Valve" flush. Defaults to self.kaggle_finish_func.
        ProducerThread (threading.Thread): The "Source" worker. This internal attribute (managed within the run method) handles the network connection and the sequential extraction of the TAR stream into the temporary buffer.
        ConsumerThread (threading.Thread): The "Processor" worker. This internal attribute (managed within the run method) monitors the buffer, executes the transformation logic, and manages the local disk cleanup once data is safely converted to Parquet.
    """
    def __init__(self, url, output_path="/kaggle/tmp/output", download_dir="/kaggle/tmp", max_files=3, transform_func=None, flush_func=None, finish_func = None, output_size_limit_gb = 40, **flush_kwargs):
        self.url = url
        self.download_dir = download_dir
        self.output_path = output_path
        self.max_files = max_files
        # The specific logic is now a "plug-in"
        self.transform_func = transform_func or self.default_transform
        self.flush_func=flush_func or self.kaggle_flush
        self.finish_func=finish_func or self.kaggle_finish_func
        
        os.makedirs(self.download_dir, exist_ok=True)
        os.makedirs(self.output_path, exist_ok=True)
        
        self.buffer_limit = threading.Semaphore(self.max_files)
        self.output_size_limit_gb = output_size_limit_gb
        # Shared duckdb connection for the instance
        self.con = duckdb.connect(database=':memory:')
        self.flush_lock = threading.Lock() # The "Security Guard"
        # This captures everything else (metadata, dataset_id, etc.)
        self.flush_kwargs = flush_kwargs
        self.flush_count = 0

    def get_output_size_gb(self):
        """
        Directly calculates the size of the output directory.
        Returns the size in GB (Decimal).
        """
        # Summing the size of every file in the output folder directly
        total_bytes = sum(
                os.path.getsize(os.path.join(self.output_path, f)) 
                for f in os.listdir(self.output_path) 
                if os.path.isfile(os.path.join(self.output_path, f))
        )
        # Convert Bytes to GB
        return total_bytes / (1024**3)


    @staticmethod
    def kaggle_flush(output_path, flush_count, **flush_kwargs):
        """
        Ships Parquet files to Kaggle. 
        Creates the dataset if it doesn't exist; otherwise, updates the version.
        """
        # 1. Pull metadata from the settings we saved in __init__
        original_metadata = flush_kwargs.get('metadata')
        metadata = original_metadata.copy()

        if not metadata:
            print("❌ Error: No metadata found in flush_settings.")
            return

        metadata['id'] = f"{metadata['id']}-part-{flush_count}"
        metadata['title'] = f"{metadata['title']} (Part {flush_count})"
            
        import os, json, subprocess

        # 2. Identify files
        files = [f for f in os.listdir(output_path) if f.endswith('.parquet')]
        if not files: return

        # 3. Write metadata file
        meta_path = os.path.join(output_path, 'dataset-metadata.json')
        with open(meta_path, 'w') as f:
            json.dump(metadata, f, indent=4)

        dataset_id = metadata.get("id")
        print(f"📤 Preparing to flush {len(files)} files to {dataset_id}...")

        # 4. Create the new dataset
        # We use 'create' instead of 'version' to ensure no data is overwritten
        create_cmd = f"kaggle datasets create -p {output_path} --public --dir-mode zip"
        result = subprocess.run(create_cmd, shell=True, capture_output=True, text=True)
    
        if result.returncode == 0:
            print(f"✅ Success! Part {flush_count} is live.")
            shutil.rmtree(output_path) 
            print(f"🗑️ Cleaned up temporary flush directory: {output_path}")
        else:
            print(f"❌ Kaggle Error: {result.stderr}")

    @staticmethod
    def kaggle_finish_func(output_path, flush_count, **flush_kwargs):
        """
        The 'Closing Bell' function. 
        Handles the final upload: 
        - If flush_count is 0, it creates the base dataset.
        - If flush_count > 0, it creates the final 'Part-N' shard.
        """
        import os, json, subprocess
        
        # 1. Check if we have anything to upload
        files = [f for f in os.listdir(output_path) if f.endswith('.parquet')]
        if not files:
            print("📭 No remaining files to upload. ETL complete.")
            return

        original_metadata = flush_kwargs.get('metadata')
        
        metadata = original_metadata.copy()

        if flush_count > 0:
            metadata['id'] = f"{metadata['id']}-part-{flush_count}"
            metadata['title'] = f"{metadata['title']} (Part {flush_count})"
        print(f"📦 Uploading final shard: Final Part")

        # 2. Write Metadata
        meta_path = os.path.join(output_path, 'dataset-metadata.json')
        with open(meta_path, 'w') as f:
            json.dump(metadata, f, indent=4)

        # 3. Final Kaggle Push
        print(f"🚀 Final push for {metadata['id']}...")
        create_cmd = f"kaggle datasets create -p {output_path} --public --dir-mode zip"
        result = subprocess.run(create_cmd, shell=True, capture_output=True, text=True)

        if result.returncode == 0:
            print(f"✅ Final Part Uploaded. Clean exit.")
            # Final disk cleanup
            for f in files: os.remove(os.path.join(output_path, f))
            if os.path.exists(meta_path): os.remove(meta_path)
        else:
            print(f"❌ Final Upload Error: {result.stderr}")

    def producer(self):
        """Streams the archive and extracts members only when the buffer has space."""
        print(f"🚀 Starting stream: {self.url}")
        try:
            response = requests.get(self.url, stream=True, timeout=100)
            # 'r|*' is essential for streaming mode
            with tarfile.open(fileobj=response.raw, mode="r|*") as tar:
                for member in tar:
                    if member.isfile():
                        self.buffer_limit.acquire() # Wait for consumer
                        
                        # Abstract name handling
                        original_name = os.path.basename(member.name)
                        tmp_name = original_name + ".tmp"

                        # Extract to .tmp (Hidden from Consumer)
                        member.name = tmp_name
                        tar.extract(member, path=self.download_dir, filter='data')

                        # Atomic hand-off (Reveals file to Consumer)
                        os.rename(os.path.join(self.download_dir, tmp_name), 
                                  os.path.join(self.download_dir, original_name))
        except Exception as e:
            print(f"Producer Stream Error: {e}")
        finally:
            print("🏁 Producer: Finished.")

    def consumer(self):
        """Monitors the download_dir and triggers the user's transform_func."""
        while True:
            # CRITICAL FIX: Only grab files that ARE NOT currently being extracted (.tmp)
            files = [f for f in os.listdir(self.download_dir) if not f.endswith('.tmp') and os.path.isfile(os.path.join(self.download_dir, f))]
            
            # Exit condition: Producer thread is dead and directory is empty
            producer_alive = any(t.name == "ProducerThread" and t.is_alive() for t in threading.enumerate())
            if not files:
                if not producer_alive:
                    break
                time.sleep(0.5)
                continue
                
            for filename in files:
                file_path = os.path.join(self.download_dir, filename)
                # Call the custom transformation logic
                self.transform_func(file_path, self.output_path, self.con)
                if os.path.exists(file_path):
                    os.remove(file_path)
                self.buffer_limit.release()
                current_size = self.get_output_size_gb()
                if current_size > self.output_size_limit_gb:  # If output hits 45GiB
                    # Use the lock ONLY for the split-second it takes to swap folders
                    with self.flush_lock:
                        print(f"🚨 Rotation Triggered ({current_size:.2f}GiB).")
        
                        # 1. Create a unique snapshot name
                        timestamp = int(time.time())
                        new_upload_dir = f"{self.output_path}_flush_{timestamp}"
                        
                        # 2. Rename the directory (Atomic operation)
                        os.rename(self.output_path, new_upload_dir)
                        
                        # 3. Recreate the original path immediately
                        os.makedirs(self.output_path, exist_ok=True)
                        
                        self.flush_count += 1
        
                    # 4. Fire the flush in a BACKGROUND thread
                    # This thread handles the slow Kaggle upload
                    threading.Thread(
                        target=self.flush_func, 
                        name=f"ETL-Flush-{self.flush_count}", # Give it a clear name
                        kwargs={'output_path': new_upload_dir, 'flush_count': self.flush_count, **self.flush_kwargs},
                        daemon=True # Ensures the thread dies if the main program crashes
                    ).start()
                    
                    print(f"🚀 Background Flush started for {new_upload_dir}. Consumer resuming...")           
            time.sleep(0.5)

    @staticmethod
    def default_transform(file_path, output_dir, db_con):
        """Specific conversion logic for your tar dataset. It uses duckdb connection for probable conversion of big dataset.
        The default transformation is for the OpTC dataset conversion that is from json.gz into parquet file"""
        filename = os.path.basename(file_path)
        try:
            sysclient = filename.replace(".json.gz", "").split("-")[-1]
            out_file = os.path.join(output_dir, f"{sysclient}.parquet")
            
            with gzip.open(file_path, 'rb') as f:
                table = paj.read_json(f)
                
            db_con.register("temp", table)
            db_con.execute(f"COPY (SELECT * EXCLUDE(properties), properties.* FROM temp) TO '{out_file}' (FORMAT PARQUET, COMPRESSION 'ZSTD')")
            db_con.unregister("temp")
            print(f"✅ Converted: {sysclient}")
        except Exception as e:
            print(f"❌ Failed {filename}: {e}")

    def run(self):
        t1 = threading.Thread(target=self.producer, name="ProducerThread")
        t2 = threading.Thread(target=self.consumer, name="ConsumerThread")
        t1.start(), t2.start()
        t1.join(), t2.join()

        # IMPORTANT: Wait for any background flushes to finish before final act
        print("⏳ Waiting for background upload threads to complete...")
        for t in threading.enumerate():
            if t.name.startswith("ETL-Flush-") and t.is_alive(): # Default names for our background threads
                t.join()
        
        # The Final Act: Cleanup and last upload
        print("🏁 All threads finished. Running finish_func...")
        self.finish_func(output_path=self.output_path, flush_count = self.flush_count + 1, **self.flush_kwargs)