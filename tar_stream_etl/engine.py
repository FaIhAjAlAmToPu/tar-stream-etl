import os, time, gzip, threading, tarfile, requests, zlib
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
    """
    def __init__(self, url, output_path="/kaggle/tmp/output", download_dir="/kaggle/tmp", max_files=3, transform_func=None, files_zipped = False):
        self.url = url
        self.download_dir = download_dir
        self.output_path = output_path
        self.max_files = max_files
        # The specific logic is now a "plug-in"
        self.transform_func = transform_func or self.default_transform
        
        os.makedirs(self.download_dir, exist_ok=True)
        os.makedirs(self.output_path, exist_ok=True)
        
        self.buffer_limit = threading.Semaphore(self.max_files)
        # Shared duckdb connection for the instance
        self.con = duckdb.connect(database=':memory:')

    def producer(self):
        """Streams the archive and extracts members only when the buffer has space."""
        print(f"🚀 Starting stream: {self.url}")
        try:
            response = requests.get(self.url, stream=True, timeout=30)
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
            print(f"🔥 Producer Stream Error: {e}")
        finally:
            print("🏁 Producer: Finished.")

    def consumer(self):
        """Monitors the download_dir and triggers the user's transform_func."""
        while True:
            # CRITICAL FIX: Only grab files that ARE NOT currently being extracted (.tmp)
            files = [f for f in os.listdir(self.download_dir) if not f.endswith('.tmp') and os.path.isfile(os.path.join(self.download_dir, f))]
            
            # Exit condition: Producer thread is dead and directory is empty
            producer_alive = any(t.name == "ProducerThread" and t.is_alive() for t in threading.enumerate())
            if not files and not producer_alive:
                break
                
            for filename in files:
                file_path = os.path.join(self.download_dir, filename)
                # Call the custom transformation logic
                self.transform_func(file_path, self.output_path, self.con)
                if os.path.exists(file_path):
                    os.remove(file_path)
                self.buffer_limit.release()
            
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