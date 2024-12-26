from primePy import primes
from pathlib import Path
import sqlite3
from typing import Set, List
from datetime import datetime
import logging
import time
import psutil
import multiprocessing as mp
import json
from contextlib import contextmanager
import sys
import traceback

def process_chunk(args):
    """Process a chunk of j-values and their corresponding k-values efficiently.
    
    Args:
        args: tuple containing:
            - j_values: List of j values to process in this chunk
            - i: The target number being processed
            - paths_dict: Dictionary containing pre-loaded paths for both j and k values
    """
    j_values, i, paths_dict = args
    results = []
    
    for j in j_values:
        k = i - j
        j_paths = paths_dict.get(j, [])
        k_paths = paths_dict.get(k, [])
        
        if not j_paths or not k_paths:
            continue
            
        for j_path in j_paths:
            j_set = frozenset(j_path)
            for k_path in k_paths:
                if not j_set.intersection(k_path):
                    results.append(sorted(j_path + k_path))
    
    return results


class ProcessPoolRetry:
    def __init__(self, max_retries=10, delay=0.05):
        self.max_retries = max_retries
        self.delay = delay
        self.pool = None
        
    @contextmanager
    def get_pool(self, processes):
        try:
            self.pool = mp.Pool(processes=processes)
            yield self.pool
        finally:
            if self.pool:
                self.pool.close()
                self.pool.join()
                
    def map_with_retry(self, func, items, chunksize=1):
        for attempt in range(self.max_retries):
            try:
                with self.get_pool(self.processes) as pool:
                    return pool.map(func, items, chunksize=chunksize)
            except Exception as e:
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(self.delay * (attempt + 1))
                logging.warning(f"Pool operation failed (attempt {attempt + 1}/{self.max_retries}): {str(e)}")

class SQLitePrimeSumCalculator:
    def __init__(self, target: int, output_dir: str = "prime_sums_output", start_from: int = 0, 
                 num_processes: int = None, checkpoint_interval: int = 100):
        self.num_processes = num_processes or max(1, mp.cpu_count() - 1)
        self.checkpoint_interval = checkpoint_interval
        
        # Set up logging
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        log_file = self.output_dir / f"prime_sums_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        self.start_from = start_from
        self.target = target
        
        # Initialize database
        self.db_path = self.output_dir / f"prime_sums_{target}.db"
        self.init_database()
        
        # Cache of primes using set for O(1) lookup
        self.primes_set: Set[int] = set(primes.upto(target))
        
        # In-memory cache for frequently accessed paths
        self.path_cache = {}
        self.cache_size = 5000  # Increased in-memory cache for faster access
        
        # Initialize pool manager
        self.pool_manager = ProcessPoolRetry(max_retries=10)
        self.pool_manager.processes = self.num_processes

    def init_database(self):
        """Initialize SQLite database with optimized settings."""
        with self.database_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS paths (
                    number INTEGER,
                    path_list TEXT,
                    PRIMARY KEY (number, path_list)
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_number ON paths(number)")
            
            # Optimize SQLite settings for bulk operations
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
            conn.execute("PRAGMA cache_size = -4000000")  # Use 8GB of memory for cache
            conn.execute("PRAGMA temp_store = MEMORY")
            conn.execute("PRAGMA mmap_size = 100000000000")  # 100GB max mapping


    @contextmanager
    def database_connection(self):
        """Context manager for database connections with retry logic."""
        max_retries = 10
        conn = None
        for attempt in range(max_retries):
            try:
                conn = sqlite3.connect(self.db_path, timeout=60)
                yield conn
                break
            except sqlite3.OperationalError as e:
                if attempt == max_retries - 1:
                    raise
                self.logger.warning(f"Database connection failed (attempt {attempt + 1}/{max_retries}): {str(e)}")
                time.sleep(0.1 * (attempt + 1))
            finally:
                if 'conn' in locals():
                    conn.close()



    def find_prime_sums(self) -> None:
        """Find all ways to sum to target using non-repeating prime numbers with error recovery."""
        self.logger.info(f"Starting calculation with {self.num_processes} processes")
        
        start_num = max(1, self.start_from)
        
        try:
            for i in range(start_num, self.target + 1):
                try:
                    self.process_number(i)
                                            
                except Exception as e:
                    self.logger.error(f"Error processing number {i}: {str(e)}")
                    self.logger.error(traceback.format_exc())
                    # Continue with next number after error
                    continue
                    
        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal, cleaning up...")
            self.cleanup()
            sys.exit(0)
            
    def create_checkpoint(self, number: int):
        """Create a checkpoint of current progress."""
        checkpoint_file = self.output_dir / f"checkpoint_{self.target}.json"
        checkpoint_data = {
            "last_processed": number,
            "timestamp": datetime.now().isoformat()
        }
        with open(checkpoint_file, 'w') as f:
            json.dump(checkpoint_data, f)
            
    def load_paths(self, number: int) -> List[List[int]]:
        """Load paths from cache or database with error handling."""
        if number in self.path_cache:
            return self.path_cache[number]
            
        with self.database_connection() as conn:
            cursor = conn.execute(
                "SELECT path_list FROM paths WHERE number = ?", 
                (number,)
            )
            paths = [json.loads(row[0]) for row in cursor.fetchall()]
            
            if paths:
                self.path_cache[number] = paths
                # Manage cache size
                if len(self.path_cache) > self.cache_size:
                    oldest_key = min(k for k in self.path_cache.keys() if k != number)
                    del self.path_cache[oldest_key]
                    
            return paths

    def save_paths(self, number: int, paths: List[List[int]]) -> None:
        """Save paths to database and update cache with error handling."""
        if not paths:
            return
            
        self.path_cache[number] = paths
        
        # Batch insert paths
        with self.database_connection() as conn:
            try: 
                conn.executemany(
                    "INSERT OR REPLACE INTO paths (number, path_list) VALUES (?, ?)",
                    [(number, json.dumps(path)) for path in paths]
                )
                conn.commit()
            except sqlite3.Error as e:
                self.logger.error(f"Error saving paths for number {number}: {str(e)}")



    def process_number(self, i: int):
        """Process a single number with efficient chunking and parallelization."""
        paths = []
        max_path_dist = 0
        start_process_time = time.time()
        
        # Handle prime case first
        if i in self.primes_set:
            paths.append([i])
            max_path_dist = 1
        
        # Get all j values we need to process
        j_values = list(range(1, i//2 + 1))
        
        # Pre-load all required paths into a single dictionary
        paths_dict = {}
        for j in j_values:
            k = i - j
            # Load j paths
            j_paths = self.load_paths(j)
            if j_paths:
                paths_dict[j] = j_paths
            # Load k paths
            k_paths = self.load_paths(k)
            if k_paths:
                paths_dict[k] = k_paths
        
        # Create balanced chunks of j values
        chunk_size = max(1, len(j_values) // (self.num_processes * 2))  # Smaller chunks for better load balancing
        j_chunks = [j_values[x:x + chunk_size] for x in range(0, len(j_values), chunk_size)]
        
        # Prepare arguments for each chunk
        chunk_args = [(chunk, i, paths_dict) for chunk in j_chunks]
        
        try:
            # Process all chunks in parallel
            with self.pool_manager.get_pool(self.num_processes) as pool:
                chunk_results = pool.map(process_chunk, chunk_args)
                
                # Collect results
                for result in chunk_results:
                    paths.extend(result)
                    if result:
                        max_path_dist = max(max_path_dist, max(len(path) for path in result))
                        
        except Exception as e:
            self.logger.error(f"Error in parallel processing: {str(e)}")
            # Fallback to sequential processing
            self.logger.info("Falling back to sequential processing")
            paths = []
            for args in chunk_args:
                result = process_chunk(args)
                paths.extend(result)
                if result:
                    max_path_dist = max(max_path_dist, max(len(path) for path in result))
        
        # Remove duplicates and save results
        unique_paths = {tuple(path) for path in paths}
        paths = [list(path) for path in unique_paths]
        
        self.save_paths(i, paths)
        
        current_time = time.time()
        self.logger.info(
            f"Number: {i}\n"
            f"Unique paths: {len(unique_paths)}\n"
            f"Max path distance: {max_path_dist}\n"
            f"Time between logs: {current_time - start_process_time:.2f}s\n"
            f"{self.get_memory_usage()}\n"
        )

    def get_memory_usage(self) -> str:
        """Get current memory usage of the process."""
        process = psutil.Process()
        memory_info = process.memory_info()
        return f"Memory RSS: {memory_info.rss / 1024 / 1024:.2f} MB"

    def cleanup(self):
        """Clean up resources with error handling."""
           
        self.path_cache.clear()
        
        try:
            with self.database_connection() as conn:
                conn.execute("PRAGMA optimize")
        except Exception as e:
            self.logger.error(f"Error optimizing database: {str(e)}")

if __name__ == '__main__':
    mp.freeze_support()
    
    calculator = SQLitePrimeSumCalculator(
        target=2025,
        output_dir="prime_sums_output",
        start_from=416,
        num_processes=mp.cpu_count() - 2,
        checkpoint_interval=10
    )
    
    try:
        calculator.find_prime_sums()
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        logging.error(traceback.format_exc())
    finally:
        calculator.cleanup()