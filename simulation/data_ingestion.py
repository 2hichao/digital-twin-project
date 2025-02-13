"""
Data ingestion module for the NIO Digital Twin Simulation.

This module handles the ingestion of data from sensors and simulation events.
It collects, processes, and stores the data in memory and writes it to disk.
Advanced thread management, extended configuration options, and robust error
recovery routines have been added to support industrial-level usage.
"""

import random
import time
import threading
import logging
import pandas as pd
import os

# Set up module-level logging.
logger = logging.getLogger("data_ingestion")
logger.setLevel(logging.DEBUG)
if not logger.handlers:
    ch = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] (%(name)s): %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    ch.setFormatter(formatter)
    logger.addHandler(ch)


class WorkerThread:
    """
    WorkerThread wraps a target function to run in its own thread with error
    handling and automatic restart capabilities.

    Attributes:
        name (str): Name of the worker thread.
        target (callable): The function to execute.
        args (tuple): Positional arguments for the target function.
        kwargs (dict): Keyword arguments for the target function.
        restart_limit (int): Maximum number of restart attempts.
    """
    def __init__(self, name, target, args=(), kwargs=None, restart_limit=3):
        self.name = name
        self.target = target
        self.args = args
        self.kwargs = kwargs if kwargs is not None else {}
        self.restart_limit = restart_limit
        self.restart_count = 0
        self.thread = None
        self.stop_event = threading.Event()
        self.lock = threading.Lock()

    def run_wrapper(self):
        """
        Execute the target function in a loop. If an exception occurs, log it and
        attempt to restart the function up to the restart limit.
        """
        while not self.stop_event.is_set():
            try:
                logger.info("Worker %s started.", self.name)
                self.target(*self.args, **self.kwargs)
                logger.info("Worker %s completed normally.", self.name)
                break  # Exit if target finishes normally.
            except Exception as e:
                logger.error("Error in worker %s: %s", self.name, e, exc_info=True)
                self.restart_count += 1
                if self.restart_count > self.restart_limit:
                    logger.error("Worker %s exceeded restart limit. Exiting.", self.name)
                    break
                logger.info("Restarting worker %s (attempt %d).", self.name, self.restart_count)
                time.sleep(2)  # Pause before attempting restart.

    def start(self):
        """
        Start the worker thread.
        """
        with self.lock:
            self.stop_event.clear()
            self.thread = threading.Thread(target=self.run_wrapper, name=self.name)
            self.thread.daemon = True
            self.thread.start()
            logger.info("Worker thread %s started.", self.name)

    def is_alive(self):
        """
        Check if the worker thread is still running.
        """
        with self.lock:
            return self.thread is not None and self.thread.is_alive()

    def stop(self):
        """
        Signal the worker thread to stop and wait for it to finish.
        """
        with self.lock:
            self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=5)
            logger.info("Worker thread %s stopped.", self.name)


class DataIngestion:
    """
    Handles data ingestion for simulation events and sensor data.

    Attributes:
        data (pd.DataFrame): DataFrame that stores ingested records.
        running (bool): Flag indicating if ingestion is running.
        lock (threading.Lock): Lock for thread-safe operations on the data.
        flush_interval (int): Time interval in seconds to flush data to disk.
        output_file (str): File path for storing ingested data.
        ingestion_interval_min (float): Minimum delay between ingestions.
        ingestion_interval_max (float): Maximum delay between ingestions.
        ingestion_worker (WorkerThread): Worker thread for data ingestion.
        flush_worker (WorkerThread): Worker thread for data flushing.
        config (dict): Configuration dictionary for various parameters.
    """
    def __init__(self, config=None):
        # Default configuration parameters.
        default_config = {
            "flush_interval": 10,
            "output_file": "ingested_data.csv",
            "ingestion_interval_min": 0.1,
            "ingestion_interval_max": 0.5,
            "columns": ["timestamp", "vehicle_id", "event", "value"],
            "restart_limit": 3
        }
        # Merge provided config with defaults.
        if config is None:
            config = {}
        self.config = {**default_config, **config}
        
        # Initialize an empty DataFrame.
        self.data = pd.DataFrame(columns=self.config["columns"])
        self.running = False
        self.lock = threading.Lock()
        self.flush_interval = self.config["flush_interval"]
        self.output_file = self.config["output_file"]
        self.ingestion_interval_min = self.config["ingestion_interval_min"]
        self.ingestion_interval_max = self.config["ingestion_interval_max"]
        self.ingestion_worker = None
        self.flush_worker = None
        
        logger.info("DataIngestion instance created with config: %s", self.config)
    
    def _ingest_data(self):
        """
        Simulate the ingestion of data from sensors or simulation events.

        This method runs in a loop, generating new data records and appending them
        to the in-memory DataFrame with robust error handling.
        """
        logger.info("Data ingestion thread started.")
        while self.running:
            try:
                timestamp = time.time()
                vehicle_id = random.randint(1, 1000)
                event = random.choice(["produced", "assembled", "tested", "inspected", "quality", "packaged"])
                value = random.uniform(0, 100)
                record = {"timestamp": timestamp, "vehicle_id": vehicle_id, "event": event, "value": value}
                
                with self.lock:
                    self.data = self.data.append(record, ignore_index=True)
                
                logger.debug("Ingested record: %s", record)
                # Sleep for a random interval to simulate sensor delay.
                time.sleep(random.uniform(self.ingestion_interval_min, self.ingestion_interval_max))
            except Exception as e:
                logger.error("Error during ingestion: %s", e, exc_info=True)
                # Recovery: pause briefly before retrying.
                time.sleep(1)
        logger.info("Data ingestion thread stopping.")
    
    def _flush_data_periodically(self):
        """
        Flush the in-memory data to disk at regular intervals.

        This method runs in a loop, calling flush_data() every flush_interval seconds.
        """
        logger.info("Data flush thread started. Flush interval: %d seconds", self.flush_interval)
        while self.running:
            try:
                time.sleep(self.flush_interval)
                self.flush_data()
            except Exception as e:
                logger.error("Error during data flush: %s", e, exc_info=True)
        # Final flush upon stopping.
        self.flush_data()
        logger.info("Data flush thread stopping.")
    
    def flush_data(self):
        """
        Write the in-memory DataFrame to a CSV file and clear the DataFrame.

        If the file exists, data is appended without headers; otherwise, a new file is created.
        """
        with self.lock:
            try:
                if not self.data.empty:
                    if os.path.exists(self.output_file):
                        self.data.to_csv(self.output_file, mode='a', header=False, index=False)
                    else:
                        self.data.to_csv(self.output_file, index=False)
                    logger.info("Flushed %d records to %s", len(self.data), self.output_file)
                    # Clear the DataFrame after flushing.
                    self.data = pd.DataFrame(columns=self.config["columns"])
                else:
                    logger.debug("No new data to flush.")
            except Exception as e:
                logger.error("Error flushing data to disk: %s", e, exc_info=True)
    
    def start(self):
        """
        Start the data ingestion and flushing processes using WorkerThread.
        """
        if self.running:
            logger.warning("Data ingestion is already running.")
            return
        self.running = True
        self.ingestion_worker = WorkerThread(
            name="DataIngestionWorker",
            target=self._ingest_data,
            restart_limit=self.config["restart_limit"]
        )
        self.ingestion_worker.start()
        self.flush_worker = WorkerThread(
            name="DataFlushWorker",
            target=self._flush_data_periodically,
            restart_limit=self.config["restart_limit"]
        )
        self.flush_worker.start()
        logger.info("Data ingestion and flush workers started.")
    
    def stop(self):
        """
        Stop the data ingestion and flushing processes.
        """
        if not self.running:
            logger.warning("Data ingestion is not running.")
            return
        self.running = False
        if self.ingestion_worker:
            self.ingestion_worker.stop()
        if self.flush_worker:
            self.flush_worker.stop()
        logger.info("Data ingestion stopped.")
    
    def get_latest_data(self, num_records=100):
        """
        Retrieve the latest 'num_records' from the in-memory data.
        """
        with self.lock:
            return self.data.tail(num_records).copy()
    
    def get_all_data(self):
        """
        Retrieve all in-memory data.
        """
        with self.lock:
            return self.data.copy()
    
    def clear_data(self):
        """
        Clear the in-memory data.
        """
        with self.lock:
            self.data = pd.DataFrame(columns=self.config["columns"])
        logger.info("In-memory data cleared.")
    
    def simulate_bulk_ingestion(self, num_records=1000):
        """
        Simulate the ingestion of a large number of records in a tight loop.
        """
        logger.info("Starting bulk ingestion of %d records.", num_records)
        for _ in range(num_records):
            try:
                record = {
                    "timestamp": time.time(),
                    "vehicle_id": random.randint(1, 1000),
                    "event": random.choice(["produced", "assembled", "tested", "inspected", "quality", "packaged"]),
                    "value": random.uniform(0, 100)
                }
                with self.lock:
                    self.data = self.data.append(record, ignore_index=True)
            except Exception as e:
                logger.error("Error during bulk ingestion: %s", e, exc_info=True)
        logger.info("Bulk ingestion completed.")
    
    def process_data(self):
        """
        Process the in-memory data to generate summary statistics.
        
        Returns:
            dict: A summary dictionary with counts for each event type.
        """
        with self.lock:
            try:
                if self.data.empty:
                    logger.warning("No data available for processing.")
                    return {}
                summary = self.data.groupby("event").size().to_dict()
                logger.info("Processed data summary: %s", summary)
                return summary
            except Exception as e:
                logger.error("Error during data processing: %s", e, exc_info=True)
                return {}
    
    def run_ingestion_loop(self):
        """
        Demonstration loop for data ingestion.

        This method starts the ingestion, runs for a set period, stops ingestion,
        flushes data, and outputs summary statistics.
        """
        logger.info("Starting demonstration ingestion loop.")
        self.start()
        try:
            # Run for a fixed period, e.g., 20 seconds.
            time.sleep(20)
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received during ingestion loop.")
        finally:
            self.stop()
            self.flush_data()
            summary = self.process_data()
            logger.info("Final data summary: %s", summary)
            logger.info("Demonstration ingestion loop finished.")


if __name__ == "__main__":
    # Standalone test for the DataIngestion module.
    config = {
        "flush_interval": 5,
        "output_file": "test_ingested_data.csv",
        "ingestion_interval_min": 0.1,
        "ingestion_interval_max": 0.3,
        "restart_limit": 3
    }
    ingestion = DataIngestion(config=config)
    ingestion.run_ingestion_loop()
