"""
Data ingestion module for the NIO Digital Twin Simulation.

This module handles the ingestion of data from sensors and simulation events.
It collects, processes, and stores the data in memory and can write it to disk.
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


class DataIngestion:
    """
    Handles data ingestion for simulation events and sensor data.

    Attributes:
        data (pd.DataFrame): DataFrame that stores ingested records.
        running (bool): Flag to control the ingestion thread.
        ingestion_thread (threading.Thread): Thread that runs the ingestion process.
        lock (threading.Lock): Lock for thread-safe operations on the data.
        flush_interval (int): Interval (in seconds) for writing data to disk.
        output_file (str): File path to write the data.
    """

    def __init__(self, flush_interval=10, output_file="ingested_data.csv"):
        # Create an empty DataFrame with defined columns.
        self.data = pd.DataFrame(columns=["timestamp", "vehicle_id", "event", "value"])
        self.running = False
        self.lock = threading.Lock()
        self.flush_interval = flush_interval
        self.output_file = output_file
        self.ingestion_thread = None
        self.flush_thread = None
        logger.info("DataIngestion instance created with flush_interval=%d, output_file=%s",
                    flush_interval, output_file)

    def start(self):
        """
        Start the data ingestion process along with periodic data flushing.
        """
        if self.running:
            logger.warning("Data ingestion is already running.")
            return
        self.running = True
        self.ingestion_thread = threading.Thread(
            target=self._ingest_data, name="DataIngestionThread", daemon=True
        )
        self.ingestion_thread.start()
        # Start a thread to flush data to disk periodically.
        self.flush_thread = threading.Thread(
            target=self._flush_data_periodically, name="DataFlushThread", daemon=True
        )
        self.flush_thread.start()
        logger.info("Data ingestion and flush threads started.")

    def stop(self):
        """
        Stop the data ingestion process and wait for threads to finish.
        """
        if not self.running:
            logger.warning("Data ingestion is not running.")
            return
        self.running = False
        if self.ingestion_thread is not None:
            self.ingestion_thread.join(timeout=5)
        if self.flush_thread is not None:
            self.flush_thread.join(timeout=5)
        logger.info("Data ingestion stopped.")

    def _ingest_data(self):
        """
        Simulate the ingestion of data.

        This method simulates reading data from sensors or simulation events and
        stores each record in a DataFrame.
        """
        logger.info("Data ingestion thread started.")
        while self.running:
            try:
                # Simulate a sensor reading.
                timestamp = time.time()
                vehicle_id = random.randint(1, 1000)
                event = random.choice(["produced", "assembled", "tested", "inspected"])
                value = random.uniform(0, 100)  # Example sensor measurement.

                # Create a new data record.
                record = {"timestamp": timestamp, "vehicle_id": vehicle_id, "event": event, "value": value}

                # Append the new record safely.
                with self.lock:
                    self.data = self.data.append(record, ignore_index=True)

                logger.debug("Ingested data: %s", record)
                # Sleep to simulate delay between sensor readings.
                time.sleep(random.uniform(0.1, 0.5))
            except Exception as e:
                logger.error("Error during data ingestion: %s", e)
        logger.info("Data ingestion thread stopping.")

    def _flush_data_periodically(self):
        """
        Write the in-memory data to disk at regular intervals.
        """
        logger.info("Data flush thread started. Flushing every %d seconds.", self.flush_interval)
        while self.running:
            try:
                time.sleep(self.flush_interval)
                self.flush_data()
            except Exception as e:
                logger.error("Error during data flush: %s", e)
        # Final flush after stopping.
        self.flush_data()
        logger.info("Data flush thread stopping.")

    def flush_data(self):
        """
        Write the in-memory data to a CSV file.
        """
        with self.lock:
            try:
                if not self.data.empty:
                    if os.path.exists(self.output_file):
                        self.data.to_csv(self.output_file, mode='a', header=False, index=False)
                    else:
                        self.data.to_csv(self.output_file, index=False)
                    logger.info("Flushed %d records to %s", len(self.data), self.output_file)
                    # Clear data after flushing.
                    self.data = pd.DataFrame(columns=["timestamp", "vehicle_id", "event", "value"])
                else:
                    logger.debug("No new data to flush.")
            except Exception as e:
                logger.error("Error flushing data to disk: %s", e)

    def get_latest_data(self, num_records=100):
        """
        Return the latest records from the in-memory data.

        Args:
            num_records (int): Number of recent records to return.

        Returns:
            pd.DataFrame: The latest data records.
        """
        with self.lock:
            return self.data.tail(num_records).copy()

    def get_all_data(self):
        """
        Return all the data currently stored in memory.

        Returns:
            pd.DataFrame: The full set of data.
        """
        with self.lock:
            return self.data.copy()

    def clear_data(self):
        """
        Clear all data from memory.
        """
        with self.lock:
            self.data = pd.DataFrame(columns=["timestamp", "vehicle_id", "event", "value"])
            logger.info("In-memory data cleared.")

    def simulate_bulk_ingestion(self, num_records=1000):
        """
        Simulate the ingestion of a large number of records.

        Args:
            num_records (int): Number of records to simulate.
        """
        logger.info("Starting bulk ingestion of %d records.", num_records)
        for _ in range(num_records):
            try:
                timestamp = time.time()
                vehicle_id = random.randint(1, 1000)
                event = random.choice(["produced", "assembled", "tested", "inspected", "packaged"])
                value = random.uniform(0, 100)
                record = {"timestamp": timestamp, "vehicle_id": vehicle_id, "event": event, "value": value}
                with self.lock:
                    self.data = self.data.append(record, ignore_index=True)
            except Exception as e:
                logger.error("Error during bulk ingestion: %s", e)
        logger.info("Bulk ingestion completed.")

    def process_data(self):
        """
        Process the in-memory data to generate summary statistics.

        Returns:
            dict: A dictionary with record counts for each event type.
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
                logger.error("Error during data processing: %s", e)
                return {}

    def run_ingestion_loop(self):
        """
        Run a demonstration loop for data ingestion and processing.

        This method starts ingestion, runs for a set period, then stops ingestion,
        flushes data, and prints summary statistics.
        """
        logger.info("Starting demonstration ingestion loop.")
        self.start()
        try:
            # Run for a fixed period.
            time.sleep(15)
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received during ingestion loop.")
        finally:
            self.stop()
            # Final flush after stopping.
            self.flush_data()
            summary = self.process_data()
            logger.info("Final data summary: %s", summary)
            logger.info("Demonstration ingestion loop finished.")


if __name__ == "__main__":
    # Test the DataIngestion module independently.
    ingestion = DataIngestion(flush_interval=5, output_file="test_ingested_data.csv")
    ingestion.run_ingestion_loop()
