"""
Data ingestion module for the NIO Digital Twin Simulation.

This module handles the ingestion of data from simulated sensors and simulation events.
It collects, processes, and stores the data in memory and writes it to disk as CSV files
as well as persisting the data into a SQLite database. It includes extensive error handling,
logging, and multiple methods to process and retrieve the data.
"""

import random
import time
import threading
import logging
import pandas as pd
import os
import sqlite3
from sqlite3 import Error

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

    This class collects data records from simulated sensor readings and events.
    It stores the data in an in-memory DataFrame and writes it to a CSV file and a SQLite database
    at regular intervals. It also provides methods to process and retrieve data.
    """

    def __init__(self, flush_interval=10, output_file="ingested_data.csv", db_file="ingestion_data.db"):
        # Create an empty DataFrame with defined columns.
        self.data = pd.DataFrame(columns=["timestamp", "vehicle_id", "event", "value"])
        self.running = False
        self.lock = threading.Lock()
        self.flush_interval = flush_interval
        self.output_file = output_file
        self.db_file = db_file
        self.ingestion_thread = None
        self.flush_thread = None

        # Initialize the database.
        self._init_db()

        logger.info("DataIngestion instance created with flush_interval=%d, output_file=%s, db_file=%s",
                    flush_interval, output_file, db_file)

    def _init_db(self):
        """
        Initialize the SQLite database.
        
        Creates a table for ingested data if it does not already exist.
        """
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ingestion_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp REAL,
                    vehicle_id INTEGER,
                    event TEXT,
                    value REAL
                )
            ''')
            conn.commit()
            conn.close()
            logger.info("Database initialized and table ensured.")
        except Error as e:
            logger.error("Error initializing database: %s", e)

    def _insert_record_db(self, record):
        """
        Insert a single record into the SQLite database.
        
        Args:
            record (dict): A dictionary containing the record data.
        """
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO ingestion_data (timestamp, vehicle_id, event, value)
                VALUES (?, ?, ?, ?)
            ''', (record["timestamp"], record["vehicle_id"], record["event"], record["value"]))
            conn.commit()
            conn.close()
            logger.debug("Record inserted into database: %s", record)
        except Error as e:
            logger.error("Error inserting record into database: %s", e)

    def get_sensor_data(self):
        """
        Simulate the retrieval of sensor data from a real sensor.

        Returns:
            dict: A dictionary with sensor data.
        """
        try:
            # Simulate a sensor reading.
            timestamp = time.time()
            vehicle_id = random.randint(1, 1000)
            event = random.choice(["produced", "assembled", "tested", "inspected", "packaged"])
            value = random.uniform(0, 100)
            sensor_data = {
                "timestamp": timestamp,
                "vehicle_id": vehicle_id,
                "event": event,
                "value": value
            }
            logger.debug("Simulated sensor data: %s", sensor_data)
            return sensor_data
        except Exception as e:
            logger.error("Error retrieving sensor data: %s", e)
            return None

    def _ingest_data(self):
        """
        Continuously ingest data from simulated sensors and events.

        This method simulates reading data from real sensors or production systems,
        and appends each record to the in-memory DataFrame as well as inserts it into the database.
        """
        logger.info("Data ingestion thread started.")
        while self.running:
            try:
                record = self.get_sensor_data()
                if record:
                    with self.lock:
                        self.data = self.data.append(record, ignore_index=True)
                    # Insert the record into the database.
                    self._insert_record_db(record)
                time.sleep(random.uniform(0.1, 0.5))
            except Exception as e:
                logger.error("Error during data ingestion loop: %s", e)
                time.sleep(1)
        logger.info("Data ingestion thread stopping.")

    def _flush_data_periodically(self):
        """
        Periodically flush the in-memory data to a CSV file.

        This method writes the in-memory DataFrame to a CSV file at regular intervals
        and then clears the DataFrame.
        """
        logger.info("Data flush thread started. Flushing every %d seconds.", self.flush_interval)
        while self.running:
            try:
                time.sleep(self.flush_interval)
                self.flush_data()
            except Exception as e:
                logger.error("Error during periodic data flush: %s", e)
        # Final flush after stopping.
        self.flush_data()
        logger.info("Data flush thread stopping.")

    def flush_data(self):
        """
        Write the in-memory data to a CSV file.

        After writing, the in-memory DataFrame is cleared.
        """
        with self.lock:
            try:
                if not self.data.empty:
                    if os.path.exists(self.output_file):
                        self.data.to_csv(self.output_file, mode='a', header=False, index=False)
                    else:
                        self.data.to_csv(self.output_file, index=False)
                    logger.info("Flushed %d records to %s", len(self.data), self.output_file)
                    self.data = pd.DataFrame(columns=["timestamp", "vehicle_id", "event", "value"])
                else:
                    logger.debug("No new data to flush.")
            except Exception as e:
                logger.error("Error flushing data to CSV: %s", e)

    def get_latest_data(self, num_records=100):
        """
        Return the latest records from the in-memory data.

        Args:
            num_records (int): The number of recent records to return.

        Returns:
            pd.DataFrame: A DataFrame containing the latest records.
        """
        with self.lock:
            try:
                return self.data.tail(num_records).copy()
            except Exception as e:
                logger.error("Error retrieving latest data: %s", e)
                return pd.DataFrame()

    def get_all_data(self):
        """
        Return a copy of all the data currently stored in memory.

        Returns:
            pd.DataFrame: A DataFrame with all the data.
        """
        with self.lock:
            try:
                return self.data.copy()
            except Exception as e:
                logger.error("Error retrieving all data: %s", e)
                return pd.DataFrame()

    def clear_data(self):
        """
        Clear all in-memory data.
        """
        with self.lock:
            self.data = pd.DataFrame(columns=["timestamp", "vehicle_id", "event", "value"])
            logger.info("In-memory data cleared.")

    def simulate_bulk_ingestion(self, num_records=1000):
        """
        Simulate the ingestion of a large number of records.

        This method is useful for testing data processing performance.
        Args:
            num_records (int): The number of records to simulate.
        """
        logger.info("Starting bulk ingestion of %d records.", num_records)
        for _ in range(num_records):
            try:
                record = self.get_sensor_data()
                if record:
                    with self.lock:
                        self.data = self.data.append(record, ignore_index=True)
                    self._insert_record_db(record)
            except Exception as e:
                logger.error("Error during bulk ingestion: %s", e)
        logger.info("Bulk ingestion completed.")

    def process_data(self):
        """
        Process the in-memory data to generate summary statistics.

        Returns:
            dict: A dictionary with counts of records per event type.
        """
        with self.lock:
            try:
                if self.data.empty:
                    logger.warning("No data available for processing.")
                    return {}
                summary = self.data.groupby("event").size().to_dict()
                logger.info("Data processing summary: %s", summary)
                return summary
            except Exception as e:
                logger.error("Error processing data: %s", e)
                return {}

    def run_ingestion_loop(self):
        """
        Run a demonstration loop for data ingestion and processing.

        This method starts the ingestion and flush threads, runs for a set period,
        then stops ingestion, flushes data, and prints a summary.
        """
        logger.info("Starting demonstration ingestion loop.")
        self.start()
        try:
            # Run the ingestion for a fixed period.
            time.sleep(15)
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received during ingestion loop.")
        finally:
            self.stop()
            # Final flush after stopping.
            self.flush_data()
            summary = self.process_data()
            logger.info("Final data processing summary: %s", summary)
            logger.info("Demonstration ingestion loop finished.")

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


if __name__ == "__main__":
    # Standalone test for the DataIngestion module.
    ingestion = DataIngestion(flush_interval=5, output_file="test_ingested_data.csv", db_file="test_ingestion_data.db")
    ingestion.run_ingestion_loop()
