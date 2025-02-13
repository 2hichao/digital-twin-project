#!/usr/bin/env python3
"""
NIO Digital Twin Main Entry

This file starts the simulation engine and the web server for the NIO Digital Twin.
It manages configuration, logging, thread monitoring, and a graceful shutdown procedure.
"""

import sys
import time
import logging
import threading
import signal
import argparse

from simulation.engine import SimulationEngine
from web_server.server import start_server

# Global flag to signal shutdown across threads.
shutdown_flag = False

def signal_handler(sig, frame):
    """
    Handle external termination signals (SIGINT, SIGTERM).
    Sets the shutdown flag to begin the graceful shutdown process.
    """
    global shutdown_flag
    logging.info("Signal %s received. Initiating shutdown procedure.", sig)
    shutdown_flag = True

def run_simulation(sim_time):
    """
    Run the simulation engine.

    Args:
        sim_time (int): The number of time units for the simulation run.
    """
    logging.info("Simulation engine thread starting with simulation time: %s", sim_time)
    engine = SimulationEngine()  # You can pass sim_time to the engine if needed.
    try:
        engine.run_simulation()
    except Exception as ex:
        logging.error("Exception in simulation engine: %s", ex)
    logging.info("Simulation engine thread has ended.")

def run_web_server():
    """
    Run the web server that provides API endpoints and visualization.
    """
    logging.info("Web server thread starting.")
    try:
        start_server()
    except Exception as ex:
        logging.error("Exception in web server: %s", ex)
    logging.info("Web server thread has ended.")

def monitor_threads(threads):
    """
    Monitor the provided threads and log their status periodically.
    
    Args:
        threads (list): List of thread objects to monitor.
    """
    logging.info("Thread monitor started.")
    while not shutdown_flag:
        for thread in threads:
            if not thread.is_alive():
                logging.warning("Thread '%s' is not active.", thread.name)
        # Wait a short period before checking again.
        time.sleep(5)
    logging.info("Thread monitor detected shutdown flag; stopping monitoring.")

def print_configuration(args):
    """
    Print the startup configuration to the log.

    Args:
        args: Parsed command-line arguments.
    """
    logging.info("Configuration Settings:")
    logging.info("  Simulation Time: %s", args.sim_time)
    logging.info("  Logging Level: %s", args.log_level.upper())

def main():
    """
    Main function to parse arguments, initialize threads, and handle shutdown.
    """
    parser = argparse.ArgumentParser(description="NIO Digital Twin Project Main Entry")
    parser.add_argument("--sim-time", type=int, default=1000,
                        help="Number of simulation time units to run the simulation engine.")
    parser.add_argument("--log-level", type=str, default="INFO",
                        help="Set the logging level: DEBUG, INFO, WARNING, ERROR, CRITICAL")
    args = parser.parse_args()

    # Configure logging format and level.
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] (%(threadName)s): %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    logging.info("Starting NIO Digital Twin Project.")
    print_configuration(args)

    # Set up signal handlers for graceful shutdown.
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Create threads for simulation engine and web server.
    sim_thread = threading.Thread(
        target=run_simulation,
        name="SimulationThread",
        args=(args.sim_time,)
    )
    server_thread = threading.Thread(
        target=run_web_server,
        name="WebServerThread"
    )
    
    # Start the simulation and web server threads.
    sim_thread.start()
    server_thread.start()

    # Start an additional thread to monitor the simulation and server threads.
    monitor_thread = threading.Thread(
        target=monitor_threads,
        name="MonitorThread",
        args=([sim_thread, server_thread],)
    )
    monitor_thread.start()

    # Main loop: wait for a shutdown signal.
    try:
        while not shutdown_flag:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Keyboard interrupt received; setting shutdown flag.")
        global shutdown_flag
        shutdown_flag = True

    logging.info("Shutdown flag is active. Waiting for threads to finish.")

    # Attempt to join all threads with a timeout.
    sim_thread.join(timeout=10)
    server_thread.join(timeout=10)
    monitor_thread.join(timeout=10)

    # Check if any threads are still active.
    if sim_thread.is_alive():
        logging.warning("Simulation thread did not finish in the allotted time.")
    if server_thread.is_alive():
        logging.warning("Web server thread did not finish in the allotted time.")
    if monitor_thread.is_alive():
        logging.warning("Monitor thread did not finish in the allotted time.")

    logging.info("All threads have been handled. Exiting program.")
    sys.exit(0)

if __name__ == "__main__":
    main()
