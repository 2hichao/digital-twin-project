#!/usr/bin/env python3
"""
NIO Digital Twin Main Entry

This module starts the simulation engine and web server for the NIO Digital Twin.
It uses advanced thread management with custom worker threads, allowing for
automatic restart of failed threads up to a defined limit. It also provides extended
configuration options via command-line arguments and includes robust error recovery routines.
"""

import sys
import time
import logging
import threading
import signal
import argparse
import traceback

from simulation.engine import SimulationEngine
from web_server.server import start_server

# Global flag to signal shutdown across threads.
shutdown_flag = False

def signal_handler(sig, frame):
    """
    Handle external termination signals (SIGINT, SIGTERM).
    Sets the shutdown flag to initiate a graceful shutdown.
    """
    global shutdown_flag
    logging.info("Signal %s received. Initiating shutdown procedure.", sig)
    shutdown_flag = True

class WorkerThread:
    """
    WorkerThread wraps a target function to run in its own thread, with error handling
    and automatic restart capability up to a specified restart limit.
    
    Attributes:
        name (str): Name of the worker.
        target (callable): The function to run in the thread.
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
        Run the target function inside a loop. If an exception occurs,
        it logs the error and restarts the function if the restart limit is not reached.
        """
        while not self.stop_event.is_set():
            try:
                logging.info("Worker %s started.", self.name)
                self.target(*self.args, **self.kwargs)
                logging.info("Worker %s completed normally.", self.name)
                break  # Exit if target finishes normally.
            except Exception as e:
                logging.error("Error in worker %s: %s", self.name, e)
                logging.error("Traceback:\n%s", traceback.format_exc())
                if self.restart_count < self.restart_limit:
                    self.restart_count += 1
                    logging.info("Restarting worker %s (attempt %d of %d)...", 
                                 self.name, self.restart_count, self.restart_limit)
                else:
                    logging.error("Worker %s reached maximum restart limit. Exiting.", self.name)
                    break
            # Short pause before restarting the target.
            time.sleep(2)

    def start(self):
        """
        Start the worker thread.
        """
        with self.lock:
            self.stop_event.clear()
            self.thread = threading.Thread(target=self.run_wrapper, name=self.name)
            self.thread.start()
            logging.info("Worker thread %s started.", self.name)

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
            self.thread.join(timeout=10)
            logging.info("Worker thread %s stopped.", self.name)

def run_simulation_worker(sim_time):
    """
    Worker function to run the simulation engine.
    
    Args:
        sim_time (int): The simulation duration (in time units).
    """
    engine = SimulationEngine(simulation_duration=sim_time)
    engine.run_simulation()

def run_server_worker():
    """
    Worker function to run the web server.
    """
    start_server()

def main():
    """
    Main function to parse configuration, start worker threads, and monitor them.
    """
    parser = argparse.ArgumentParser(
        description="NIO Digital Twin Main Entry with Advanced Thread Management"
    )
    parser.add_argument("--sim-time", type=int, default=1000,
                        help="Number of simulation time units to run the simulation engine.")
    parser.add_argument("--log-level", type=str, default="INFO",
                        help="Logging level: DEBUG, INFO, WARNING, ERROR, CRITICAL")
    parser.add_argument("--server-port", type=int, default=8000,
                        help="Port for the web server to listen on.")
    parser.add_argument("--restart-limit", type=int, default=3,
                        help="Maximum number of restart attempts for each worker thread.")
    args = parser.parse_args()

    # Configure logging.
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] (%(threadName)s): %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    logging.info("Starting NIO Digital Twin Project with advanced thread management.")
    logging.info("Configuration: sim_time=%d, server_port=%d, restart_limit=%d",
                 args.sim_time, args.server_port, args.restart_limit)

    # Register signal handlers for graceful shutdown.
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Create worker threads for simulation and web server.
    simulation_worker = WorkerThread(
        name="SimulationWorker",
        target=run_simulation_worker,
        args=(args.sim_time,),
        restart_limit=args.restart_limit
    )
    server_worker = WorkerThread(
        name="ServerWorker",
        target=run_server_worker,
        restart_limit=args.restart_limit
    )

    # Start worker threads.
    simulation_worker.start()
    server_worker.start()

    # Main monitor loop: check worker threads periodically and attempt restart if needed.
    try:
        while not shutdown_flag:
            time.sleep(5)
            if not simulation_worker.is_alive():
                logging.warning("Simulation worker is not alive. Attempting restart...")
                simulation_worker.start()
            if not server_worker.is_alive():
                logging.warning("Server worker is not alive. Attempting restart...")
                server_worker.start()
    except Exception as e:
        logging.error("Exception in main loop: %s", e, exc_info=True)
    finally:
        logging.info("Shutdown initiated. Stopping all worker threads...")
        simulation_worker.stop()
        server_worker.stop()
        logging.info("All worker threads have been stopped. Exiting program.")
        sys.exit(0)

if __name__ == "__main__":
    main()
