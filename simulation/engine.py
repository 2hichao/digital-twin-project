"""
Engine module for the NIO Digital Twin Simulation.

This module contains the SimulationEngine class, which sets up the simulation environment,
integrates various simulation processes such as production, optional quality control, and a
placeholder for maintenance. Each process is managed using the simpy library.
"""

import simpy
import time
import logging
import random

from simulation.models import ProductionLine

# Optional module: QualityCheck. If not available, the quality check process will be skipped.
try:
    from simulation.models import QualityCheck
except ImportError:
    QualityCheck = None

class SimulationEngine:
    """
    The SimulationEngine class sets up the simulation environment and manages simulation processes.
    
    Attributes:
        env (simpy.Environment): The simulation environment.
        production_line (ProductionLine): The production line simulation process.
        quality_check (QualityCheck or None): Optional quality control simulation process.
        simulation_duration (int): The total simulation time in time units.
        logger (logging.Logger): Logger for simulation events.
    """
    
    def __init__(self, simulation_duration=1000, seed=None):
        """
        Initialize the SimulationEngine with a simulation duration and optional random seed.

        Args:
            simulation_duration (int): Total time units to run the simulation.
            seed (int): Optional seed for reproducible random behavior.
        """
        self.simulation_duration = simulation_duration
        if seed is not None:
            random.seed(seed)
        self.env = simpy.Environment()
        self.production_line = ProductionLine(self.env)
        self.quality_check = QualityCheck(self.env, self.production_line) if QualityCheck else None
        
        self.setup_logging()
        self.logger.info("SimulationEngine instance created with duration=%d", self.simulation_duration)
    
    def setup_logging(self):
        """
        Set up logging for the simulation engine.
        
        Configures the logger to display the time, log level, and message.
        """
        self.logger = logging.getLogger("SimulationEngine")
        self.logger.setLevel(logging.DEBUG)
        
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                fmt="%(asctime)s [%(levelname)s] (%(name)s): %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    def start_processes(self):
        """
        Start simulation processes within the environment.
        
        This method starts the production process, optional quality control process, and includes
        a placeholder for a maintenance process.
        """
        self.logger.info("Starting simulation processes.")
        
        # Start the production process.
        self.logger.debug("Launching production process.")
        self.env.process(self.production_line.start_production())
        
        # Start the quality control process if available.
        if self.quality_check is not None:
            self.logger.debug("Launching quality check process.")
            self.env.process(self.quality_check.run_quality_checks())
        else:
            self.logger.debug("Quality check process not available.")
        
        # Placeholder: Maintenance process can be added in future revisions.
        self.logger.debug("Maintenance process not implemented in this version.")
    
    def run_simulation(self):
        """
        Execute the simulation environment.

        This method runs all simulation processes until the simulation_duration is reached.
        It logs the start and end times, along with the total execution time in real seconds.
        """
        self.logger.info("Preparing simulation run for %d time units.", self.simulation_duration)
        self.start_processes()
        
        start_time = time.time()
        try:
            self.logger.debug("Running simulation environment.")
            self.env.run(until=self.simulation_duration)
        except Exception as ex:
            self.logger.error("An error occurred during simulation run: %s", ex)
        finally:
            end_time = time.time()
            elapsed = end_time - start_time
            self.logger.info("Simulation run complete. Elapsed real time: %.2f seconds.", elapsed)
        
        self.post_process_results()
    
    def post_process_results(self):
        """
        Handle post-simulation processing.

        This method logs simulation results such as the total number of vehicles produced.
        It can be extended to include report generation or further data analysis.
        """
        self.logger.info("Processing simulation results.")
        try:
            produced_count = self.production_line.get_produced_count()
            self.logger.info("Total vehicles produced: %d", produced_count)
        except Exception as ex:
            self.logger.error("Error during result processing: %s", ex)
    
    def reset_simulation(self):
        """
        Reset the simulation environment.

        Clears the current simulation state and restarts the environment. This is useful for running
        multiple simulation sessions without restarting the application.
        """
        self.logger.info("Resetting simulation environment.")
        self.env = simpy.Environment()
        self.production_line = ProductionLine(self.env)
        if QualityCheck:
            self.quality_check = QualityCheck(self.env, self.production_line)
        else:
            self.quality_check = None
        self.logger.debug("Simulation environment reset complete.")
    
    def run_multiple_simulations(self, runs=3):
        """
        Run the simulation multiple times for analysis.

        Args:
            runs (int): The number of simulation runs to execute.
        """
        self.logger.info("Starting multiple simulation runs: %d runs.", runs)
        for i in range(1, runs + 1):
            self.logger.info("Starting simulation run %d.", i)
            self.run_simulation()
            self.reset_simulation()
            self.logger.info("Completed simulation run %d.", i)
        self.logger.info("All simulation runs have been completed.")

# Additional helper functions or classes for future simulation modules can be added here.

if __name__ == "__main__":
    # Run the simulation engine directly if this module is executed as a script.
    engine = SimulationEngine(simulation_duration=1000, seed=123)
    engine.run_simulation()
    
    # Optionally, to run multiple simulation sessions for analysis, uncomment the following:
    # engine.run_multiple_simulations(runs=3)
