"""
Engine module for the NIO Digital Twin Simulation.

This module sets up the simulation environment and manages several simulation processes:
  - Multiple production lines (primary and secondary).
  - A maintenance process that periodically inspects and performs maintenance.
  - A supply chain process that replenishes raw materials.
  - A quality control process that inspects produced vehicles.

Each process is managed using the simpy library and includes detailed logging
for tracking simulation events and interactions.
"""

import simpy
import time
import logging
import random

# Import ProductionLine model from simulation/models.py.
from simulation.models import ProductionLine

class SimulationEngine:
    def __init__(self, simulation_duration=1000, seed=None):
        """
        Initialize the SimulationEngine with a simulation duration, an optional random seed,
        and create the simulation environment along with production lines and auxiliary processes.
        """
        if seed is not None:
            random.seed(seed)
        self.simulation_duration = simulation_duration
        self.env = simpy.Environment()

        # Set up a logger for the simulation engine.
        self.logger = logging.getLogger("SimulationEngine")
        self.logger.setLevel(logging.DEBUG)
        if not self.logger.handlers:
            ch = logging.StreamHandler()
            formatter = logging.Formatter(
                fmt="%(asctime)s [%(levelname)s] (%(name)s): %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

        # Create primary production line.
        self.primary_line = ProductionLine(self.env)
        # Create secondary production line (simulate an alternative production line).
        self.secondary_line = ProductionLine(self.env)
        # Maintain a list of production lines for unified management.
        self.production_lines = [self.primary_line, self.secondary_line]

        # Initialize maintenance log to store maintenance records.
        self.maintenance_log = []
        # Set initial raw material stock for production.
        self.raw_material_stock = 1000
        self.raw_material_threshold = 200  # Threshold to trigger supply orders.
        
        self.logger.info("SimulationEngine initialized with simulation_duration=%d", self.simulation_duration)
    
    def start_production_processes(self):
        """
        Start production processes for all production lines.
        """
        self.logger.info("Starting production processes for %d production lines.", len(self.production_lines))
        for idx, line in enumerate(self.production_lines):
            self.env.process(self.run_production_line(line, idx))
    
    def run_production_line(self, production_line, line_index):
        """
        Run the production process for a single production line.

        This process continuously produces vehicles if raw materials are available.
        It consumes raw materials for each produced vehicle and processes it through production steps.
        """
        self.logger.info("Production line %d process started.", line_index + 1)
        while True:
            # Check if there is sufficient raw material.
            if self.raw_material_stock < 1:
                self.logger.warning("Production line %d halted due to lack of raw materials.", line_index + 1)
                yield self.env.timeout(5)
                continue
            # Produce a vehicle.
            vehicle = production_line.produce_vehicle()
            self.logger.info("Production line %d produced vehicle %d at simulation time %d.",
                             line_index + 1, vehicle.id, self.env.now)
            # Consume a random amount of raw material.
            material_used = random.randint(5, 10)
            self.raw_material_stock -= material_used
            self.logger.debug("Production line %d consumed %d units. Stock left: %d.",
                              line_index + 1, material_used, self.raw_material_stock)
            # Process the vehicle through production stations.
            yield self.env.process(production_line.process_vehicle(vehicle))
            # Wait a random period before producing the next vehicle.
            yield self.env.timeout(random.uniform(0.5, 2))
    
    def maintenance_process(self):
        """
        Perform periodic maintenance on each production line.

        This process waits for a fixed interval, then checks each production line.
        With a certain probability, it performs maintenance on a line.
        """
        self.logger.info("Maintenance process started.")
        while True:
            # Wait a fixed interval before maintenance check.
            yield self.env.timeout(50)
            self.logger.info("Maintenance check triggered at simulation time %d.", self.env.now)
            for idx, line in enumerate(self.production_lines):
                # Determine if maintenance is needed (30% chance).
                if random.random() < 0.3:
                    self.logger.info("Maintenance required on production line %d.", idx + 1)
                    yield self.env.process(self.perform_maintenance(line, idx))
                else:
                    self.logger.info("Production line %d is operating normally.", idx + 1)
    
    def perform_maintenance(self, production_line, line_index):
        """
        Carry out maintenance on a production line.

        The maintenance duration is random. The process logs the maintenance activity.
        """
        self.logger.info("Performing maintenance on production line %d at simulation time %d.", line_index + 1, self.env.now)
        maintenance_duration = random.uniform(5, 15)
        yield self.env.timeout(maintenance_duration)
        maintenance_record = {
            "line": line_index + 1,
            "time": self.env.now,
            "duration": maintenance_duration
        }
        self.maintenance_log.append(maintenance_record)
        self.logger.info("Maintenance on production line %d completed. Duration: %.2f time units.",
                         line_index + 1, maintenance_duration)
    
    def supply_chain_process(self):
        """
        Simulate the supply chain process responsible for replenishing raw materials.

        This process checks the raw material stock at regular intervals and triggers a replenishment
        if the stock is below the defined threshold.
        """
        self.logger.info("Supply chain process started.")
        while True:
            # Check stock every 20 simulation time units.
            yield self.env.timeout(20)
            if self.raw_material_stock < self.raw_material_threshold:
                self.logger.info("Raw material stock low (%d units). Initiating supply order.", self.raw_material_stock)
                yield self.env.process(self.replenish_raw_materials())
            else:
                self.logger.info("Raw material stock sufficient (%d units).", self.raw_material_stock)
    
    def replenish_raw_materials(self):
        """
        Replenish the raw material stock.

        The process simulates an order that takes some time to deliver and then increases the stock.
        """
        self.logger.info("Replenishing raw materials at simulation time %d.", self.env.now)
        supply_duration = random.uniform(10, 20)
        yield self.env.timeout(supply_duration)
        materials_added = random.randint(300, 500)
        self.raw_material_stock += materials_added
        self.logger.info("Raw materials replenished. Added %d units. New stock: %d.", materials_added, self.raw_material_stock)
    
    def quality_control_process(self):
        """
        Run quality control inspections at regular intervals.

        This process selects a random vehicle from each production line for inspection.
        It then performs an inspection process that assigns a quality score.
        """
        self.logger.info("Quality control process started.")
        while True:
            yield self.env.timeout(30)
            for idx, line in enumerate(self.production_lines):
                if line.vehicles:
                    vehicle = random.choice(line.vehicles)
                    self.logger.info("Inspecting vehicle %d from production line %d at simulation time %d.",
                                     vehicle.id, idx + 1, self.env.now)
                    yield self.env.process(self.inspect_vehicle(vehicle, idx))
            # Small delay before the next inspection cycle.
            yield self.env.timeout(5)
    
    def inspect_vehicle(self, vehicle, line_index):
        """
        Inspect a vehicle and record the result.

        The inspection takes a short random time and assigns a random quality score.
        """
        inspection_duration = random.uniform(1, 3)
        yield self.env.timeout(inspection_duration)
        quality_score = random.uniform(0, 1)
        if quality_score > 0.7:
            result = "passed"
        else:
            result = "failed"
        vehicle.add_quality_check({"score": quality_score, "result": result})
        self.logger.info("Inspection of vehicle %d on production line %d: %s (score: %.2f).",
                         vehicle.id, line_index + 1, result, quality_score)
    
    def run_simulation(self):
        """
        Run the entire simulation until the specified simulation duration is reached.

        This method starts all production, maintenance, supply chain, and quality control processes.
        """
        self.logger.info("Starting simulation run for %d time units.", self.simulation_duration)
        # Start all production lines.
        self.start_production_processes()
        # Start the maintenance process.
        self.env.process(self.maintenance_process())
        # Start the supply chain process.
        self.env.process(self.supply_chain_process())
        # Start the quality control process.
        self.env.process(self.quality_control_process())

        start_time = time.time()
        try:
            self.env.run(until=self.simulation_duration)
        except Exception as ex:
            self.logger.error("Error during simulation run: %s", ex)
        finally:
            end_time = time.time()
            elapsed = end_time - start_time
            self.logger.info("Simulation run complete. Elapsed real time: %.2f seconds.", elapsed)
            self.post_simulation_report()
    
    def post_simulation_report(self):
        """
        Generate a report at the end of the simulation.

        The report includes the total number of vehicles produced, details of maintenance activities,
        and the final raw material stock.
        """
        self.logger.info("Generating post simulation report.")
        total_vehicles = sum(line.get_produced_count() for line in self.production_lines)
        self.logger.info("Total vehicles produced: %d", total_vehicles)
        self.logger.info("Maintenance activities log:")
        for record in self.maintenance_log:
            self.logger.info("  Production line %d: Maintenance at time %d, duration %.2f",
                             record["line"], record["time"], record["duration"])
        self.logger.info("Final raw material stock: %d", self.raw_material_stock)


if __name__ == "__main__":
    # For standalone testing of the simulation engine.
    engine = SimulationEngine(simulation_duration=500, seed=42)
    engine.run_simulation()
