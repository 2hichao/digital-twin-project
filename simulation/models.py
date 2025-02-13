"""
Models module for the NIO Digital Twin Simulation.

This module defines the classes representing the entities in the simulation,
such as vehicles, production stages, quality checks, and components.
"""

import simpy
import random
import time
import logging

# Set up module-level logging.
logger = logging.getLogger("models")
logger.setLevel(logging.DEBUG)
if not logger.handlers:
    ch = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] (%(name)s): %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    ch.setFormatter(formatter)
    logger.addHandler(ch)


class Vehicle:
    """
    Represents a vehicle produced on the production line.

    Attributes:
        id (int): Unique identifier for the vehicle.
        creation_time (float): Simulation time when the vehicle was created.
        status (str): Current status of the vehicle.
        assembly_history (list): Log of steps completed during production.
        quality_checks (list): Log of quality check results.
        components (dict): Dictionary of components added to the vehicle.
    """
    def __init__(self, vehicle_id, creation_time):
        self.id = vehicle_id
        self.creation_time = creation_time
        self.status = "created"
        self.assembly_history = []
        self.quality_checks = []
        self.components = {}
    
    def update_status(self, new_status):
        logger.debug("Vehicle %d: Changing status from %s to %s", self.id, self.status, new_status)
        self.status = new_status
    
    def add_assembly_step(self, step):
        timestamp = time.time()
        logger.debug("Vehicle %d: Adding assembly step: %s at %s", self.id, step, timestamp)
        self.assembly_history.append((step, timestamp))
    
    def add_quality_check(self, check_result):
        timestamp = time.time()
        logger.debug("Vehicle %d: Adding quality check result: %s at %s", self.id, check_result, timestamp)
        self.quality_checks.append((check_result, timestamp))
    
    def add_component(self, component_name, details):
        logger.debug("Vehicle %d: Adding component %s with details %s", self.id, component_name, details)
        self.components[component_name] = details
    
    def get_summary(self):
        summary = {
            "id": self.id,
            "status": self.status,
            "creation_time": self.creation_time,
            "assembly_history": self.assembly_history,
            "quality_checks": self.quality_checks,
            "components": self.components,
        }
        return summary


class ProductionLine:
    """
    Simulates the production process of vehicles.

    The production process includes several stations:
      - Assembly
      - Painting
      - Inspection
      - Testing

    Each station takes a random amount of time to process a vehicle.
    """
    def __init__(self, env):
        self.env = env
        self.vehicle_count = 0
        self.vehicles = []
        self.production_rate = 1  # Vehicles per simulation cycle
        self.station_durations = {
            "assembly": (2, 5),    # (min_time, max_time)
            "painting": (1, 3),
            "inspection": (1, 2),
            "testing": (2, 4)
        }
        self.logger = logger
    
    def produce_vehicle(self):
        """
        Create a new vehicle and add it to the production list.
        """
        self.vehicle_count += 1
        vehicle = Vehicle(vehicle_id=self.vehicle_count, creation_time=self.env.now)
        self.vehicles.append(vehicle)
        self.logger.info("Vehicle %d produced at simulation time %s", vehicle.id, self.env.now)
        return vehicle
    
    def start_production(self):
        """
        Process that continuously produces vehicles.
        """
        while True:
            vehicle = self.produce_vehicle()
            self.env.process(self.process_vehicle(vehicle))
            yield self.env.timeout(1 / self.production_rate)
    
    def process_vehicle(self, vehicle):
        """
        Process a single vehicle through all production stations.
        """
        yield self.env.process(self.assembly_station(vehicle))
        yield self.env.process(self.painting_station(vehicle))
        yield self.env.process(self.inspection_station(vehicle))
        yield self.env.process(self.testing_station(vehicle))
        vehicle.update_status("completed")
        self.logger.info("Vehicle %d production completed at simulation time %s", vehicle.id, self.env.now)
    
    def assembly_station(self, vehicle):
        """
        Simulate the assembly station.
        """
        vehicle.update_status("assembly")
        duration = random.uniform(*self.station_durations["assembly"])
        vehicle.add_assembly_step("assembly started")
        self.logger.debug("Vehicle %d: Assembly started, duration %.2f", vehicle.id, duration)
        yield self.env.timeout(duration)
        vehicle.add_assembly_step("assembly completed")
        self.logger.debug("Vehicle %d: Assembly completed", vehicle.id)
    
    def painting_station(self, vehicle):
        """
        Simulate the painting station.
        """
        vehicle.update_status("painting")
        duration = random.uniform(*self.station_durations["painting"])
        vehicle.add_assembly_step("painting started")
        self.logger.debug("Vehicle %d: Painting started, duration %.2f", vehicle.id, duration)
        yield self.env.timeout(duration)
        vehicle.add_assembly_step("painting completed")
        self.logger.debug("Vehicle %d: Painting completed", vehicle.id)
    
    def inspection_station(self, vehicle):
        """
        Simulate the inspection station.
        """
        vehicle.update_status("inspection")
        duration = random.uniform(*self.station_durations["inspection"])
        vehicle.add_assembly_step("inspection started")
        self.logger.debug("Vehicle %d: Inspection started, duration %.2f", vehicle.id, duration)
        yield self.env.timeout(duration)
        vehicle.add_assembly_step("inspection completed")
        self.logger.debug("Vehicle %d: Inspection completed", vehicle.id)
    
    def testing_station(self, vehicle):
        """
        Simulate the testing station.
        """
        vehicle.update_status("testing")
        duration = random.uniform(*self.station_durations["testing"])
        vehicle.add_assembly_step("testing started")
        self.logger.debug("Vehicle %d: Testing started, duration %.2f", vehicle.id, duration)
        yield self.env.timeout(duration)
        vehicle.add_assembly_step("testing completed")
        self.logger.debug("Vehicle %d: Testing completed", vehicle.id)
    
    def get_produced_count(self):
        """
        Return the total number of vehicles produced.
        """
        return len(self.vehicles)
    
    def get_vehicle_by_id(self, vehicle_id):
        """
        Retrieve a vehicle by its identifier.
        """
        for v in self.vehicles:
            if v.id == vehicle_id:
                return v
        return None


class QualityCheck:
    """
    Simulates the quality control process for vehicles.

    This process periodically picks vehicles and performs a quality check.
    """
    def __init__(self, env, production_line):
        self.env = env
        self.production_line = production_line
        self.check_interval = 5  # Time units between quality checks
        self.quality_threshold = 0.7  # Minimum score to pass
        self.logger = logger
    
    def run_quality_checks(self):
        """
        Process that periodically performs quality checks.
        """
        while True:
            if self.production_line.vehicles:
                vehicle = random.choice(self.production_line.vehicles)
                self.logger.info("Performing quality check on Vehicle %d at simulation time %s", vehicle.id, self.env.now)
                yield self.env.process(self.perform_quality_check(vehicle))
            yield self.env.timeout(self.check_interval)
    
    def perform_quality_check(self, vehicle):
        """
        Perform a quality check on a single vehicle.

        A random score is generated. If the score is above the threshold, the check is passed.
        """
        self.logger.debug("Vehicle %d: Quality check started.", vehicle.id)
        duration = random.uniform(0.5, 1.5)
        yield self.env.timeout(duration)
        quality_score = random.random()
        if quality_score >= self.quality_threshold:
            result = "passed"
        else:
            result = "failed"
        vehicle.add_quality_check({"score": quality_score, "result": result})
        self.logger.info("Vehicle %d quality check result: %s (score: %.2f)", vehicle.id, result, quality_score)


class Component:
    """
    Represents a component of a vehicle.

    Stores information about the component such as name, type, production time, and other properties.
    """
    def __init__(self, name, component_type, production_time, properties=None):
        self.name = name
        self.component_type = component_type
        self.production_time = production_time
        self.properties = properties if properties is not None else {}
        self.creation_time = time.time()
        logger.debug("Component %s created of type %s with production time %.2f", self.name, self.component_type, self.production_time)
    
    def update_property(self, key, value):
        logger.debug("Component %s: Updating property %s to %s", self.name, key, value)
        self.properties[key] = value
    
    def get_details(self):
        return {
            "name": self.name,
            "type": self.component_type,
            "production_time": self.production_time,
            "properties": self.properties,
            "creation_time": self.creation_time
        }


class AssemblyStation:
    """
    Simulates a station where components are assembled into a vehicle.

    Each station can handle a specific part of the production process.
    """
    def __init__(self, env, name, process_time_range=(1, 3)):
        self.env = env
        self.name = name
        self.process_time_range = process_time_range
        self.logger = logger
        self.processed_components = 0
    
    def assemble(self, vehicle, component_name):
        """
        Process to assemble a component onto a vehicle.
        """
        self.logger.info("Vehicle %d: Starting assembly at station %s for component %s", vehicle.id, self.name, component_name)
        duration = random.uniform(*self.process_time_range)
        yield self.env.timeout(duration)
        component = Component(component_name, "assembly", duration)
        vehicle.add_component(component_name, component.get_details())
        self.processed_components += 1
        self.logger.info("Vehicle %d: Assembly at station %s for component %s completed", vehicle.id, self.name, component_name)
    
    def get_processed_count(self):
        """
        Return the number of components processed at this station.
        """
        return self.processed_components


def simulate_production(env, production_line, assembly_stations):
    """
    Simulate a production run where vehicles are assembled with components from multiple stations.

    This function iterates over production cycles and assigns vehicles to each assembly station.
    """
    while True:
        vehicle = production_line.produce_vehicle()
        for station in assembly_stations:
            yield env.process(station.assemble(vehicle, f"component_{station.name}"))
        yield env.timeout(1)


if __name__ == "__main__":
    # Test the models by running a simple simulation.
    test_env = simpy.Environment()
    prod_line = ProductionLine(test_env)
    quality = QualityCheck(test_env, prod_line)
    
    # Create a couple of assembly stations.
    station_a = AssemblyStation(test_env, "A", process_time_range=(1, 2))
    station_b = AssemblyStation(test_env, "B", process_time_range=(1, 2))
    
    # Start the production process.
    test_env.process(prod_line.start_production())
    test_env.process(quality.run_quality_checks())
    test_env.process(simulate_production(test_env, prod_line, [station_a, station_b]))
    
    # Run the simulation for a set period.
    test_env.run(until=30)
    
    logger.info("Total vehicles produced: %d", prod_line.get_produced_count())
    for vehicle in prod_line.vehicles:
        logger.info("Vehicle %d summary: %s", vehicle.id, vehicle.get_summary())
