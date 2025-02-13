"""
Models module for the NIO Digital Twin Simulation.

This module defines classes representing the entities in the simulation,
including vehicles with rich properties, production lines with detailed
production steps, advanced quality check logic, and various production stations.
Additional classes such as Component, AssemblyStation, and PaintingStation
are also defined to simulate various parts of the production process.
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

###############################################################################
# Vehicle Class
###############################################################################

class Vehicle:
    """
    Represents a vehicle produced on the production line with rich properties.
    
    Attributes:
        id (int): Unique identifier for the vehicle.
        creation_time (float): Simulation time when the vehicle was created.
        status (str): Current status of the vehicle.
        color (str): Vehicle color.
        engine_type (str): Type of engine installed.
        components (dict): Dictionary storing installed components.
        production_history (list): Log of production steps and their timestamps.
        quality_history (list): Log of quality checks performed on the vehicle.
        maintenance_needed (bool): Flag indicating if maintenance is required.
        additional_features (dict): Extra configurable features.
    """
    def __init__(self, vehicle_id, creation_time):
        self.id = vehicle_id
        self.creation_time = creation_time
        self.status = "created"
        self.color = random.choice(["Red", "Blue", "Green", "Black", "White"])
        self.engine_type = random.choice(["Electric", "Hybrid", "Internal Combustion"])
        self.components = {}
        self.production_history = []
        self.quality_history = []
        self.maintenance_needed = False
        self.additional_features = {
            "autonomous_driving": random.choice([True, False]),
            "infotainment": "Basic",
            "safety_rating": None,
        }
    
    def update_status(self, new_status):
        logger.debug("Vehicle %d: Status updated from %s to %s", self.id, self.status, new_status)
        self.status = new_status
        self.production_history.append((f"Status updated to {new_status}", time.time()))
    
    def add_production_step(self, step_name, description=""):
        timestamp = time.time()
        logger.debug("Vehicle %d: Production step added: %s at %s. %s", self.id, step_name, timestamp, description)
        self.production_history.append((step_name, timestamp, description))
    
    def add_quality_check(self, check_details):
        timestamp = time.time()
        logger.debug("Vehicle %d: Quality check added: %s at %s", self.id, check_details, timestamp)
        self.quality_history.append((check_details, timestamp))
    
    def add_component(self, component_name, component_details):
        logger.debug("Vehicle %d: Adding component %s with details %s", self.id, component_name, component_details)
        self.components[component_name] = component_details
        self.add_production_step(f"Installed {component_name}", "Component installation completed.")
    
    def mark_for_maintenance(self):
        self.maintenance_needed = True
        self.add_production_step("Marked for maintenance", "Quality issues detected.")
        logger.info("Vehicle %d marked for maintenance.", self.id)
    
    def get_summary(self):
        summary = {
            "id": self.id,
            "creation_time": self.creation_time,
            "status": self.status,
            "color": self.color,
            "engine_type": self.engine_type,
            "components": self.components,
            "production_history": self.production_history,
            "quality_history": self.quality_history,
            "maintenance_needed": self.maintenance_needed,
            "additional_features": self.additional_features,
        }
        return summary

###############################################################################
# ProductionLine Class
###############################################################################

class ProductionLine:
    """
    Simulates the production process for vehicles with detailed production steps.
    
    The production process includes multiple steps such as welding, assembly, painting,
    inspection, testing, and packaging.
    """
    def __init__(self, env):
        self.env = env
        self.vehicle_count = 0
        self.vehicles = []
        self.logger = logger
        self.production_rate = 1  # Vehicles per cycle
    
    def produce_vehicle(self):
        """
        Create a new vehicle and log its production.
        """
        self.vehicle_count += 1
        vehicle = Vehicle(vehicle_id=self.vehicle_count, creation_time=self.env.now)
        self.vehicles.append(vehicle)
        self.logger.info("Vehicle %d produced at simulation time %.2f", vehicle.id, self.env.now)
        vehicle.add_production_step("Vehicle Produced", "Vehicle creation completed.")
        return vehicle
    
    def process_vehicle(self, vehicle):
        """
        Process a vehicle through detailed production steps.
        """
        # Welding step
        yield self.env.process(self.welding_station(vehicle))
        # Assembly step
        yield self.env.process(self.assembly_station(vehicle))
        # Painting step
        yield self.env.process(self.painting_station(vehicle))
        # Inspection step
        yield self.env.process(self.inspection_station(vehicle))
        # Testing step
        yield self.env.process(self.testing_station(vehicle))
        # Packaging step
        yield self.env.process(self.packaging_station(vehicle))
        vehicle.update_status("completed")
        self.logger.info("Vehicle %d completed production at simulation time %.2f", vehicle.id, self.env.now)
    
    def welding_station(self, vehicle):
        """
        Simulate the welding station.
        """
        vehicle.update_status("welding")
        vehicle.add_production_step("Welding started")
        duration = random.uniform(1.0, 3.0)
        self.logger.debug("Vehicle %d: Welding duration %.2f", vehicle.id, duration)
        yield self.env.timeout(duration)
        vehicle.add_production_step("Welding completed")
        self.logger.debug("Vehicle %d: Welding completed", vehicle.id)
    
    def assembly_station(self, vehicle):
        """
        Simulate the assembly station.
        """
        vehicle.update_status("assembly")
        vehicle.add_production_step("Assembly started")
        duration = random.uniform(2.0, 5.0)
        self.logger.debug("Vehicle %d: Assembly duration %.2f", vehicle.id, duration)
        yield self.env.timeout(duration)
        # Install critical components.
        vehicle.add_component("Chassis", {"material": "Aluminum", "quality": random.choice(["A", "B", "C"])})
        vehicle.add_component("Engine", {"type": vehicle.engine_type, "horsepower": random.randint(150, 400)})
        vehicle.add_production_step("Assembly completed", "Chassis and Engine installed.")
        self.logger.debug("Vehicle %d: Assembly completed", vehicle.id)
    
    def painting_station(self, vehicle):
        """
        Simulate the painting station.
        """
        vehicle.update_status("painting")
        vehicle.add_production_step("Painting started")
        duration = random.uniform(1.0, 3.0)
        self.logger.debug("Vehicle %d: Painting duration %.2f", vehicle.id, duration)
        yield self.env.timeout(duration)
        # Apply a new color.
        vehicle.color = random.choice(["Red", "Blue", "Green", "Black", "White", "Silver"])
        vehicle.add_production_step("Painting completed", f"Color applied: {vehicle.color}")
        self.logger.debug("Vehicle %d: Painting completed", vehicle.id)
    
    def inspection_station(self, vehicle):
        """
        Simulate the inspection station.
        """
        vehicle.update_status("inspection")
        vehicle.add_production_step("Inspection started")
        duration = random.uniform(1.0, 2.5)
        self.logger.debug("Vehicle %d: Inspection duration %.2f", vehicle.id, duration)
        yield self.env.timeout(duration)
        vehicle.add_production_step("Inspection completed")
        self.logger.debug("Vehicle %d: Inspection completed", vehicle.id)
    
    def testing_station(self, vehicle):
        """
        Simulate the testing station.
        """
        vehicle.update_status("testing")
        vehicle.add_production_step("Testing started")
        duration = random.uniform(2.0, 4.0)
        self.logger.debug("Vehicle %d: Testing duration %.2f", vehicle.id, duration)
        yield self.env.timeout(duration)
        # Simulate performance test.
        performance = random.uniform(0, 1)
        if performance < 0.5:
            vehicle.add_production_step("Testing failed", "Performance below threshold")
            vehicle.mark_for_maintenance()
        else:
            vehicle.add_production_step("Testing passed", "Performance meets standard")
        self.logger.debug("Vehicle %d: Testing completed", vehicle.id)
    
    def packaging_station(self, vehicle):
        """
        Simulate the packaging station.
        """
        vehicle.update_status("packaging")
        vehicle.add_production_step("Packaging started")
        duration = random.uniform(0.5, 1.5)
        self.logger.debug("Vehicle %d: Packaging duration %.2f", vehicle.id, duration)
        yield self.env.timeout(duration)
        vehicle.add_production_step("Packaging completed", "Vehicle ready for delivery")
        self.logger.debug("Vehicle %d: Packaging completed", vehicle.id)
    
    def get_produced_count(self):
        """
        Return the total number of vehicles produced.
        """
        return len(self.vehicles)
    
    def get_vehicle_by_id(self, vehicle_id):
        """
        Retrieve a vehicle by its unique identifier.
        """
        for v in self.vehicles:
            if v.id == vehicle_id:
                return v
        return None

###############################################################################
# QualityCheck Class
###############################################################################

class QualityCheck:
    """
    Performs advanced quality checks on vehicles using multiple criteria.
    
    The quality check logic assesses several parameters including assembly accuracy,
    paint quality, and overall performance. It returns a detailed result.
    """
    def __init__(self, env, production_line):
        self.env = env
        self.production_line = production_line
        self.logger = logger
        # Define thresholds for quality assessment.
        self.assembly_threshold = 0.75
        self.paint_threshold = 0.80
        self.performance_threshold = 0.70
    
    def run_quality_checks(self):
        """
        Periodically run quality checks on random vehicles from the production line.
        """
        while True:
            yield self.env.timeout(20)
            if self.production_line.vehicles:
                vehicle = random.choice(self.production_line.vehicles)
                self.logger.info("Performing advanced quality check on vehicle %d at simulation time %.2f",
                                 vehicle.id, self.env.now)
                yield self.env.process(self.perform_quality_check(vehicle))
    
    def perform_quality_check(self, vehicle):
        """
        Perform an advanced quality check on a single vehicle.
        The process includes multiple sub-checks and computes an overall quality score.
        """
        self.logger.debug("Vehicle %d: Starting advanced quality check.", vehicle.id)
        yield self.env.timeout(random.uniform(0.5, 1.5))
        assembly_score = random.uniform(0, 1)
        paint_score = random.uniform(0, 1)
        performance_score = random.uniform(0, 1)
        overall_score = (assembly_score + paint_score + performance_score) / 3.0
        result = "passed" if (assembly_score >= self.assembly_threshold and 
                              paint_score >= self.paint_threshold and 
                              performance_score >= self.performance_threshold) else "failed"
        detailed_result = {
            "assembly_score": round(assembly_score, 2),
            "paint_score": round(paint_score, 2),
            "performance_score": round(performance_score, 2),
            "overall_score": round(overall_score, 2),
            "result": result
        }
        vehicle.add_quality_check(detailed_result)
        self.logger.info("Vehicle %d quality check result: %s", vehicle.id, detailed_result)

###############################################################################
# Component Class
###############################################################################

class Component:
    """
    Represents a component of a vehicle with detailed properties.
    
    Each component includes name, type, production time, quality grade, and additional specifications.
    """
    def __init__(self, name, component_type, production_time, quality_grade=None, specifications=None):
        self.name = name
        self.component_type = component_type
        self.production_time = production_time
        self.quality_grade = quality_grade if quality_grade else random.choice(["A", "B", "C"])
        self.specifications = specifications if specifications is not None else {}
        self.creation_time = time.time()
        logger.debug("Component %s created: type=%s, production_time=%.2f, quality=%s",
                     self.name, self.component_type, self.production_time, self.quality_grade)
    
    def update_specification(self, key, value):
        logger.debug("Component %s: Updating specification %s to %s", self.name, key, value)
        self.specifications[key] = value
    
    def get_details(self):
        return {
            "name": self.name,
            "type": self.component_type,
            "production_time": self.production_time,
            "quality_grade": self.quality_grade,
            "specifications": self.specifications,
            "creation_time": self.creation_time
        }

###############################################################################
# AssemblyStation Class
###############################################################################

class AssemblyStation:
    """
    Simulates an assembly station that installs components onto a vehicle.
    
    This station processes vehicles and installs various components like interior electronics.
    """
    def __init__(self, env, name, process_time_range=(1, 3)):
        self.env = env
        self.name = name
        self.process_time_range = process_time_range
        self.logger = logger
        self.processed_components = 0
    
    def assemble(self, vehicle, component_name):
        self.logger.info("Vehicle %d: Assembly at station %s for component %s started.", vehicle.id, self.name, component_name)
        duration = random.uniform(*self.process_time_range)
        yield self.env.timeout(duration)
        component = Component(component_name, "assembly", duration)
        vehicle.add_component(component_name, component.get_details())
        self.processed_components += 1
        self.logger.info("Vehicle %d: Assembly at station %s for component %s completed.", vehicle.id, self.name, component_name)
    
    def get_processed_count(self):
        return self.processed_components

###############################################################################
# PaintingStation Class
###############################################################################

class PaintingStation:
    """
    Simulates a painting station for vehicles.
    
    This station applies a coat of paint and ensures even color distribution.
    """
    def __init__(self, env, name, process_time_range=(1, 3)):
        self.env = env
        self.name = name
        self.process_time_range = process_time_range
        self.logger = logger
        self.processed_vehicles = 0
    
    def paint(self, vehicle):
        self.logger.info("Vehicle %d: Painting at station %s started.", vehicle.id, self.name)
        duration = random.uniform(*self.process_time_range)
        yield self.env.timeout(duration)
        new_color = random.choice(["Red", "Blue", "Green", "Black", "White", "Silver"])
        vehicle.color = new_color
        vehicle.add_production_step("Painted", f"Color applied: {new_color}")
        self.processed_vehicles += 1
        self.logger.info("Vehicle %d: Painting at station %s completed.", vehicle.id, self.name)
    
    def get_processed_count(self):
        return self.processed_vehicles

###############################################################################
# Standalone Test Routine
###############################################################################

if __name__ == "__main__":
    # Standalone test for the models module.
    test_env = simpy.Environment()
    prod_line = ProductionLine(test_env)
    quality_checker = QualityCheck(test_env, prod_line)
    assembly_station = AssemblyStation(test_env, "Assembly A")
    painting_station = PaintingStation(test_env, "Painting A")
    
    # Start production process.
    test_env.process(prod_line.produce_vehicle())
    test_env.process(quality_checker.run_quality_checks())
    
    def test_station():
        vehicle = prod_line.produce_vehicle()
        yield test_env.process(assembly_station.assemble(vehicle, "Infotainment System"))
        yield test_env.process(painting_station.paint(vehicle))
        yield test_env.process(prod_line.process_vehicle(vehicle))
        logger.info("Test vehicle summary: %s", vehicle.get_summary())
    
    test_env.process(test_station())
    test_env.run(until=50)
