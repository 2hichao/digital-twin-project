"""
simulation package initializer.

This module sets up the simulation environment and imports core modules used
by the digital twin system. It provides a unified interface for simulation
components such as the simulation engine, production models, data ingestion,
and visualization utilities.

The purpose is to provide a single entry point for simulation-related functions.
"""

import logging

# Set up a package-level logger.
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Configure a console handler with a simple message format.
if not logger.handlers:
    ch = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] (%(name)s): %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    ch.setFormatter(formatter)
    logger.addHandler(ch)

# Import core modules of the simulation package.
try:
    from .engine import SimulationEngine
    from .models import Vehicle, ProductionLine
    from .data_ingestion import DataIngestion
    from .visualization import Visualization
    logger.info("Simulation package modules imported successfully.")
except Exception as error:
    logger.error("Error during importing simulation package modules: %s", error)

def initialize_simulation():
    """
    Initialize the simulation environment.

    This function sets up the simulation engine and returns a dictionary
    with the engine and production line objects.

    Returns:
        dict: A dictionary with keys 'engine' and 'production_line'.
    """
    try:
        engine = SimulationEngine()
        production_line = engine.production_line if hasattr(engine, "production_line") else None
        logger.info("Simulation environment initialized successfully.")
        return {"engine": engine, "production_line": production_line}
    except Exception as error:
        logger.error("Error during simulation initialization: %s", error)
        return {}

def reset_simulation_state():
    """
    Reset the simulation state if needed.

    This function can be extended to reset various parts of the simulation,
    such as clearing production data, restarting processes, or other tasks.
    """
    try:
        # Example placeholder for resetting simulation state.
        logger.info("Resetting simulation state.")
        # Additional reset actions would be added here.
    except Exception as error:
        logger.error("Error during simulation state reset: %s", error)

# Expose public API of the simulation package.
__all__ = [
    "SimulationEngine",
    "Vehicle",
    "ProductionLine",
    "DataIngestion",
    "Visualization",
    "initialize_simulation",
    "reset_simulation_state",
]

# Additional module-level initializations can be added below if required.
logger.debug("simulation package __init__.py execution completed.")
