"""
Server module for the NIO Digital Twin Web Server.

This module creates and configures the FastAPI application and provides
a set of API endpoints for interacting with the simulation system. These
endpoints include:
  - A root endpoint that returns a basic HTML dashboard.
  - Endpoints to fetch the latest data, full history, and system status.
  - Endpoints to generate and retrieve visualizations.
  - Endpoints to get and update simulation configuration.
  
It also sets up a global exception handler and starts the server using uvicorn.
"""

import time
import json
import logging
from typing import Optional

import uvicorn
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from pydantic import BaseModel

# Import simulation modules.
from simulation.data_ingestion import DataIngestion
from simulation.visualization import Visualization

# Set up a module-level logger.
logger = logging.getLogger("web_server.server")
logger.setLevel(logging.DEBUG)
if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] (%(name)s): %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

# Create the FastAPI app instance.
app = FastAPI(
    title="NIO Digital Twin Web Server",
    description="API server for the NIO Digital Twin system.",
    version="1.0.0"
)

# Initialize the data ingestion system.
data_ingestion = DataIngestion(flush_interval=10, output_file="ingested_data.csv")
data_ingestion.start()

# Define a Pydantic model for simulation configuration updates.
class SimulationConfig(BaseModel):
    simulation_duration: Optional[int] = 1000
    production_rate: Optional[float] = 1.0

# Global simulation configuration instance.
simulation_config = SimulationConfig()


@app.get("/", response_class=HTMLResponse)
async def root():
    """
    Root endpoint that returns a basic HTML dashboard.
    """
    html_content = """
    <html>
        <head>
            <title>NIO Digital Twin Dashboard</title>
        </head>
        <body>
            <h1>Welcome to the NIO Digital Twin Dashboard</h1>
            <p>Use the provided API endpoints to interact with the simulation system.</p>
            <ul>
                <li><a href="/data">Latest Data</a></li>
                <li><a href="/status">System Status</a></li>
                <li><a href="/visualize">Generate Visualization</a></li>
                <li><a href="/visualize/image">View Visualization Image</a></li>
                <li><a href="/config">View Simulation Configuration</a></li>
                <li><a href="/history">Full Data History</a></li>
            </ul>
        </body>
    </html>
    """
    logger.info("Root endpoint accessed; returning dashboard HTML.")
    return HTMLResponse(content=html_content, status_code=200)


@app.get("/data", response_class=JSONResponse)
async def get_latest_data(records: Optional[int] = 100):
    """
    Retrieve the latest ingested data records.

    Query Parameters:
        records (int): The number of recent records to retrieve.

    Returns:
        JSON: A list of the latest data records.
    """
    try:
        latest_data = data_ingestion.get_latest_data(num_records=records)
        logger.info("Returning %d latest data records.", len(latest_data))
        data_json = json.loads(latest_data.to_json(orient="records"))
        return JSONResponse(content=data_json)
    except Exception as e:
        logger.error("Error retrieving latest data: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving data."
        )


@app.get("/status", response_class=JSONResponse)
async def get_status():
    """
    Retrieve the current status of the simulation system.

    Returns:
        JSON: Status information including data ingestion status, number of records,
              simulation configuration, and current timestamp.
    """
    try:
        status_info = {
            "ingestion_running": data_ingestion.running,
            "ingested_records": len(data_ingestion.get_all_data()),
            "simulation_config": simulation_config.dict(),
            "timestamp": time.time()
        }
        logger.info("Status requested: %s", status_info)
        return JSONResponse(content=status_info)
    except Exception as e:
        logger.error("Error retrieving system status: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving system status."
        )


@app.get("/visualize", response_class=JSONResponse)
async def generate_visualization():
    """
    Trigger the generation of a visualization based on ingested data.

    This endpoint generates a production count plot and saves it as an image.
    
    Returns:
        JSON: A message confirming the generation and the filename of the image.
    """
    try:
        logger.info("Visualization endpoint called; generating production count plot.")
        # Use all ingested data for visualization.
        all_data = data_ingestion.get_all_data()
        viz = Visualization(data=all_data)
        fig_ax = viz.plot_production_count()
        if fig_ax is None:
            raise ValueError("Visualization could not be generated.")
        fig, _ = fig_ax
        image_file = "production_count.png"
        fig.savefig(image_file, dpi=150)
        message = f"Visualization generated and saved as {image_file}."
        logger.info(message)
        return JSONResponse(content={"message": message, "image_file": image_file})
    except Exception as e:
        logger.error("Error generating visualization: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error generating visualization."
        )


@app.get("/visualize/image", response_class=FileResponse)
async def get_visualization_image():
    """
    Serve the latest visualization image file.

    Returns:
        FileResponse: The image file if it exists.
    """
    image_file = "production_count.png"
    try:
        logger.info("Visualization image requested; serving file: %s", image_file)
        return FileResponse(image_file, media_type="image/png")
    except Exception as e:
        logger.error("Error retrieving visualization image: %s", e)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Visualization image not found."
        )


@app.get("/config", response_class=JSONResponse)
async def get_config():
    """
    Retrieve the current simulation configuration settings.

    Returns:
        JSON: The current simulation configuration.
    """
    try:
        config_data = simulation_config.dict()
        logger.info("Returning simulation configuration: %s", config_data)
        return JSONResponse(content=config_data)
    except Exception as e:
        logger.error("Error retrieving simulation configuration: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving configuration."
        )


@app.post("/config", response_class=JSONResponse)
async def update_config(new_config: SimulationConfig):
    """
    Update the simulation configuration settings.

    Request Body:
        new_config (SimulationConfig): The new configuration parameters.

    Returns:
        JSON: Confirmation of the updated configuration.
    """
    try:
        global simulation_config
        simulation_config = new_config
        logger.info("Simulation configuration updated to: %s", simulation_config.dict())
        return JSONResponse(
            content={
                "message": "Configuration updated successfully.",
                "new_config": simulation_config.dict()
            }
        )
    except Exception as e:
        logger.error("Error updating configuration: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error updating configuration."
        )


@app.get("/history", response_class=JSONResponse)
async def get_full_history():
    """
    Retrieve the full history of ingested data records.

    Returns:
        JSON: A list containing all ingested data records.
    """
    try:
        all_data = data_ingestion.get_all_data()
        logger.info("Returning full history with %d records.", len(all_data))
        data_json = json.loads(all_data.to_json(orient="records"))
        return JSONResponse(content=data_json)
    except Exception as e:
        logger.error("Error retrieving full data history: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving full data history."
        )


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """
    Global exception handler for any unhandled exceptions.

    Returns:
        JSONResponse: Error message and details.
    """
    logger.error("Unhandled exception: %s", exc)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"message": "Internal server error", "detail": str(exc)}
    )


def start_server():
    """
    Start the FastAPI server using uvicorn.
    """
    logger.info("Starting FastAPI server via uvicorn on host 0.0.0.0, port 8000...")
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    # When executed directly, start the web server.
    start_server()
