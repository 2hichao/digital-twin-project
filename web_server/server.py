"""
Server module for the NIO Digital Twin Web Server.

This module creates and configures the FastAPI application and provides a variety
of API endpoints to interact with the digital twin simulation. The API endpoints include:
  - A root endpoint returning a basic HTML dashboard.
  - Endpoints for retrieving sensor data, vehicle details, maintenance logs, production reports, and simulation configuration.
  - Authentication endpoints (login) using token-based auth.
  - Endpoints for updating configuration.
  
Advanced error handling, logging, and authentication (via API keys) are incorporated.
"""

import time
import json
import logging
from typing import Optional, Dict, Any

import uvicorn
from fastapi import FastAPI, HTTPException, Request, status, Depends, Header
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.security import APIKeyHeader
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
    version="2.0.0"
)

# Set up a simple API key based authentication.
API_KEY = "secret-token-123"  # In production, use a secure mechanism.
API_KEY_NAME = "X-API-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

def get_api_key(api_key: str = Depends(api_key_header)):
    """
    Dependency that verifies the provided API key.
    """
    if api_key == API_KEY:
        return api_key
    else:
        logger.warning("Unauthorized access attempt with API key: %s", api_key)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API Key",
            headers={"WWW-Authenticate": "API Key"},
        )

# Global simulation configuration instance.
class SimulationConfig(BaseModel):
    simulation_duration: Optional[int] = 1000
    production_rate: Optional[float] = 1.0
    maintenance_interval: Optional[int] = 50
    supply_threshold: Optional[int] = 200

simulation_config = SimulationConfig()

# Initialize data ingestion system.
data_ingestion = DataIngestion(flush_interval=10, output_file="ingested_data.csv", db_file="ingestion_data.db")
data_ingestion.start()

# In-memory storage for simulation logs and vehicle details (for demonstration).
SIMULATION_VEHICLES: Dict[int, Dict[str, Any]] = {}
MAINTENANCE_LOGS: list = []

###############################################################################
# API Endpoints
###############################################################################

@app.get("/", response_class=HTMLResponse)
async def root():
    """
    Root endpoint returning a basic HTML dashboard.
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
                <li><a href="/vehicles">Vehicle Details</a></li>
                <li><a href="/maintenance">Maintenance Logs</a></li>
                <li><a href="/reports">Production Reports</a></li>
                <li><a href="/config">Simulation Configuration</a></li>
                <li><a href="/auth/login">Login</a> (simulate token retrieval)</li>
            </ul>
        </body>
    </html>
    """
    logger.info("Root endpoint accessed; returning dashboard HTML.")
    return HTMLResponse(content=html_content, status_code=200)

@app.get("/auth/login", response_class=JSONResponse)
async def login(username: str, password: str):
    """
    Simulated login endpoint. In a real system, validate credentials against a secure user store.
    Returns an API key token if successful.
    """
    logger.info("Login attempt for username: %s", username)
    # For demonstration, accept any username/password combination.
    if username and password:
        token = API_KEY  # In production, generate a secure token.
        logger.info("User %s successfully logged in.", username)
        return JSONResponse(content={"message": "Login successful", "token": token})
    else:
        logger.warning("Login failed for username: %s", username)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials"
        )

@app.get("/data", response_class=JSONResponse)
async def get_latest_data(records: Optional[int] = 100, api_key: str = Depends(get_api_key)):
    """
    Retrieve the latest ingested sensor data records.
    """
    try:
        latest_data = data_ingestion.get_latest_data(num_records=records)
        logger.info("Returning %d latest data records.", len(latest_data))
        data_json = json.loads(latest_data.to_json(orient="records"))
        return JSONResponse(content=data_json)
    except Exception as e:
        logger.error("Error retrieving latest data: %s", e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving latest data."
        )

@app.get("/vehicles", response_class=JSONResponse)
async def get_vehicle_details(api_key: str = Depends(get_api_key)):
    """
    Retrieve details of all vehicles from the simulation.
    This endpoint returns in-memory vehicle data for demonstration purposes.
    """
    try:
        # In a real system, retrieve vehicle data from a database or simulation module.
        logger.info("Retrieving details for %d vehicles.", len(SIMULATION_VEHICLES))
        return JSONResponse(content=SIMULATION_VEHICLES)
    except Exception as e:
        logger.error("Error retrieving vehicle details: %s", e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving vehicle details."
        )

@app.get("/maintenance", response_class=JSONResponse)
async def get_maintenance_logs(api_key: str = Depends(get_api_key)):
    """
    Retrieve maintenance logs from the simulation.
    """
    try:
        logger.info("Returning %d maintenance logs.", len(MAINTENANCE_LOGS))
        return JSONResponse(content={"maintenance_logs": MAINTENANCE_LOGS})
    except Exception as e:
        logger.error("Error retrieving maintenance logs: %s", e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving maintenance logs."
        )

@app.get("/reports", response_class=JSONResponse)
async def get_production_report(api_key: str = Depends(get_api_key)):
    """
    Generate and retrieve a production report summary.
    This report aggregates data such as the number of vehicles produced,
    average quality scores, and raw material usage.
    """
    try:
        # For demonstration, we aggregate data from the data ingestion module.
        all_data = data_ingestion.get_all_data()
        report = {
            "total_records": len(all_data),
            "event_summary": all_data.groupby("event").size().to_dict(),
            "average_value": round(all_data["value"].mean(), 2) if not all_data.empty else None,
            "timestamp": time.time()
        }
        logger.info("Production report generated: %s", report)
        return JSONResponse(content=report)
    except Exception as e:
        logger.error("Error generating production report: %s", e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error generating production report."
        )

@app.get("/config", response_class=JSONResponse)
async def get_config(api_key: str = Depends(get_api_key)):
    """
    Retrieve the current simulation configuration settings.
    """
    try:
        config_data = simulation_config.dict()
        logger.info("Returning simulation configuration: %s", config_data)
        return JSONResponse(content=config_data)
    except Exception as e:
        logger.error("Error retrieving simulation configuration: %s", e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving simulation configuration."
        )

@app.post("/config", response_class=JSONResponse)
async def update_config(new_config: SimulationConfig, api_key: str = Depends(get_api_key)):
    """
    Update the simulation configuration settings.
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
        logger.error("Error updating simulation configuration: %s", e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error updating configuration."
        )

###############################################################################
# Advanced Error Handling and Logging
###############################################################################

@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    """
    Middleware to log request details and add a processing time header.
    """
    start_time = time.time()
    try:
        response = await call_next(request)
    except Exception as e:
        logger.error("Unhandled exception in middleware: %s", e, exc_info=True)
        raise e
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    logger.info("Request %s processed in %.4f seconds", request.url.path, process_time)
    return response

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """
    Global exception handler for unhandled exceptions.
    """
    logger.error("Unhandled exception: %s", exc, exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"message": "Internal server error", "detail": str(exc)}
    )

###############################################################################
# Server Startup Function
###############################################################################

def start_server():
    """
    Start the FastAPI server using uvicorn.
    """
    logger.info("Starting FastAPI server via uvicorn on host 0.0.0.0, port 8000...")
    try:
        uvicorn.run(app, host="0.0.0.0", port=8000)
    except Exception as e:
        logger.error("Error starting uvicorn server: %s", e, exc_info=True)

###############################################################################
# Main
###############################################################################

if __name__ == "__main__":
    # When executed directly, start the web server.
    start_server()
