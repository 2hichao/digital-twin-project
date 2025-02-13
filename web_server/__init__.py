"""
web_server package initializer for the NIO Digital Twin project.

This module sets up the web server package, defines default configuration options,
configures logging, and imports the core modules needed for the API endpoints.
It provides functions to create and start the FastAPI server.
"""

import os
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Set up a package-level logger.
logger = logging.getLogger("web_server")
logger.setLevel(logging.DEBUG)
if not logger.handlers:
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] (%(name)s): %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

# Default configuration for the web server.
DEFAULT_HOST = os.environ.get("WEB_SERVER_HOST", "0.0.0.0")
DEFAULT_PORT = int(os.environ.get("WEB_SERVER_PORT", "8000"))
DEFAULT_DEBUG = os.environ.get("WEB_SERVER_DEBUG", "False").lower() in ("true", "1")

def create_app() -> FastAPI:
    """
    Create and configure the FastAPI application for the web server.

    Returns:
        FastAPI: The configured FastAPI application.
    """
    app = FastAPI(
        title="NIO Digital Twin Web Server",
        debug=DEFAULT_DEBUG,
        description="This API server provides endpoints for the NIO Digital Twin system."
    )

    # Add middleware to allow cross-origin requests.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    logger.info("FastAPI app created with title: 'NIO Digital Twin Web Server'.")

    # Attempt to import and include routes from the routes module.
    try:
        from . import routes  # Assumes that a routes.py or routes package exists.
        app.include_router(routes.router)
        logger.info("Routes imported and included in the FastAPI app.")
    except Exception as e:
        logger.error("Failed to import routes: %s", e)

    return app

# Provide an alias for create_app.
get_app = create_app

def start_web_server(app: FastAPI = None, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT):
    """
    Start the web server using uvicorn.

    If no app is provided, a default app is created using create_app.

    Args:
        app (FastAPI, optional): A FastAPI application. If not provided, a default is created.
        host (str): The host address for the server.
        port (int): The port number for the server.
    """
    if app is None:
        app = create_app()

    try:
        import uvicorn
        logger.info("Starting uvicorn server at http://%s:%d", host, port)
        uvicorn.run(app, host=host, port=port)
    except Exception as e:
        logger.error("Error starting the uvicorn server: %s", e)

# Exported objects and functions.
__all__ = [
    "logger",
    "create_app",
    "get_app",
    "start_web_server",
    "DEFAULT_HOST",
    "DEFAULT_PORT",
    "DEFAULT_DEBUG",
]

# Extra initialization: log that the package was loaded.
logger.debug("web_server package __init__.py executed successfully.")
