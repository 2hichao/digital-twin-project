"""
web_server package initializer for the NIO Digital Twin project.

This module sets up the web server package by defining global configuration,
multiple middleware for security, logging, and rate limiting, and creates the
FastAPI application. It reads configuration from environment variables and sets
up default values. Additional middleware layers such as CORS, security headers,
custom request logging, and a dummy rate limiter are integrated.
"""

import os
import logging
import uuid
import time
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

# Read configuration from environment variables.
DEFAULT_HOST = os.environ.get("WEB_SERVER_HOST", "0.0.0.0")
DEFAULT_PORT = int(os.environ.get("WEB_SERVER_PORT", "8000"))
DEFAULT_DEBUG = os.environ.get("WEB_SERVER_DEBUG", "False").lower() in ("true", "1")
ALLOWED_ORIGINS = os.environ.get("CORS_ALLOWED_ORIGINS", "*").split(",")

# Set up a package-level logger.
logger = logging.getLogger("web_server")
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

logger.info("Initializing web_server package with host=%s, port=%d, debug=%s", DEFAULT_HOST, DEFAULT_PORT, DEFAULT_DEBUG)
logger.info("CORS allowed origins: %s", ALLOWED_ORIGINS)

###############################################################################
# Custom Middleware Implementations
###############################################################################

class RequestIDMiddleware(BaseHTTPMiddleware):
    """
    Middleware to assign a unique request ID to each incoming request.
    """
    async def dispatch(self, request: Request, call_next):
        request_id = str(uuid.uuid4())
        request.state.request_id = request_id
        response: Response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        logger.debug("Assigned Request ID %s to %s", request_id, request.url.path)
        return response

class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """
    Middleware to add security headers to the response.
    """
    async def dispatch(self, request: Request, call_next):
        response: Response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        logger.debug("Security headers added to response for %s", request.url.path)
        return response

class LoggingMiddleware(BaseHTTPMiddleware):
    """
    Middleware to log incoming requests and outgoing responses.
    """
    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        logger.info("Incoming request: %s %s", request.method, request.url.path)
        response: Response = await call_next(request)
        process_time = time.time() - start_time
        logger.info("Request %s completed in %.4f seconds with status %d", request.url.path, process_time, response.status_code)
        response.headers["X-Process-Time"] = str(process_time)
        return response

class DummyRateLimitMiddleware(BaseHTTPMiddleware):
    """
    Dummy middleware to simulate rate limiting.
    In production, integrate with a real rate limiting system.
    """
    RATE_LIMIT = 100  # Dummy limit: 100 requests per minute.

    async def dispatch(self, request: Request, call_next):
        # For demonstration purposes, we log each request and simulate a check.
        # In real-world usage, you would implement logic to track request counts per IP/user.
        client_ip = request.client.host
        logger.debug("Rate limiting check for client IP: %s", client_ip)
        # Dummy condition: if request header "X-Dummy-RateLimit" is set to "exceed", simulate limit reached.
        if request.headers.get("X-Dummy-RateLimit") == "exceed":
            logger.warning("Rate limit exceeded for client IP: %s", client_ip)
            return JSONResponse(status_code=429, content={"detail": "Rate limit exceeded"})
        return await call_next(request)

###############################################################################
# Application Factory Function
###############################################################################

def create_app() -> FastAPI:
    """
    Create and configure the FastAPI application for the web server.

    Returns:
        FastAPI: The fully configured FastAPI application with middleware and routers.
    """
    app = FastAPI(
        title="NIO Digital Twin Web Server",
        debug=DEFAULT_DEBUG,
        description="API server for the NIO Digital Twin system with enhanced middleware and configuration."
    )

    # Add CORS middleware.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=ALLOWED_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    logger.info("CORS middleware added.")

    # Add GZip middleware to compress responses.
    app.add_middleware(GZipMiddleware, minimum_size=1000)
    logger.info("GZip middleware added.")

    # Add custom middleware layers.
    app.add_middleware(RequestIDMiddleware)
    logger.info("RequestID middleware added.")
    
    app.add_middleware(SecurityHeadersMiddleware)
    logger.info("Security headers middleware added.")
    
    app.add_middleware(LoggingMiddleware)
    logger.info("Logging middleware added.")
    
    app.add_middleware(DummyRateLimitMiddleware)
    logger.info("Dummy rate limiting middleware added.")

    # Additional configuration can be loaded here.
    app.state.config = {
        "host": DEFAULT_HOST,
        "port": DEFAULT_PORT,
        "debug": DEFAULT_DEBUG,
        "allowed_origins": ALLOWED_ORIGINS,
    }
    logger.info("Application state configuration set: %s", app.state.config)

    # Import and include routers (if any).
    try:
        from . import routes  # Ensure a routes.py exists within the web_server package.
        if hasattr(routes, "router"):
            app.include_router(routes.router)
            logger.info("Routes from 'routes' module have been included in the app.")
    except Exception as e:
        logger.error("Error including routes: %s", e)

    return app

# Expose a function to get the application instance.
get_app = create_app

###############################################################################
# Additional Helper Functions
###############################################################################

def load_configuration() -> dict:
    """
    Load additional configuration parameters from environment variables or config files.
    
    Returns:
        dict: A dictionary of configuration settings.
    """
    config = {
        "log_level": os.environ.get("WEB_SERVER_LOG_LEVEL", "DEBUG"),
        "max_connections": int(os.environ.get("WEB_SERVER_MAX_CONNECTIONS", "100")),
        "timeout": int(os.environ.get("WEB_SERVER_TIMEOUT", "30")),
    }
    logger.info("Additional configuration loaded: %s", config)
    return config

def setup_logging():
    """
    Set up advanced logging configuration for the web server.
    """
    log_level = os.environ.get("WEB_SERVER_LOG_LEVEL", "DEBUG").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.DEBUG),
        format="%(asctime)s [%(levelname)s] (%(name)s): %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    logger.info("Logging configured with level %s", log_level)

###############################################################################
# Initialization Execution
###############################################################################

# Initialize logging and load additional configuration upon package import.
setup_logging()
extra_config = load_configuration()
logger.debug("Extra configuration: %s", extra_config)

logger.debug("web_server package __init__.py executed successfully.")
