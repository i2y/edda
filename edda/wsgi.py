"""
WSGI adapter for Edda framework.

This module provides a WSGI adapter that wraps EddaApp (ASGI) for use with
WSGI servers like gunicorn or uWSGI.

The adapter uses a2wsgi to convert the ASGI interface to WSGI.
"""

from typing import Any

from a2wsgi import ASGIMiddleware

from edda.app import EddaApp


def create_wsgi_app(edda_app: EddaApp) -> Any:
    """
    Create a WSGI-compatible application from an EddaApp instance.

    This function wraps an EddaApp (ASGI) with a2wsgi's ASGIMiddleware,
    making it compatible with WSGI servers like gunicorn or uWSGI.

    Args:
        edda_app: An initialized EddaApp instance

    Returns:
        A WSGI-compatible application callable

    Example:
        Basic usage with EddaApp::

            from edda import EddaApp
            from edda.wsgi import create_wsgi_app
            from edda.storage.sqlalchemy_storage import SQLAlchemyStorage

            # Create storage and EddaApp
            storage = SQLAlchemyStorage("sqlite:///edda.db")
            app = EddaApp(storage=storage)

            # Create WSGI application
            wsgi_app = create_wsgi_app(app)

        Running with gunicorn::

            # In your module (e.g., demo_app.py):
            from edda import EddaApp
            from edda.wsgi import create_wsgi_app

            application = EddaApp(...)  # ASGI
            wsgi_application = create_wsgi_app(application)  # WSGI

            # Command line:
            $ gunicorn demo_app:wsgi_application --workers 4

        Running with uWSGI::

            $ uwsgi --http :8000 --wsgi-file demo_app.py --callable wsgi_application

    Background tasks (auto-resume, timer checks, etc.) will run in each
    worker process.

    For production deployments, ASGI servers (uvicorn, hypercorn) are
    recommended for better performance with Edda's async architecture.
    WSGI support is provided for compatibility with existing infrastructure
    and for users who prefer synchronous programming with sync activities.

    See Also:
        - :class:`edda.app.EddaApp`: The main ASGI application class
        - :func:`edda.activity.activity`: Decorator supporting sync activities
    """
    # Type ignore due to a2wsgi's strict ASGI type checking
    # EddaApp implements ASGI 3.0 interface correctly
    return ASGIMiddleware(edda_app)  # type: ignore[arg-type]


__all__ = ["create_wsgi_app"]
