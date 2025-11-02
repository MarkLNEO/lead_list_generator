import logging
from contextlib import ContextDecorator


class RequestLogCapture(ContextDecorator):
    """Minimal stub for capturing logs per request during tests.

    In production this can push logs to Supabase; here we just ensure the
    context exists and does not interfere with pipeline execution.
    """

    def __init__(self, supabase_client=None, request_id=None):
        self.supabase = supabase_client
        self.request_id = request_id

    def __enter__(self):
        logging.debug("RequestLogCapture start for request %s", self.request_id)
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc:
            logging.debug("RequestLogCapture caught exception: %s", exc)
        logging.debug("RequestLogCapture end for request %s", self.request_id)
        # Do not suppress exceptions
        return False

