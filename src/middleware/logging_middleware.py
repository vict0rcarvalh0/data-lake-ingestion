import logging
from flask import request
import json_log_formatter

formatter = json_log_formatter.JSONFormatter()

json_handler = logging.FileHandler(filename='logs/app.log')
json_handler.setFormatter(formatter)

logger = logging.getLogger()
logger.addHandler(json_handler)
logger.setLevel(logging.INFO)

def setup_logging(app):
    @app.before_request
    def log_request_info():
        logger.info({
            'event': 'request',
            'method': request.method,
            'url': request.url,
            'headers': dict(request.headers),
            'body': request.get_data(as_text=True) if request.data else None
        })

    @app.after_request
    def log_response_info(response):
        logger.info({
            'event': 'response',
            'status': response.status,
            'headers': dict(response.headers)
        })
        return response
