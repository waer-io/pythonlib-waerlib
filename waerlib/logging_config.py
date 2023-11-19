import logging
import json

# Thread-local storage
import threading
import uuid

"""
    See example/logging_config.py for example usage.
"""

local_storage = threading.local()
undefined_id = "N/A"


class StructuredFormatter(logging.Formatter):
    def format(self, record):
        severity = {
            'DEBUG': 'DEBUG',
            'INFO': 'INFO',
            'WARNING': 'WARNING',
            'ERROR': 'ERROR',
            'CRITICAL': 'CRITICAL'
        }.get(record.levelname, 'DEFAULT')

        structured_record = {
            'severity': severity,
            'level': record.levelname,
            'message': record.getMessage(),
            'pathname': record.pathname,
            'lineno': record.lineno,
            'request_id': get_request_id(),
            'user_id': get_user_id()
        }

        # Exception info, if there is any
        if record.exc_info:
            structured_record['exc_info'] = self.formatException(record.exc_info)
        return json.dumps(structured_record)


def setup_logging():
    log_level = logging.DEBUG

    stdout_handler = logging.StreamHandler()
    stdout_handler.setLevel(log_level)
    stdout_handler.setFormatter(StructuredFormatter())

    logging.basicConfig(level=log_level, handlers=[stdout_handler])


def setup_active_request(request, app):
    request_id = request.headers.get(get_request_id_header_key())

    if request_id is None:
        request_id = str(uuid.uuid4())
        app.logger.warning(f"Received endpoint {request.path} without a request id. Created one: {request_id}")

    app.logger.debug(f"Request: {request.scheme} {request.method} {request.full_path} [{request_id}]")

    store_request_id(request_id)


def clear_active_request(response, app):
    del_current_request_id()
    del_current_user_id()
    app.logger.debug(f"Response: {response.code} [{get_request_id()}]")


def get_request_id():
    return getattr(local_storage, 'request_id', undefined_id)


def get_user_id():
    return getattr(local_storage, 'user_id', undefined_id)


def get_request_id_header_key():
    return "X-Request-ID"


def get_user_id_key():
    return "user_id"


def store_request_id(request_id):
    local_storage.request_id = request_id


def store_user_id(user_id):
    local_storage.user_id = user_id


def del_current_request_id():
    if hasattr(local_storage, 'request_id'):
        del local_storage.request_id


def del_current_user_id():
    if hasattr(local_storage, 'user_id'):
        del local_storage.user_id
