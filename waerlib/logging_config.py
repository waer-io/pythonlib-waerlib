import logging
import json

# Thread-local storage
import threading

"""
    See example/logging_config.py for example usage.
"""


local_storage = threading.local()
undefined_request_id = "N/A"


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
            'request_id': get_request_id()
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


def get_request_id():
    return getattr(local_storage, 'request_id', undefined_request_id)


def get_request_id_header_key():
    return "X-Request-ID"


def store_request_id(request_id):
    local_storage.request_id = request_id


def del_current_request_id():
    if hasattr(local_storage, 'request_id'):
        del local_storage.request_id
