from waerlib.logging_config import setup_logging

if __name__ == '__main__':
    setup_logging()

    """
    Additionally, to set up and clean up the thread local request id:
    
    add to, for example, app.py or elsewhere where the other routes are.
    
    @app.before_request
    def start_request():
        request_id = request.headers.get(get_request_id_header_key())
    
        if request_id is None:
            request_id = str(uuid.uuid4())
            app.logger.warning(f"Received endpoint {request.path} without a request id. Created one: {request_id}")
    
        store_request_id(request_id)
    
    
    @app.after_request
    def end_request(response):
        # Clean up after the request is finished
        del_current_request_id()
        return response
    
    """

