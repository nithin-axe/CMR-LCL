import logging
import os

# Initialize basic logger
def init_logger(app):
    log_dir = os.path.join(app.root_path, "..", "data", "logs")
    os.makedirs(log_dir, exist_ok=True)
    
    log_file = os.path.join(log_dir, "app.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s in %(module)s: %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    app.logger.setLevel(logging.INFO)
    app.logger.info("Logging initialized.")
