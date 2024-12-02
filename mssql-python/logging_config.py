import logging
import os

def setup_logging(log_level=logging.INFO):
    """
    Set up logging configuration.

    This method configures the logging settings for the application.
    It sets the log level, format, and log file location.

    Args:
        log_level (int): The logging level (default: logging.INFO).
    """
    # Get the directory of the current script
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Construct the path to the log file
    log_file = os.path.join(current_dir, 'application.log')
    
    # Configure logging
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(filename)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )