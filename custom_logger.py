import logging

def get_logger(name=__name__):
    '''
    get a custom logger
    :param name: name of the file
    :return: logger instance
    '''
    # Create log handler
    logHandler = logging.StreamHandler()

    # Set handler format
    logFormat = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%d-%b-%y")
    logHandler.setFormatter(logFormat)
    # Create logger
    logger = logging.getLogger(name)
    # Add handler to logger
    logger.addHandler(logHandler)
    logger.setLevel(logging.DEBUG)

    return logger
