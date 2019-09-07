print 'PRE JOB SCRIPT'

import logging


formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

def setup_logger(name, log_file, level=logging.INFO):
    """Function setup as many loggers as you want"""

    handler = logging.FileHandler(log_file)        
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger

def __main__(one, two):
    print 'main pre job'
    print dir(one)
    print dir(two)
    # first file logger
    logger = setup_logger('first_logger', '/var/tmp/logfile_pre_job.log')
    logger.info('PRE JOB SCRIPT')

logger = setup_logger('first_logger', '/var/tmp/logfile_pre_job.log')
logger.info('PRE JOB SCRIPT OUT OF MAIN')