print 'POST TASK SCRIPT'

# import logging

# formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

# def setup_logger(name, log_file, level=logging.INFO):
#     """Function setup as many loggers as you want"""

#     handler = logging.FileHandler(log_file)        
#     handler.setFormatter(formatter)

#     logger = logging.getLogger(name)
#     logger.setLevel(level)
#     logger.addHandler(handler)

#     return logger

def __main__(plugin, task_type):
    print 'main'
    print dir(plugin)
    print dir(task_type)
    current_task = plugin.GetCurrentTask()
    print 'plugin.GetCurrentTask', current_task
    print 'dir plugin.GetCurrentTask'
    for item in dir(current_task):
        if 'get' in str(item):
            try:
                to_eval = "current_task."+str(item)+"()"
                print 'to_eval', to_eval
                print item, eval(to_eval)
            except:
                print 'exception', item
        else:
            print item

    print 'plugin.GetCurrentTaskId', plugin.GetCurrentTaskId()
    print 'dir plugin.GetCurrentTaskId', dir(plugin.GetCurrentTaskId())
    print 'plugin.GetDataFilename', plugin.GetDataFilename()
    print 'dir plugin.GetDataFilename', dir(plugin.GetDataFilename())
    # first file logger
    #logger = setup_logger('first_logger', '/var/tmp/logfile_post_task.log')
    #logger.info('POST TASK SCRIPT')

# GetCurrentTask', 'GetCurrentTaskId', 'GetDataFilename