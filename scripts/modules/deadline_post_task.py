print 'POST TASK SCRIPT'

import os
import sys
import json
import re

from System import DateTime, TimeSpan
from System.IO import *
from System.Text.RegularExpressions import *

from Deadline.Scripting import *
from Deadline.Plugins import *

from FranticX.Processes import *

hfs=os.environ['HFS']
print 'hfs:', hfs
houpythonlib = os.path.join(hfs, 'houdini/python2.7libs')
print 'houpythonlib:', houpythonlib
sys.path.append(houpythonlib)

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

# import pdg

def __main__(plugin, task_type):
    print 'main post task'
    # print dir(plugin)
    # print dir(task_type)
    # current_task = plugin.GetCurrentTask()
    # print 'plugin.GetCurrentTask', current_task
    # print 'dir plugin.GetCurrentTask'
    # for item in dir(current_task):
    #     if 'get' in str(item):
    #         try:
    #             to_eval = "current_task."+str(item)+"()"
    #             print 'to_eval', to_eval
    #             print item, eval(to_eval)
    #         except:
    #             print 'exception', item
    #     else:
    #         print item

    # print 'plugin.GetCurrentTaskId', plugin.GetCurrentTaskId()
    # print 'dir plugin.GetCurrentTaskId', dir(plugin.GetCurrentTaskId())
    # print 'plugin.GetDataFilename', plugin.GetDataFilename()
    # print 'dir plugin.GetDataFilename', dir(plugin.GetDataFilename())

    # first file logger
    #logger = setup_logger('first_logger', '/var/tmp/logfile_post_task.log')
    #logger.info('POST TASK SCRIPT')

    # clone upstream workitem data.  didn't work on the remote side

    # itemname = None
    # callbackserver = None

    # try:
    #     if not itemname:
    #         itemname = os.environ['PDG_ITEM_NAME']
    #         print 'itemname', itemname
    #     if not callbackserver:
    #         callbackserver = os.environ['PDG_RESULT_SERVER']
    # except KeyError as exception: 
    #     print "ERROR: {} must be in environment or specified via argument flag.".format(exception.message)
    #     exit(1)

    # work_item = WorkItem(getWorkItemJsonPath(itemname))

    # print 'dir work_item', work_item

# GetCurrentTask', 'GetCurrentTaskId', 'GetDataFilename