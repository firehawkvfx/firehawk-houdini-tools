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

hfs_env=os.environ['HFS']
print 'hfs_env:', hfs_env
houpythonlib = os.path.join(hfs_env, 'houdini/python2.7libs')
print 'houpythonlib:', houpythonlib
sys.path.append(houpythonlib)

# pythonpath = os.environ['PYTHONPATH']
# print 'pythonpath', pythonpath

sys.path.append('/usr/lib64/python2.7/site-packages')
sys.path.append('/home/deadlineuser/.local/lib/python2.7/site-packages')
sys.path.append('/usr/lib/python2.7/site-packages')

# menu_path = os.path.join(os.environ['FIREHAWK_HOUDINI_TOOLS'], 'scripts/s3_sync')
# print 'menu_path', menu_path
# sys.path.append(menu_path)
# module_path = os.path.join(os.environ['FIREHAWK_HOUDINI_TOOLS'], 'scripts/modules')
# print 'module_path', module_path
# sys.path.append(module_path)

import pdgcmd

def __main__(plugin, task_type):
    print 'main post task'
    # if hfs_env:
    #     # Append $PYTHONPATH if not set
    #     houdini_python_libs = houpythonlib
    #     python_path = GetProcessEnvironmentVariable('PYTHONPATH')
    #     if python_path:
    #         if not houdini_python_libs in python_path:
    #             python_path.append(path_combine + houdini_python_libs)
    #     else:
    #         python_path = houdini_python_libs

    #     LogInfo('Setting PYTHONPATH: {}'.format(python_path))
    #     SetProcessEnvironmentVariable('PYTHONPATH', python_path)
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
    # print 'get work item'
    # work_item = WorkItem(getWorkItemJsonPath(itemname))

    # print 'dir work_item', work_item