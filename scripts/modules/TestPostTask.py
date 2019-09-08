import os
import sys
import traceback
import json

from Deadline.Scripting import *
from Deadline.Plugins import *

def __main__( *args ):
    deadlinePlugin = args[0]
    job = deadlinePlugin.GetJob()

    deadlinePlugin.LogInfo("In Test Post Task!")

    task = deadlinePlugin.GetCurrentTask()


    deadlinePlugin.LogInfo("Finished Test Post Task!")

