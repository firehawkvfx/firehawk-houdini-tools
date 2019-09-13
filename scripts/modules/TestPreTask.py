# This deadline pre task script is used in the deadline scheduler in TOPS.  it allows overwriting of the hip path to be loaded to optimise for remote locations.


import os
import sys
import traceback
import json

from Deadline.Scripting import *
from Deadline.Plugins import *


def append_hlibs_to_sys(hfs):
    """
    Add the $HFS/bin and $HFS/houdini/python2.7libs directories to sys path
    and os.environ['PATH']
    """
    hfsbin = hfs + "/bin"
    hlibs = hfs + "/houdini/python2.7libs"

    hfsbin = hfsbin.encode('ascii', 'ignore')
    hlibs = hlibs.encode('ascii', 'ignore')

    # This is required to load the DLLs
    sys.path.append(hfsbin)
    sys.path.append(hlibs)
    print(sys.path)

    path_combine = ':'
    if sys.platform == 'win32':
        path_combine = ';'

    # This is required to load the pdg module
    os.environ['PATH'] = os.environ['PATH'] + path_combine + hfsbin + path_combine + hlibs
    


def __main__( *args ):
    deadlinePlugin = args[0]
    job = deadlinePlugin.GetJob()

    deadlinePlugin.LogInfo("In Pre Task")

    # Get HFS
    hfs_env = job.GetJobEnvironmentKeyValue('HFS')
    deadlinePlugin.LogInfo("got hfs env")
    if not hfs_env:
        deadlinePlugin.LogWarning('$HFS not found in job environment.')
        return 0

    # Evaluate it locallly to this machine
    hfs_env = RepositoryUtils.CheckPathMapping(hfs_env)

    # Append Houdini bin and python2.7libs folders
    append_hlibs_to_sys(hfs_env)


    # The task index (corresponds to the task file)
    startFrame = deadlinePlugin.GetStartFrame()
    # deadlinePlugin.LogInfo("got start frame")

    startupDir = deadlinePlugin.GetStartupDirectory()

    # The PDG job directory will contain the task file
    jobDir = deadlinePlugin.GetPluginInfoEntryWithDefault('PDGJobDirectory', '')
    if not jobDir:
        deadlinePlugin.FailRender('PDGJobDirectory is not specified. Unable to get task file.')

    taskFilePath = os.path.join(jobDir, 'task_{}.txt'.format(startFrame))
    

    deadlinePlugin.LogInfo('Looking for task file: {}'.format(taskFilePath))

    # Wait until task file has been synchronized. The file is written by the submission machine and this waits until its available
    # in the mounted directory.
    # This file contains all the data to execute the work item.
    line = deadlinePlugin.WaitForCommandFile(taskFilePath, False, deadlinePlugin.taskFileTimeout)

    deadlinePlugin.LogInfo('Found task file: {}'.format(taskFilePath))

    if not line:
        deadlinePlugin.FailRender('Task file not found at {}'.format(taskFilePath))

    executable = None
    arguments = ''

    try:
        # Load the task file's data as json dict and process properties

        json_obj = json.loads(line)
        deadlinePlugin.LogInfo('loaded JSON OBJ')
        
        executable = RepositoryUtils.CheckPathMapping(json_obj['executable'].replace( "\"", "" ))
        arguments = RepositoryUtils.CheckPathMapping(json_obj['arguments'])

        # ### Alter work item command path ###
        command = arguments

        if '.hip' in command.lower():
            prod_root = os.environ["PROD_ROOT"]
            prod_onsite_root = os.environ["PROD_ONSITE_ROOT"]
            prod_cloud_root = os.environ["PROD_CLOUD_ROOT"]

            def convert_path(item, target_base_path):
                result = None
                
                if item.startswith(prod_root):
                    result = "{base}"+item[len(prod_root):]
                elif item.startswith(prod_onsite_root):
                    result = "{base}"+item[len(prod_onsite_root):]
                elif item.startswith(prod_cloud_root):
                    result = "{base}"+item[len(prod_cloud_root):]
                else:
                    deadlinePlugin.LogInfo('no path match to convert path')
                result = str(target_base_path+result[len("{base}"):])
                return result

            command_list = command.split(' ')
            new_command_list = []
            for item in command_list:
                if '.hip' in item.lower():
                    item = convert_path(item, prod_root)
                new_command_list.append(item)
            
            command = ' '.join(new_command_list)
            deadlinePlugin.LogInfo('command updated:')
            json_obj['arguments'] = command

            arguments = RepositoryUtils.CheckPathMapping(json_obj['arguments'])

            with open(taskFilePath, 'w') as outfile:
                json.dump(json_obj, outfile)

            deadlinePlugin.LogInfo('dump json data to file: {}'.format(taskFilePath))

        deadlinePlugin.LogInfo('Task Executable: %s' % executable)
        deadlinePlugin.LogInfo('Task Arguments: %s' % arguments)


        
    except:
        deadlinePlugin.FailRender('Unable to parse task file as json\n\t {}'.format(traceback.format_exc(1)))



    # You can update the hip file in the argumens, then write it back into the task file at taskFilePath
    # Then the PDGDeadline plugin will again load this file and execute the task.

    deadlinePlugin.LogInfo("Finished Test Pre Task!")









