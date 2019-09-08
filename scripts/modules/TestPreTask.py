
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

    deadlinePlugin.LogInfo("In Test Pre Task!")

    # Get HFS
    hfs_env = job.GetJobEnvironmentKeyValue('HFS')
    deadlinePlugin.LogInfo("got hfs env")
    if not hfs_env:
        deadlinePlugin.LogWarning('$HFS not found in job environment.')
        return 0

    # Evaluate it locallly to this machine
    hfs_env = RepositoryUtils.CheckPathMapping(hfs_env)
    deadlinePlugin.LogInfo("checked path mapping")
    # Append Houdini bin and python2.7libs folders
    append_hlibs_to_sys(hfs_env)
    deadlinePlugin.LogInfo("appended libs")

    # # Import PDG module (don't think there is a use for this but this should work)
    # try:
    #     import pdg
    # except:
    #     deadlinePlugin.LogWarning('Unable to import pdg\n\t {}'.format(traceback.format_exc(1)))

    # deadlinePlugin.LogInfo("PDG has been loaded: {}".format(pdg.__file__))

    # The task index (corresponds to the task file)
    startFrame = deadlinePlugin.GetStartFrame()
    deadlinePlugin.LogInfo("got start frame")

    startupDir = deadlinePlugin.GetStartupDirectory()

    # The PDG job directory will contain the task file
    jobDir = deadlinePlugin.GetPluginInfoEntryWithDefault('PDGJobDirectory', '')
    deadlinePlugin.LogInfo("got job dir")
    if not jobDir:
        deadlinePlugin.FailRender('PDGJobDirectory is not specified. Unable to get task file.')

    taskFilePath = os.path.join(jobDir, 'task_{}.txt'.format(startFrame))
    

    deadlinePlugin.LogInfo('Looking for task file: {}'.format(taskFilePath))

    # Wait until task file has been synchronized. The file is written by the submission machine and this waits until its available
    # in the mounted directory.
    # This file contains all the data to execute the work item.
    line = deadlinePlugin.WaitForCommandFile(taskFilePath, False, deadlinePlugin.taskFileTimeout)
    if not line:
        deadlinePlugin.FailRender('Task file not found at {}'.format(taskFilePath))

    executable = None
    arguments = ''

    try:
        # Load the task file's data as json dict and process properties

        deadlinePlugin.LogInfo('get json data')
        json_obj = json.loads(line)

        
        deadlinePlugin.LogInfo("Read json file: {}".format( str(taskFilePath) ) )

        executable = RepositoryUtils.CheckPathMapping(json_obj['executable'].replace( "\"", "" ))
        arguments = RepositoryUtils.CheckPathMapping(json_obj['arguments'])

        deadlinePlugin.LogInfo('Task Executable: %s' % executable)
        deadlinePlugin.LogInfo('Task Arguments: %s' % arguments)

        with open(taskFilePath, 'w') as outfile:
            json.dump(json_obj, outfile)

        deadlinePlugin.LogInfo('dump json data to file: {}'.format(taskFilePath))
        
    except:
        deadlinePlugin.FailRender('Unable to parse task file as json\n\t {}'.format(traceback.format_exc(1)))



    # You can update the hip file in the argumens, then write it back into the task file at taskFilePath
    # Then the PDGDeadline plugin will again load this file and execute the task.

    deadlinePlugin.LogInfo("Finished Test Pre Task!")









