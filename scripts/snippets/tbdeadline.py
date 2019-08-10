#
# PROPRIETARY INFORMATION.  This software is proprietary to
# Side Effects Software Inc., and is not to be reproduced,
# transmitted, or disclosed in any way without written permission.
#
# Produced by:
#    Side Effects Software Inc
#    123 Front Street West, Suite 1401
#    Toronto, Ontario
#    Canada   M5J 2M2
#    416-504-9876
#
# NAME:         tbdeadline.py ( Python )
#
# COMMENTS:     Defines a Thinkbox Deadline scheduler implementation.
#               This module depends on the deadline commandline
#               which is installed with Deadline, for example: 
#               %DEADLINE_PATH%\deadlinecommand.exe
#
#               To use this module you must ensure that DEADLINE_PATH is set.
#               This module depends on the following mapped paths, which can be done
#               in Deadline, via Configure Repository Options -> Mapped Paths:
#                   -'$HFS'            => location of HFS
#                   -'$HFS/bin/hython' => location of hython
#                   -'$PYTHON'         => location of Python
#				
#				On Windows, it is required to add ".exe" to both hython and python.
#				This can be done directly on the node parm interface:
#					e.g.: \$HFS/bin/hython.exe
#				or in Deadline's Path Mapping:
#				-'$HFS/bin/hython' => 'C:/Program Files/Side Effects Software/Houdini 17.5.173/bin/hython.exe'
#               
#               This module also requires deadline_jobpreload.py (found in same location 
#               as this file), which will be invoked prior to each job.

import datetime
import os
import sys
import shutil
import logging
import time
import subprocess
import shlex
import re
import threading
import json
import traceback

### Firehawk versioning alterations
import hou
menu_path = os.environ['FIREHAWK_HOUDINI_TOOLS'] + '/scripts/modules'
sys.path.append(menu_path)
import firehawk_submit as firehawk_submit
###

import pdg
from pdg.scheduler import PyScheduler, evaluateParamOr
from pdg.job.callbackserver import CallbackServerMixin
from pdg.utils import TickTimer

logger = logging.getLogger(__name__)

def GetDeadlineCommand():
    """
    Finds and returns the Deadline command with full path on current platform, or empty string if not found.
    Requires that Deadline be installed and DEADLINE_PATH environment path is setup.
    """
    deadlineCommand = ""
    deadlineBin = ""
    try:
        deadlineBin = os.environ['DEADLINE_PATH']
    except KeyError:
        # If the error is a key error it means that DEADLINE_PATH is not set. 
        # However Deadline command may be in the PATH or on OSX it could be in the file /Users/Shared/Thinkbox/DEADLINE_PATH
        pass

    # On OSX, we look for the DEADLINE_PATH file if the environment variable does not exist.
    if deadlineBin == "" and sys.platform.startswith("darwin") and os.path.exists( "/Users/Shared/Thinkbox/DEADLINE_PATH" ):
        with open( "/Users/Shared/Thinkbox/DEADLINE_PATH" ) as f:
            deadlineBin = f.read().strip()

    if len(deadlineBin):
        deadlineCommand = os.path.join(deadlineBin, "deadlinecommand")

    return deadlineCommand

def CallDeadlineCommand(arguments, hideWindow=True, readStdout=True):
    """
    Calls the deadline command with given arguments.
    Requires that Deadline be installed and DEADLINE_PATH environment path is setup. 
    Returns the output from the invoked command as well as any errors.
    """
    deadlineCommand = GetDeadlineCommand()
    if len(deadlineCommand) == 0:
        return ""

    startupinfo = None
    creationflags = 0
    if os.name == 'nt':
        if hideWindow:
            # Python 2.6 has subprocess.STARTF_USESHOWWINDOW, and Python 2.7 has subprocess._subprocess.STARTF_USESHOWWINDOW, so check for both.
            if hasattr( subprocess, '_subprocess' ) and hasattr( subprocess._subprocess, 'STARTF_USESHOWWINDOW' ):
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= subprocess._subprocess.STARTF_USESHOWWINDOW
            elif hasattr( subprocess, 'STARTF_USESHOWWINDOW' ):
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        else:
            # Still show top-level windows, but don't show a console window
            CREATE_NO_WINDOW = 0x08000000   #MSDN process creation flag
            creationflags = CREATE_NO_WINDOW

    environment = {}
    for key in os.environ.keys():
        environment[key] = str(os.environ[key])

    # Need to set the PATH, because windows seems to load DLLs from the PATH earlier than cwd....
    if os.name == 'nt':
        deadlineCommandDir = os.path.dirname( deadlineCommand )
        if not deadlineCommandDir == "" :
            environment['PATH'] = deadlineCommandDir + os.pathsep + os.environ['PATH']

    arguments.insert( 0, deadlineCommand )
    #logger.debug("deadline popen: {}".format(str(arguments)))

    # Specifying PIPE for all handles to workaround a Python bug on Windows. The unused handles are then closed immediatley afterwards.
    proc = subprocess.Popen(arguments, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, startupinfo=startupinfo, env=environment, creationflags=creationflags)

    output = ""
    errors = ""
    if readStdout:
        output, errors = proc.communicate()

    return output.strip(), errors.strip()

def GetJobIdFromSubmission(submissionResults):
    """
    Returns the job ID found in the given submission results string.
    The submission results should be the output from the deadline command to schedule a job.
    """
    job_id = ""
    for line in submissionResults.split():
        if line.startswith( "JobID=" ):
            job_id = line.replace( "JobID=", "" ).strip()
            break

    return job_id


class DeadlineScheduler(CallbackServerMixin, PyScheduler):
    """
    Scheduler implementation that interfaces with Thinkbox's Deadline scheduler.
    Creates plugin targetted jobs with job info file and plugin info file.
    Calls back on job completion and failure.
    """
    def __init__(self, scheduler, name):
        PyScheduler.__init__(self, scheduler, name)
        CallbackServerMixin.__init__(self, False)
        self.active_jobs = {}
        self.jobs_lock = threading.Lock()
        self.tick_timer = None
        self.custom_port_range = None
        self.launched_monitor = False
        self.initLogger(logger, logging.ERROR)


    @classmethod
    def templateName(cls):
        return "deadlinescheduler"


    @classmethod
    def templateBody(cls):
        return json.dumps({
            "name": "deadlinescheduler",
            "parameters" : [
                {
                    "name" : "deadline_connection_type",
                    "label" : "Connection Type",
                    "type" : "String",
                    "size" : 1,
                    "value" : "Direct"
                },
                {
                    "name" : "deadline_repository",
                    "label" : "Repository",
                    "type" : "String",
                    "size" : 1,
                },
                {
                    "name" : "localsharedroot",
                    "label" : "Local Shared Root Path",
                    "type" : "String",
                    "size" : 1,
                },
                {
                    "name" : "overrideremoterootpath",
                    "label" : "Enable Custom Remote Root Path",
                    "type" : "Integer",
                    "size" : 1,
                },
                {
                    "name" : "remotesharedroot",
                    "label" : "Remote Shared Root Path",
                    "type" : "String",
                    "size" : 1,
                    "value" : "\$PDG_DIR"
                },
                {
                    "name" : "deadline_launch_monitor",
                    "label" : "Launch Monitor",
                    "type" : "String",
                    "size" : 1,
                },
				{
                    "name" : "callbackportrange",
                    "label" : "Custom TCP port range for callback server",
                    "type" : "Integer",
                    "size" : 2,
                },
                {
                    "name" : "overrideportrange",
                    "label" : "Enable callbackportrange",
                    "type" : "Integer",
                    "size" : 1,
                },
                {
                    "name" : "deadline_plugin",
                    "label" : "Plugin",
                    "type" : "String",
                    "size" : 1,
                    "value" : "CommandLine",
                    "tag" : ["pdg::scheduler"]
                },
                {
                    "name" : "deadline_force_reload_plugin",
                    "label" : "Force Reload Plugin",
                    "type" : "String",
                    "size" : 1,
                    "value" : "false",
                    "tag" : ["pdg::scheduler"]
                },
                {
                    "name" : "deadline_pre_job_script",
                    "label" : "Pre Job Script",
                    "type" : "String",
                    "size" : 1,
                    "tag" : ["pdg::scheduler"]
                },
                {
                    "name" : "deadline_post_job_script",
                    "label" : "Post Job Script",
                    "type" : "String",
                    "size" : 1,
                    "tag" : ["pdg::scheduler"]
                },
                {
                    "name" : "deadline_job_pool",
                    "label" : "Job Pool",
                    "type" : "String",
                    "size" : 1,
                    "value" : "none",
                    "tag" : ["pdg::scheduler"]
                },
                {
                    "name" : "deadline_job_group",
                    "label" : "Job Group",
                    "type" : "String",
                    "size" : 1,
                    "value" : "none",
                    "tag" : ["pdg::scheduler"]
                },
                {
                    "name" : "deadline_job_frames",
                    "label" : "Frames",
                    "type" : "String",
                    "size" : 1,
                    "tag" : ["pdg::scheduler"]
                },
                {
                    "name" : "deadline_job_priority",
                    "label" : "Job Priority",
                    "type" : "Integer",
                    "size" : 1,
                    "value" : 50,
                    "tag" : ["pdg::scheduler"]
                },
                {
                    "name" : "deadline_job_dept",
                    "label" : "Job Department",
                    "type" : "String",
                    "size" : 1,
                    "tag" : ["pdg::scheduler"]
                },
                {
                    "name" : "deadline_job_batch_name",
                    "label" : "Job Batch Name",
                    "type" : "String",
                    "size" : 1,
                    "tag" : ["pdg::scheduler"]
                },
                {
                    "name" : "deadline_job_comment",
                    "label" : "Job Comment",
                    "type" : "String",
                    "size" : 1,
                    "tag" : ["pdg::scheduler"]
                },
                {
                    "name" : "deadline_on_job_complete",
                    "label" : "OnJobComplete",
                    "type" : "String",
                    "size" : 1,
                    "value" : "Nothing",
                    "tag" : ["pdg::scheduler"]
                },
                {
                    "name" : "deadline_hfs",
                    "label" : "HFS",
                    "type" : "String",
                    "size" : 1,
                    "value" : "\$HFS",
                    "tag" : ["pdg::scheduler"]
                },
                {
                    "name" : "deadline_hython",
                    "label" : "Hython",
                    "type" : "String",
                    "size" : 1,
                    "value" : "\$HFS/bin/hython",
                    "tag" : ["pdg::scheduler"]
                },
                {
                    "name" : "deadline_python",
                    "label" : "Python",
                    "type" : "String",
                    "size" : 1,
                    "value" : "\$PYTHON",
                    "tag" : ["pdg::scheduler"]
                },
                {
                    "name" : "deadline_envmulti",
                    "label" : "Environment",
                    "type" : "Integer",
                    "tag" : ["pdg::scheduler"]
                },
                {
                    "name" : "deadline_jobfile_kvpair",
                    "label" : "Plugin File Key-Values",
                    "type" : "Integer",
                    "tag" : ["pdg::scheduler"]
                },
                {
                    "name" : "deadline_pluginfile_kvpair",
                    "label" : "Plugin File Key-Values",
                    "type" : "Integer",
                    "tag" : ["pdg::scheduler"]
                }
            ]
        })


    def __del__(self):
        """
        Clean up
        """
        if self.tick_timer:
            self.tick_timer.cancel()

    def onStart(self):
        """
        onStart(self) -> boolean

        [virtual] Scheduler start callback.
        """
        return True

    def onStop(self):
        """
        onStop(self) -> boolean

        [virtual] Scheduler stop callback.
        """
        self.stopCallbackServer()
        self._stopSharedServers()
        return True

    def onStartCook(self, static, cook_set):
        """
        onStartCook(self, static, cook_set) -> boolean

        [virtual] Cook start callback. Starts a root job for the cook session
        """
        # print 'firehawk_log onStartCook self:', self
        # print 'firehawk_log onStartCook static:', static
        # print 'firehawk_log onStartCook cook_set:', cook_set

        # def work_item_state_change(event):
        #     print ""
        #     print 'state change'
        #     print 'event.node.name', event.node.name
        #     print 'event.lastState', event.lastState, 'event.currentState', event.currentState
        #     print 'event.workItemId', event.workItemId

        #     print 'state change event', event, 'event.node', event.node
        #     print 'self.name', self.name
        #     print 'event.node.workItems', event.node.workItems
            
        #     print 'event.node', event.node
        #     print 'event.dependencyId', event.dependencyId
            
        #     print 'event.type', event.type
        #     print 'event.message', event.message
        #     print ''

        def cook_done(event):
            if self.node_status=='cooking':
                # print "event", event.node.name, event.message
                self.node_status=='done'
                ### remove handler since the main job is about to execute, and we dont need this anymore. ###
                #self.graph_context.removeEventHandler(self.handler)
                # print "remove node handler.  cook complete", event.node.name
                # print 'self', self
                #print 'dir(self)', dir(self)
                # print 'event', event
                #print 'dir(event)', dir(event)
                #event.node.removeEventHandler(self.handler_item_state)
                event.node.removeEventHandler(self.handler_cook_complete)
                #self.handler_cook_complete
            else:
                print 'error node_status is not "cooking", this function should not be called', self.node_status

        if cook_set:
            for node in cook_set:
                if not node.isCooked:
                    # print ""
                    # print "node.name", node.name
                    # print "node.isCooked", node.isCooked
                    # print "node.workItems", node.workItems
                    if len(node.workItems) > 0:
                        print "item0", node.workItems[0]
                    # print "node.regenerateReason", node.regenerateReason
                    # print "node.staticWorkItems", node.staticWorkItems
                    # print "node.outputs", node.outputs
                    # print "node.outputNames", node.outputNames
                    # print 'adding handler for node not cooked', node.name

                    self.node_status='cooking'
                    #self.handler_item_state = node.addEventHandler(work_item_state_change, pdg.EventType.WorkItemStateChange)
                    self.handler_cook_complete = node.addEventHandler(cook_done, pdg.EventType.CookComplete)
                    # remove handler when cook is done / cancelled.

        logger.debug('firehawk_log onStartCook: {}'.format('test'))

        # Sanity check the local shared root
        localsharedroot = self["localsharedroot"].evaluateString()
        if not os.path.exists(localsharedroot):
            raise RuntimeError('localsharedroot file path not found: ' + localsharedroot)

        self._updateWorkingDir()

        file_root = self.workingDir(True)
        if not os.path.exists(file_root):
            os.makedirs(file_root)
        if not os.path.exists(self.tempDir(True)):
            os.makedirs(self.tempDir(True))

        # print 'firehawk_log onStartCook file_root:', file_root

        # override the listening port
        overrideportrange = self['overrideportrange'].evaluateInt()
        if overrideportrange > 0:
            callbackportrange = self["callbackportrange"].evaluateInt()
            if callbackportrange != self.custom_port_range:
                self.custom_port_range = callbackportrange
                self.stopCallbackServer()
                self.startCallbackServer()

        if not self.isCallbackServerRunning():
            self.startCallbackServer()

        self.tick_timer = TickTimer(0.25, self.tick)
        self.tick_timer.start()

        return True

    def onStopCook(self, cancel):
        """
        Callback invoked by PDG when graph cook ends.
        Notify Deadline to stop (fail) all active jobs.
        """
        if self.active_jobs and len(self.active_jobs) > 0:
            self.jobs_lock.acquire()
            job_ids = ''.join('{},'.format(item) for item in self.active_jobs.keys())
            self.jobs_lock.release()
            cmd_arg = self.getUserRepositoryCommandArgument(['FailJob', job_ids])
            CallDeadlineCommand(cmd_arg)

        if self.tick_timer:
            self.tick_timer.cancel()

        self._stopSharedServers()
        return True

    def onSchedule(self, work_item):
        """
        [virtual] Called when the scheduler should schedule a work item.
        Returns True on success, else False. Must be implemented by PyScheduler
        subclasses.
        """

        if len(work_item.command) == 0:
            return pdg.scheduleResult.CookSucceeded

        try:
            item_name = work_item.name
            item_id = work_item.index
            node = work_item.node
            node_name = node.name
            item_command = work_item.command

            
            
            print ""
            print "onschedule work_item.node", work_item.node
            print "work_item.index", work_item.index

            print "work_item.command", work_item.command
            
            

            logger.debug('onSchedule input: {} {} {}'.format(node_name, item_name, item_command))

            # Typical work item command:
            #executable __PDG_TEMP__/__PDG_ITEM_NAME__.proto __PDG_DIR__ __PDG_RESULT_SERVER__

            # Map PDG variables
            job_name = "{}_{}".format(item_name, item_id)

            temp_dir = self.tempDir(False)
            work_dir = self.workingDir(False)
            script_dir = self.scriptDir(False)
            
            print "temp_dir", temp_dir
            print "work_dir", work_dir

            
            

            item_command = item_command.replace("__PDG_ITEM_NAME__", item_name)
            item_command = item_command.replace("__PDG_SHARED_TEMP__", temp_dir)
            item_command = item_command.replace("__PDG_TEMP__", temp_dir)
            item_command = item_command.replace("__PDG_DIR__", work_dir)
            item_command = item_command.replace("__PDG_SCRIPTDIR__", script_dir)
            item_command = item_command.replace("__PDG_RESULT_SERVER__", self.workItemResultServerAddr() )
            item_command = item_command.replace("__PDG_PYTHON__", self.pythonBin(work_item, sys.platform, False))
            item_command = item_command.replace("__PDG_HYTHON__", self.hythonBin(work_item, sys.platform, False))

            
            print "item_command", item_command
            
            render_hip = re.sub(r'(^.*?-p.\")(.*?)(".-n.*)', r"\2", item_command)
            print "render_hip", render_hip
            if '.hip' in render_hip:
                print "Hip file ref will be replaced with optimal path for data transfer.  Ensure that the hip is synchronised correctly prior to submission."
                prod_root = os.environ['PROD_ROOT']
                prod_onsite_root = os.environ['PROD_ONSITE_ROOT']
                prod_cloud_root = os.environ['PROD_CLOUD_ROOT']

                def convert_path(item, target_base_path):
                    result = ''
                    if item.startswith(prod_root):
                        result = "{base}"+item[len(prod_root):]
                    elif item.startswith(prod_onsite_root):
                        result = "{base}"+item[len(prod_onsite_root):]
                    elif item.startswith(prod_cloud_root):
                        result = "{base}"+item[len(prod_cloud_root):]
                    else:
                        print "No path match:", item, 'for items:', prod_root, prod_onsite_root, prod_cloud_root
                    result = target_base_path+result[len("{base}"):]
                    return result

                render_hip = convert_path(render_hip, prod_root)
                
                # ensure the item command uses hip from current physical location
                item_command = re.sub(r'(^.*?-p.\")(.*?)(".-n.*)', r"\1"+render_hip+r"\3", item_command)
                print "item_command post edit", item_command

            cmd_argv = shlex.split(item_command)

            print "item_command", item_command
            
            if len(cmd_argv) < 2:
                logger.error('Could not shelx command: ' + item_command)
                return pdg.scheduleResult.Succeeded

            
            temp_root_local = self.tempDir(True)
            
            ### firehawk on schedule version handling
            index_key = work_item.data.stringData('index_key', 0)
            if index_key is not None:
                print "on schedule version handling", work_item
                rop_path = work_item.data.stringData('rop', 0)
                hou_node = hou.node(rop_path)
                print "hou_node", hou_node
                firehawk_submit.submit(hou_node).onScheduleVersioning(work_item)
            ### end firehawk on schedule version handling

            # Json data is written at this point for the live session.

            # Ensure directories exist and serialize the work item
            self.createJobDirsAndSerializeWorkItems(work_item)

            # Get HFS from parm
            hfs_path = self.evaluateStringOverride(work_item.node, 'deadline', 'hfs', work_item, '')

            # Job info file: __PDG_TEMP__/cmdjob_item_name_index.txt
            job_file_name = '{}/cmdjob_{}_{}.txt'.format(temp_root_local, item_name, str(item_id))
            
            # import hou
            # from shutil import copyfile

            # hip_path = hou.hipFile.path()
            # hip_basename = hou.hipFile.basename()
            # hou.hipFile.save()
            
            # pdg_hip_name = '/'+work_dir.strip('/')+'/'+hip_basename
            # print "copy hip to staging dir again", pdg_hip_name
            # copyfile(hip_path, pdg_hip_name)
            # # version_dir = work_dir
            # # print 'write verison to ', work_dir

            with open(job_file_name, 'w') as job_file:
                plugin_name = self.evaluateStringOverride(node, 'deadline', 'plugin', work_item, '')
                job_file.write('Plugin={}\n'.format(plugin_name))
                job_file.write('Name={}\n'.format(job_name))

                self.writeStringDataFromWorkItem(work_item, job_file, 'pre_job_script', 'PreJobScript')
                self.writeStringDataFromWorkItem(work_item, job_file, 'post_job_script', 'PostJobScript')
                self.writeStringDataFromWorkItem(work_item, job_file, 'job_pool', 'Pool')
                self.writeStringDataFromWorkItem(work_item, job_file, 'job_group', 'Group')
                self.writeStringDataFromWorkItem(work_item, job_file, 'job_frames', 'Frames')
                self.writeStringDataFromWorkItem(work_item, job_file, 'job_dept', 'Department')
                self.writeStringDataFromWorkItem(work_item, job_file, 'job_batch_name', 'BatchName')
                self.writeStringDataFromWorkItem(work_item, job_file, 'job_comment', 'Comment')
                self.writeStringDataFromWorkItem(work_item, job_file, 'on_job_complete', 'OnJobComplete')
                self.writeStringDataFromWorkItem(work_item, job_file, 'force_reload_plugin', 'ForceReloadPlugin')

                self.writeIntDataFromWorkItem(work_item, job_file, 'job_priority', 'Priority')

                self.writeJobFileKeyValues(job_file, work_item, work_dir)

                # Job environment
                env_idx = 0
                env_idx = self.writeJobEnv(job_file, env_idx, 'PDG_RESULT_SERVER', str(self.workItemResultServerAddr()))
                env_idx = self.writeJobEnv(job_file, env_idx, 'PDG_ITEM_NAME', item_name)
                env_idx = self.writeJobEnv(job_file, env_idx, 'PDG_DIR', str(work_dir))
                env_idx = self.writeJobEnv(job_file, env_idx, 'PDG_TEMP', str(temp_dir))
                env_idx = self.writeJobEnv(job_file, env_idx, 'PDG_SHARED_TEMP', str(temp_dir))
                env_idx = self.writeJobEnv(job_file, env_idx, 'PDG_INDEX', str(work_item.index))
                env_idx = self.writeJobEnv(job_file, env_idx, 'PDG_INDEX4', "{:04d}".format(work_item.index))
                env_idx = self.writeJobEnv(job_file, env_idx, 'PDG_SCRIPTDIR', str(script_dir))

                env_idx = self.writeJobEnv(job_file, env_idx, 'PDG_JOBID', 'DL_JOB_ID')
                env_idx = self.writeJobEnv(job_file, env_idx, 'PDG_JOBID_VAR', 'PDG_JOBID')

                env_idx = self.writeJobEnv(job_file, env_idx, 'HFS', hfs_path)

                env_idx = self.writeJobFileEnvKeyValues(job_file, work_item, env_idx)
                

            # Plugin info file: __PDG_TEMP__/cmdplugin.txt_name_index.txt
            plugin_file_name = '{}/cmdplugin_{}_{}.txt'.format(temp_root_local, item_name, str(item_id))

            with open(plugin_file_name, 'w') as plugin_file:
                plugin_file.write('ShellExecute=False\n')
                plugin_file.write('Executable=%s\n' % cmd_argv[0])

                # Remove executable from arguments and surround each argument with quotes
                del cmd_argv[0]
                cmd_str = ''.join('"{}" '.format(item) for item in cmd_argv)
                plugin_file.write('Arguments={}\n'.format(str(cmd_str)))

                self.writePluginFileKeyValues(plugin_file, work_item)

            deadline_cmd = self.getUserRepositoryCommandArgument(['-submitJob', job_file_name, plugin_file_name])
            logger.debug('onSchedule deadline command: {}'.format(deadline_cmd))

            job_result, job_err = CallDeadlineCommand( deadline_cmd )
            if job_result.startswith("Error"):
                error_msg = "Deadline submit job command failed!\n{}\n{}".format(job_result, job_err)
                logger.error(error_msg)
                return pdg.scheduleResult.Failed

            job_id = GetJobIdFromSubmission( job_result )
            if not job_id:
                logger.error('onSchedule: Failed to schedule job or no job id found!')
                return pdg.scheduleResult.Failed
            else:
                logger.debug('Job submitted with ID: {}'.format(job_id))

            self.jobs_lock.acquire()
            self.active_jobs[job_id] = item_name
            self.jobs_lock.release()
            work_item.data.setString("deadline_jobid", job_id, 0)

            # Launch monitor if set
            monitor_host_name = self['deadline_launch_monitor'].evaluate()
            if len(monitor_host_name) and not self.launched_monitor:
                deadline_cmd = self.getUserRepositoryCommandArgument(['--RemoteControl', monitor_host_name, 'LaunchMonitor'])
                CallDeadlineCommand( deadline_cmd )
                self.launched_monitor = True
            print "### end onschedule ###"
            return pdg.scheduleResult.Succeeded
        except:
            import traceback
            traceback.print_exc()
            sys.stderr.flush()
            return pdg.scheduleResult.Failed

    def _updateWorkingDir(self):
        """
        Full path to working dir, rooted with env var which can be interpreted by slave on farm.
        Local working dir is set as user provided.
        Non-local is set to either same as local, or can be overriden to be a variable that 
        Deadline is configured to replace with its own path mapping.
        """
        workingbase = self["pdg_workingdir"].evaluateString()
        if os.path.isabs(workingbase):
            raise RuntimeError("Relative Job Directory \'" + workingbase + "\' must be relative path!")

        local_wd = os.path.normpath(self["localsharedroot"].evaluateString() + "/" + workingbase)
        local_wd = local_wd.replace("\\", "/")
        if self["overrideremoterootpath"].evaluateInt() == 0:
            remote_wd = local_wd
        else:
            remote_wd = '{}/{}'.format(self['remotesharedroot'].evaluateString(), workingbase)
        self.setWorkingDir(local_wd, remote_wd)
    
    def pythonBin(self, work_item, platform, local):
        """
        Returns path to python executable.
        """
        return self.evaluateStringOverride( work_item.node, 'deadline', 'python', work_item, '')

    def hythonBin(self, work_item, platform, local):
        """
        Returns path to python executable.
        """
        return self.evaluateStringOverride( work_item.node, 'deadline', 'hython', work_item, '')

    def submitAsJob(self, graph_file, node_path):
        logger.error("This Deadline scheduler does not support cooking the network as a single job.")
        return ""

    def workItemSucceeded(self, name, index, cook_duration, jobid=''):
        """
        Called by CallbackServerMixin when a workitem signals success.
        """
        logger.debug('Job Succeeded: {}'.format(name))
        self.onWorkItemSucceeded(name, index, cook_duration)

    def workItemFailed(self, name, index, jobid=''):
        """
        Called by CallbackServerMixin when a workitem signals failure.
        """
        logger.debug('Job Failed: name={}, index={}, jobid={}'.format(name, index, jobid))
        self.onWorkItemFailed(name, index)

    def workItemCancelled(self, name, index, jobid=''):
        """
        Called by CallbackServerMixin when a workitem signals cancelled.
        """
        logger.debug('Job Cancelled: {}'.format(name))
        self.onWorkItemCanceled(name, index)

    def workItemStartCook(self, name, index, jobid=''):
        """
        Called by CallbackServerMixin when a workitem signals start.
        """
        self.onWorkItemStartCook(name, index)

    def workItemFileResult(self, item_name, subindex, result, tag, checksum, jobid=''):
        """
        Called by CallbackServerMixin when a workitem signals file result data reported.
        """
        self.onWorkItemFileResult(item_name, subindex, result, tag, checksum)

    def workItemSetAttribute(self, item_name, subindex, attr_name, data, jobid=''):
        """
        Called by CallbackServerMixin when a workitem signals simple result data reported.
        """
        self.onWorkItemSetAttribute(item_name, subindex, attr_name, data)

    def tick(self):
        """
        Called during a cook. Checks on jobs in flight to see if
        any have finished.
        """
        try:
            finished_job_id = None
            self.jobs_lock.acquire()

            for id,work_item_name in self.active_jobs.iteritems():
                deadline_cmd = self.getUserRepositoryCommandArgument(["GetJob", str(id)])
                job_info, job_err = CallDeadlineCommand(deadline_cmd)
                if len(job_info) < 1 or job_info.startswith("Error"):
                    error_msg = "Deadline get job info command failed!\n{}\n{}".format(job_info, job_err)
                    logger.error(error_msg)
                    self.workItemFailed(work_item_name, -1)
                    finished_job_id = id
                    break

                # Get the Status from the job info (e.g. Status=Completed)
                status_match = re.search('^Status=.*$', job_info, re.MULTILINE)
                if status_match:
                    status = status_match.group(0).replace("Status=", "").strip()
                    if status == "Completed":
                        # Parse the TotalRenderTime to get total time in seconds (e.g. TotalRenderTime=00:00:07.4310000)
                        total_render_time = 0
                        time_match = re.search('^TotalRenderTime=.*$', job_info, re.MULTILINE)
                        if time_match:
                            time_value = time_match.group(0).replace("TotalRenderTime=", "").strip()
                            h, m, s = time_value.split(':')
                            total_render_time = int(datetime.timedelta(hours=int(h), minutes=int(m), seconds=float(s)).total_seconds())

                        self.workItemSucceeded(work_item_name, -1, total_render_time)
                        finished_job_id = id
                        break
                    elif status == "Failed":
                        self.workItemFailed(work_item_name, -1)
                        finished_job_id = id
                        break

            if finished_job_id:
                del self.active_jobs[finished_job_id]

            self.jobs_lock.release()

        except:
            import traceback
            traceback.print_exc()
            sys.stderr.flush()
            self.jobs_lock.release()
            return False
        return True

    def getLogURI(self, work_item):
        """
        Returns the URI to the log file for the given work_item.
        Note that Deadline archives its log files in bz2 format, so 
        'file:///path/to/log.bz2' will be returned
        """
        work_item_id = work_item.data.stringData('deadline_jobid', 0)
        if work_item_id is not None:
            deadline_cmd = self.getUserRepositoryCommandArgument(['--GetJobLogReportFilenames', work_item_id])
            log_query, log_err = CallDeadlineCommand(deadline_cmd)
            if log_query.startswith("Error"):
                error_msg = "Deadline get job log command failed!\n{}\n{}".format(log_query, log_err)
                logger.error(error_msg)
                return ""
            elif log_query:
                log_query = log_query.replace('\\', '/')
                log_lines = log_query.strip().splitlines()
                # If user ran pre-job script, then there might be 2 or more subjobs within this job.
                # Getting the last log file since that's probably the actual job that ran.
                log_uri = log_lines[len(log_lines) - 1] if len(log_lines) > 0 else log_lines
                if log_uri:
                    if log_uri is not None:
                        log_uri = 'file:///{}'.format(log_uri)
                        if log_uri.endswith('.bz2'):
                            return log_uri
        return ""

    def writeStringDataFromWorkItem(self, work_item, fileobj, work_item_var_name, deadline_var_name):
        """
        Write out the string data in work_item with name work_item_var_name to fileobj using key deadline_var_name.
        After writing, fileobj will contain the following key-value pair: deadline_var_name=work_item['work_item_var_name']
        """
        value = self.evaluateStringOverride( work_item.node, 'deadline', work_item_var_name, work_item, '')
        if len(value):
            fileobj.write('{}={}\n'.format(deadline_var_name, value))

    def writeIntDataFromWorkItem(self, work_item, fileobj, work_item_var_name, deadline_var_name):
        """
        Write out the integer data in work_item with name work_item_var_name to fileobj using key deadline_var_name.
        After writing, fileobj will contain the following key-value pair: deadline_var_name=work_item['work_item_var_name']
        """
        value = self.evaluateIntOverride( work_item.node, 'deadline', work_item_var_name, work_item, 0)
        if value is not None:
            fileobj.write('{}={}\n'.format(deadline_var_name, value))

    def writeJobEnv(self, job_file, env_idx, key, value):
        """
        Write out the environment key-value pair to the given job_file, at the given index.
        """
        job_file.write('EnvironmentKeyValue{}={}={}\n'.format(env_idx, key, value))
        env_idx += 1
        return env_idx

    def writeFileKeyValues(self, out_file, kvpairs):
        for key, value in kvpairs.iteritems():
            out_file.write('{}={}\n'.format(key, value))

    def writeJobFileKeyValues(self, job_file, work_item, work_dir):
        """
        Writes out jobfile key-value pairs.
        Also writes out OutputDirectory0 if not specified in key-value pairs.
        """
        kvpairs = {}
        hasOutputJobEntry = False
        def _getParmPairs(node):
                nvars = self.evaluateIntOverride(node, 'deadline', 'jobfile_kvpair', work_item, 0)                
                if nvars:
                    for i in xrange(1, nvars + 1):
                        name = self.evaluateStringOverride(node, 'deadline', 'jobfile_key' + str(i), work_item, '')
                        val = self.evaluateStringOverride(node, 'deadline', 'jobfile_value' + str(i), work_item, '')
                        if not name:
                            continue
                        if name == "OutputDirectory0":
                            hasOutputJobEntry = True
                        kvpairs[name] = val

        _getParmPairs(None)
        _getParmPairs(work_item.node)
        self.writeFileKeyValues(job_file, kvpairs)

        # Add default output directory as working directory
        if not hasOutputJobEntry:
            job_file.write('OutputDirectory0={}\n'.format(work_dir))

    def writeJobFileEnvKeyValues(self, job_file, work_item, env_idx):
        """
        Writes out jobfile environment key-value pairs.
        """
        kvpairs = {}
        def _writeParmHelper(node, env_idx):
            nvars = self.evaluateIntOverride(node, 'deadline', 'envmulti', work_item, 0)
            if nvars:
                for i in xrange(1, nvars + 1):
                    name = self.evaluateStringOverride(node, 'deadline', 'envname' + str(i), work_item, '')
                    val = self.evaluateStringOverride(node, 'deadline', 'envvalue' + str(i), work_item, '')
                    if not name:
                        continue
                    kvpairs[name] = val

        _writeParmHelper(None, env_idx)
        _writeParmHelper(work_item.node, env_idx)

        for key, value in kvpairs.iteritems():
            env_idx = self.writeJobEnv(job_file, env_idx, key, value)
        return env_idx

    def writePluginFileKeyValues(self, plugin_file, work_item):
        """
        Writes out pluginfile environment key-value pairs.
        """
        kvpairs = {}
        def _writeParmHelper(node):
            nvars = self.evaluateIntOverride(node, 'deadline', 'pluginfile_kvpair', work_item, 0)
            if nvars:
                for i in xrange(1, nvars + 1):
                    name = self.evaluateStringOverride(node, 'deadline', 'pluginfile_key' + str(i), work_item, '')
                    val = self.evaluateStringOverride(node, 'deadline', 'pluginfile_value' + str(i), work_item, '')
                    if not name:
                        continue
                    kvpairs[name] = val

        _writeParmHelper(None)
        _writeParmHelper(work_item.node)
        self.writeFileKeyValues(plugin_file, kvpairs)

    def getUserRepositoryCommandArgument(self, cmd_args):
        """
        Returns deadline command argument formulated from user-specified deadline repository,
        connection, and command-specific arguments.
        If user left the repository field empty, just returns the cmd_args, which implies
        using default repository.
        """
        final_args = []
        repo = self['deadline_repository'].evaluate()
        if repo:
             final_args = ['--RunCommandForRepository', self['deadline_connection_type'].evaluate(), repo]
        return final_args + cmd_args

    def _stopSharedServers(self):
        for sharedserver_name in self.getSharedServers():
            self.endSharedServer(sharedserver_name)

    def onSharedServerStarted(self, args):
        """
        Called when a job has started a new sharedserver
        """
        logger.debug("sharedserver started: {}, args = {}".format(args["name"], args))
        self.setSharedServerInfo(args["name"], args)

    def endSharedServer(self, sharedserver_name):
        """
        Called by a job or on cook end to terminate the sharedserver
        """
        try:
            info = self.getSharedServerInfo(sharedserver_name)
            logger.debug("Killing sharedserver: " + sharedserver_name)
            from pdgjob.sharedserver import shutdownServer
            # FIXME:
            # at this point we need to kill the server which is running somewhere on the farm
            # it would be nice to do this directly with deadline, but the server is not officially a job.
            # This will need to be reworked so that the onFailed/onSuccess callbacks of the top-level
            # job are responsible for cleaning up the server.
            shutdownServer(info)

            # Setting info to empty string removes from the scheduler internal list
            self.clearSharedServerInfo(sharedserver_name)
        except:
            traceback.print_exc()
            return False
        return True


