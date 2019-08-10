#
# PROPRIETARY INFORMATION.  This software is proprietary to
# Side Effects Software Inc., and is not to be reproduced,
# transmitted, or disclosed in any way without written permission.
#
# Produced by:
#	Side Effects Software Inc
#	123 Front Street West, Suite 1401
#	Toronto, Ontario
#	Canada   M5J 2M2
#	416-504-9876
#
# NAME:	        local.py ( Python )
#
# COMMENTS:     Defines a local scheduler implementation that runs work item
#               commands in asynchronously. The commands run in seperate
#               processes, up to some specified maximum process count.

import json
import logging
import os
import re
import shlex
import signal
import subprocess
import sys
import threading
import time
import traceback
from collections import namedtuple
from distutils.spawn import find_executable
from multiprocessing import cpu_count

from pdg import createHarsServer, createProcessJob, scheduleResult
from pdg.job.callbackserver import CallbackServerMixin
from pdg.scheduler import PyScheduler, convertEnvMapToUTF8
from pdg.staticcook import StaticCookMixin
from pdg.utils import TickTimer, expand_vars

### Firehawk versioning alterations
import hou
menu_path = os.environ['FIREHAWK_HOUDINI_TOOLS'] + '/scripts/modules'
sys.path.append(menu_path)
import firehawk_submit as firehawk_submit
###

RunItem = namedtuple('RunItem', 'item_name process output_file single hars_server cpu_slots')
HarsServer = namedtuple('HarsServer', 'hdanorm token pipe pid')

logger = logging.getLogger(__name__)

class LocalScheduler(CallbackServerMixin, StaticCookMixin, PyScheduler):
    def __init__(self, scheduler, name):
        try:
            PyScheduler.__init__(self, scheduler, name)
            StaticCookMixin.__init__(self)
            CallbackServerMixin.__init__(self, True)
            self.initLogger(logger, logging.ERROR)
            self.schedule_lock = threading.RLock()
            self.running_single = False
            self.run_list = []
            self.subprocessJob = None
            self.hars_pools = {}
            # timer for our tick function
            self.tick_timer = None
            self.static_cook = False
            # increments for each cook
            self.cook_id = '0'
        except:
            traceback.print_exc()

    @classmethod
    def templateName(cls):
        return "localscheduler"

    @classmethod
    def templateBody(cls):
        return json.dumps(
            {
            "name": "localscheduler",
            "parameters" : [
                #
                # 0 = 1/4 of cpu count
                # 1 = use maxprocs parm below
                # -1 = All cpus except 1
                {
                    "name" : "maxprocsmenu",
                    "label" : "Max CPUs Mode",
                    "type" : "Integer",
                    "size" : 1,
                    "value" : 0
                },
                # maximum cpu-slots.  Each task costs N cpu-slots to run,
                # as specified using the pdg::scheduler local_CPUs_to_use.  This
                # number is interpreted as:
                # 0 : detected-physical-cores
                # -M : max(1, detected-physical-cores - M)
                # M  : M
                {
                    "name" : "maxprocs",
                    "label" : "Maximum CPUs",
                    "type" : "Integer",
                    "size" : 1,
                    "value" : 1
                },
                # Menu for temp dir mode [Working Dir, Houdini Temp, Custom]
                {
                    "name" : "tempdirmenu",
                    "label" : "Temp Dir Menu",
                    "type" : "Integer",
                    "size" : 1
                },
                # Toggle for appending PID to the temp dir
                {
                    "name" : "tempdirappendpid",
                    "label" : "Temp Dir Append PID",
                    "type" : "Integer",
                    "size" : 1
                },
                # custom temp dir
                {
                    "name" : "tempdircustom",
                    "label" : "Temp Dir Custom Path",
                    "type" : "String",
                    "size" : 1
                },
                # pool size means the max HARS processes for each
                # HDA path.
                {
                    "name" : "hdaprocessorpoolsize",
                    "label" : "HDAProcessor Pool Size",
                    "type" : "Integer",
                    "size" : 1,
                    "value" : 0
                },
                # toggles the use of HARS pooling on and off
                {
                    "name" : "local_usepool",
                    "label" : "Use Pooled Execution",
                    "type" : "Integer",
                    "size" : 1,
                    "value" : 0,
                    "tag" : ["pdg::scheduler"]
                },
                # multi of [local_envname#, local_envvalue#]
                {
                    "name" : "local_envmulti",
                    "label" : "Environment",
                    "type" : "Integer",
                    "tag" : ["pdg::scheduler"]
                },
                # Don't start any new jobs when this job starts
                {
                    "name" : "local_single",
                    "label" : "Single",
                    "type" : "Integer",
                    "tag" : ["pdg::scheduler"]
                },
                # toggle on and use a fixed number of cpu slots for
                # this job instead of the default of 1
                {
                    "name" : "local_is_CPU_number_set",
                    "label" : "Set Number of CPUs",
                    "type" : "Integer",
                    "tag" : ["pdg::scheduler"]
                },
                # number of cpu-slots to use for each task
                {
                    "name" : "local_CPUs_to_use",
                    "label" : "CPUs per Job",
                    "type" : "Integer",
                    "tag" : ["pdg::scheduler"]
                },
                # Override HOUDINI_MAXTHREADS toggle
                {
                    "name" : "local_usehoudinimaxthreads",
                    "label" : "Set Number of CPUs",
                    "type" : "Integer",
                    "tag" : ["pdg::scheduler"]
                },
                # Override HOUDINI_MAXTHREADS
                {
                    "name" : "local_houdinimaxthreads",
                    "label" : "CPUs per Job",
                    "type" : "Integer",
                    "tag" : ["pdg::scheduler"]
                }
            ]
        })

    def onStart(self):
        return True

    def onStop(self):
        """
        Called when scheduler is destroyed
        """
        self.stopCallbackServer()
        self.killPools()
        return True

    def pythonBin(self, platform):
        """
        Returns the path to a python executable.  This executable
        will be used to execute generic python and is substituted in commands 
        with the __PDG_PYTHON__ token. 
        
        platform Is an identifier with the same rules as python's sys.platform.
                 (should be 'linux*' | 'darwin' | 'win*')
        local    True means returns the absolute path on the local file system.
        """
        # local python can be overriden with PDG_PYTHON env var
        val = 'python'
        if platform.startswith('win'):
            val = '$HFS/python27/python.exe'
        elif platform.startswith('linux'):
            val = '$HFS/python/bin/python'
        val = os.environ.get('PDG_PYTHON') or os.path.expandvars(val)
        return val

    def hythonBin(self, platform):
        """
        Returns the path to a hython executable.  This executable
        will be used to execute hython and is substituted in commands 
        with the __PDG_HYTHON__ token. 
        
        platform Is an identifier with the same rules as python's sys.platform.
                 (should be 'linux*' | 'darwin' | 'win*')
        """
        # local hython can be overriden with PDG_HYTHON env var
        val = 'hython'
        if platform.startswith('win'):
            val = '$HFS/bin/hython.exe'
        elif platform.startswith('linux') or platform.startswith('darwin'):
            val = '$HFS/bin/hython'
        val = os.environ.get('PDG_HYTHON') or os.path.expandvars(val)
        return val

    def onStartCook(self, static, cook_set):
        TempWorkDir,TempHoudiniTemp,TempCustom = 0,1,2
        self.cook_id = str(int(self.cook_id) + 1)

        # Since this is a local scheduler, we always return a local path.
        local_wd = os.path.abspath(self["pdg_workingdir"].evaluateString()).replace("\\", "/")
        self.setWorkingDir(local_wd, local_wd)
        tempdir_menu = self['tempdirmenu'].evaluateInt()
        tempdirappendpid = self['tempdirappendpid'].evaluateInt() > 0
        if tempdir_menu == TempWorkDir:
            if not tempdirappendpid:
                local_temp = local_wd + '/pdgtemp'
                self.setTempDir(local_temp, local_temp)
            else:
                # this is the default, not setTempDir needed
                pass
        elif tempdir_menu == TempHoudiniTemp:
            local_temp =  os.path.expandvars('$HOUDINI_TEMP_DIR/$HIPNAME/pdgtemp')
            if tempdirappendpid:
                local_temp += '/{}'.format(os.getpid())
            self.setTempDir(local_temp, local_temp)
        elif tempdir_menu == TempCustom:
            local_temp = self['tempdircustom'].evaluateString()
            if tempdirappendpid:
                local_temp += '/{}'.format(os.getpid())
            self.setTempDir(local_temp, local_temp)

        # determine the number of cpu slots to allocate to this scheduler
        max_cpu_slots_mode = self["maxprocsmenu"].evaluateInt()
        self.max_cpu_slots = self["maxprocs"].evaluateInt()
        if max_cpu_slots_mode == 1:
            if self.max_cpu_slots < 0:
                self.max_cpu_slots = max(1, cpu_count() + self.max_cpu_slots)
        elif max_cpu_slots_mode == 0:
            self.max_cpu_slots = cpu_count() / 4
        elif max_cpu_slots_mode == -1:
            self.max_cpu_slots = max(1, cpu_count() - 1)
        
        self.static_onStartCook()
        self.static_cook = static
        if not os.path.exists(self.workingDir(True)):
            os.makedirs(self.workingDir(True))

        if not os.path.exists(self.tempDir(True)):
            os.makedirs(self.tempDir(True))

        self.running_single = False
        self.tick_timer = TickTimer(0.5, self.tick)
        self.tick_timer.start()

        if not self.isCallbackServerRunning():
            self.startCallbackServer()
        
        return True

    def onStopCook(self, cancel):
        if self.tick_timer:
            self.tick_timer.cancel()
            self.tick_timer.join()
        self.kill()
        return True

    def submitAsJob(self, graph_file, node_path):
        # we don't support cooking network
        logger.debug("submitAsJob({},{})".format(graph_file, node_path))
        return ""

    def addProcessorPool(self, unique_name, hda_path, num_slaves):
        # spin up a new processor pool

        # create process job on demand
        if num_slaves > 0 and self.subprocessJob is None:
            self.subprocessJob = createProcessJob()
        
        servers = []
        for i in range(num_slaves):
            token = '{:02d}'.format(i)
            pipe_name = unique_name + '_' + token
            pid = createHarsServer(pipe_name, self.subprocessJob)
            servers.append(HarsServer(hda_path, token, pipe_name, pid))
            logger.debug('addProcessorPool ' + repr(servers[-1]))
        self.hars_pools[hda_path] = servers

    def killPools(self):
        try:
            # gather all the unused and used pool servers and kill them
            servers = [ri.hars_server for ri in self.run_list if ri.hars_server is not None]
            for harss in self.hars_pools.itervalues():
                servers.extend(harss)
            for server in servers:
                self._terminateProcess(server.pid)

            # Also kill all the sharedservers
            for sharedserver_name in self.getSharedServers():
                ok = self.endSharedServer(sharedserver_name, True)
                if not ok:
                    self.cookWarning("failed to terminate shared server '{}'".format(sharedserver_name))
        except:
            traceback.print_exc()
        self.hars_pools = {}

    def _generateEnvironment(self, work_item, cpu_slots):
        """
        Generate an environment dict for the given workitem, based on 
        the current process's environ.
        """
        # Populate the task environment.  We inherit the PDG host
        # process's environment
        job_env = os.environ.copy()

        # Tell Houdini tasks to limit the number of threads to the number
        # of cpu slots the user has specified (default 1), limited by 
        # whatever is set in the host environment.
        
        # NOTE: The user can override this behavior by explicitly setting
        # the env var in a pdg::scheduler or item environment
        # FIXME: This houdini-specific hack is... unfortunate
        usehoudinimaxthreads = self.evaluateIntOverride(work_item.node,
            'local', 'usehoudinimaxthreads', work_item, 0) > 0
        if usehoudinimaxthreads:
            houdinimaxthreads = self.evaluateIntOverride(work_item.node,
            'local', 'houdinimaxthreads', work_item, 1)
            job_env['HOUDINI_MAXTHREADS'] = str(houdinimaxthreads)
        elif cpu_slots >= 1:
            # They haven't indicated an override, so just inherit the generic 'cpus per task' value
            # unless they have indicated the env var specifically
            maxthreads = cpu_count() / self.max_cpu_slots
            if 'HOUDINI_MAXTHREADS' in job_env:
                maxthreads = min(maxthreads, int(job_env['HOUDINI_MAXTHREADS']))
            job_env['HOUDINI_MAXTHREADS'] = str(maxthreads)

        # check the task environment variables
        env_map = work_item.environment
        setmap = {}
        for var,val in env_map.iteritems():
            var = str(var.strip().encode('ascii', 'ignore'))
            setmap[var] = str(val).strip().encode('ascii', 'ignore')
        job_env.update(setmap)

        # set the special env vars
        job_env['PDG_RESULT_SERVER'] = str(self.workItemResultServerAddr())
        job_env['PDG_ITEM_NAME'] = str(work_item.name)
        job_env['PDG_DIR'] = str(self.workingDir(False))
        job_env['PDG_TEMP'] = str(self.tempDir(False))
        job_env['PDG_SHARED_TEMP'] = str(self.tempDir(False))
        job_env['PDG_INDEX'] = str(work_item.index)
        job_env['PDG_INDEX4'] = '{:04d}'.format(work_item.index)
        job_env['PDG_SCRIPTDIR'] = str(self.scriptDir(False))
        # The env var that will hold our job identifier on the farm, it
        # is used to detect stale / invalid result callbacks
        job_env['PDG_JOBID'] = self.cook_id
        job_env['PDG_JOBID_VAR'] = 'PDG_JOBID'

        # local env is supplied as multiparm of key:key
        job_env_dict, removekeys = self.resolveEnvParams('local', work_item, True)

        # special case PATH - we want to prepend the given value instead of replace
        if 'PATH' in job_env_dict and 'PATH' in job_env:
            job_env_dict['PATH'] = job_env_dict['PATH'] + os.pathsep + job_env['PATH']
        
        job_env.update(job_env_dict)

        try:
            # add special houdini vars
            local_environ = os.environ
            job_env['HIP'] = local_environ['HIP']
            # we don't want HFS in the environment because it will short-circuit houdini
            # normal startup
            job_env['ORIGINAL_HFS'] = local_environ['HFS']
            # backup special vars - this only matters for hython and hbatch-based jobs
            job_env['ORIGINAL_HIP'] = job_env['HIP']
            job_env['HIPNAME'] = local_environ['HIPNAME']
            job_env['ORIGINAL_HIPNAME'] = job_env['HIPNAME']
        except KeyError:
            pass

        # process any removals
        for k in removekeys:
            if k in job_env:
                del job_env[k]

        # ensure there is no unicode in the environment
        job_env = convertEnvMapToUTF8(job_env)

        return job_env

    def tick(self):
        """
        Called during a cook. Checks on jobs in flight to see if
        any have finished.  Also starts new jobs if any are availible.
        """
        try:
            want_yield = False
            for run_item in self.run_list:
                if run_item.process is None:
                    # early fail for process that didn't start
                    run_item.output_file.close()
                    try:
                        if os.path.getsize(run_item.output_file.name) == 0:
                            os.remove(run_item.output_file.name)
                    except:
                        pass
                    self.run_list.remove(run_item)
                    self.workItemFailed(run_item.item_name, -1)
                    continue
                # check if we have a return code yet
                code = run_item.process.poll()
                if code is not None:
                    run_item.output_file.close()
                    try:
                        if os.path.getsize(run_item.output_file.name) == 0:
                            os.remove(run_item.output_file.name)
                    except:
                        pass
                    self.run_list.remove(run_item)

                    if run_item.single:
                        self.running_single = False
                        logger.debug("Running Single OFF")

                    # if this item was using a pool slot, return the slave spec to the pool
                    if run_item.hars_server:
                        self.hars_pools[run_item.hars_server.hdanorm].append(run_item.hars_server)
                        # yield this tick to allow HARS time to clean up the connection
                        want_yield = True

                    if code == 0:
                        self.workItemSucceeded(run_item.item_name, -1, 0)
                    else:
                        self.workItemFailed(run_item.item_name, -1)
            if want_yield:
                return

            cpu_slots_used = sum([run_item.cpu_slots for run_item in self.run_list])
            if cpu_slots_used < self.max_cpu_slots:
                # we have a free slot, request an onSchedule callback right now!
                if self.static_cook:
                    self.static_tick()
                else:
                    self.requestTask()
        except:
            traceback.print_exc()
            sys.stderr.flush()

    def kill(self):
        with self.schedule_lock:
            try:
                for run_item in self.run_list:
                    try:
                        if run_item.process.pid > 0:
                            self._terminateProcess(run_item.process.pid)
                            run_item.output_file.write("\nRunning workitem task killed by scheduler\n")
                            run_item.output_file.close()
                    except:
                        pass
                self.killPools()
            finally:
                self.run_list = []
                self.running_single = False

    def _verifyJobId(self, name, jobid):
        job_id = self.cook_id
        # if jobid is empty, don't fail
        if jobid:
            reported_job_id = jobid
            if job_id != reported_job_id:
                logger.debug("Job ID mismatch {} != {}".format(job_id, reported_job_id))
                return None
        return job_id

    def workItemSucceeded(self, name, index, cook_duration, jobid=''):
        """
        callback for work item finishing without error
        """
        job_id = self._verifyJobId(name, jobid)
        if job_id is None:
            return
        self.onWorkItemSucceeded(name, index, cook_duration)

        if self.static_cook:
            self.static_completed(name, index)
            
    def workItemFailed(self, name, index, jobid=''):
        """
        callback for work item failure
        """
        job_id = self._verifyJobId(name, jobid)
        if job_id is None:
            return
        self.onWorkItemFailed(name, index)

    def workItemCanceled(self, name, index, jobid=''):
        """
        callback for work item being cancelled
        """
        job_id = self._verifyJobId(name, jobid)
        if job_id is None:
            return
        self.onWorkItemCanceled(name, index)

    def workItemStartCook(self, name, index, jobid=''):
        """
        callback for work item starting
        """
        job_id = self._verifyJobId(name, jobid)
        if job_id is None:
            return
        self.onWorkItemStartCook(name, index)

    def workItemFileResult(self, item_name, subindex, result, tag, checksum, jobid=''):
        """
        Called by mixin when a workitem signals file result data reported.
        """
        job_id = self._verifyJobId(item_name, jobid)
        if job_id is None:
            return
        self.onWorkItemFileResult(item_name, subindex, result, tag, checksum)

    def workItemSetAttribute(self, item_name, attr_name, data, jobid=''):
        """
        Called by mixin when a workitem signals simple result data reported.
        """
        job_id = self._verifyJobId(item_name, jobid)
        if job_id is None:
            return
        self.onWorkItemSetAttribute(item_name, attr_name, data)
    
    def onSharedServerStarted(self, args):
        """
        Called when a job has started a new sharedserver
        """
        logger.debug("sharedserver started: {}, args = {}".format(args["name"], args))
        self.setSharedServerInfo(args["name"], args)
        # FIXME: Add this server to our process job to ensure cleanup

    def endSharedServer(self, sharedserver_name, kill=False):
        """
        Called by a job or on cook end to terminate the sharedserver
        """
        try:
            info = self.getSharedServerInfo(sharedserver_name)
            logger.debug('sharedserver {} info = {}'.format(sharedserver_name, info))
            if info:
                pid = info["pid"]
                # pid <= 0 indicates an external server we don't want to clear,
                # the info will hang around once it's added
                if pid > 0:
                    from pdgjob.sharedserver import shutdownServer
                    if kill:
                        self._terminateProcess(pid)
                    else:
                        shutdownServer(info)
        except:
            traceback.print_exc()
            return False
        finally:
            # Setting info to empty string removes from the scheduler internal list
            self.clearSharedServerInfo(sharedserver_name)
        return True

    def _terminateProcess(self, pid):
        """
        Called to forcibly kill process
        """
        if os.name == 'nt':
            subprocess.Popen("taskkill /F /T /PID %i" % pid , shell=True)
        else:
            os.kill(pid, signal.SIGKILL)

    def isWorkItemReady(self, work_item_name, index):
        if self.static_cook:
            return self.static_isWorkItemReady(work_item_name, index)
        return self.cppObject().isWorkItemReady(work_item_name, index)

    def _replaceMagicVars(self, item_command, work_item = None):
        temp_dir_local = self.tempDir(True)
        if work_item:
            item_command = item_command.replace("__PDG_ITEM_NAME__", work_item.name)
        item_command = item_command.replace("__PDG_SHARED_TEMP__", temp_dir_local)
        item_command = item_command.replace("__PDG_TEMP__", temp_dir_local)
        item_command = item_command.replace("__PDG_SHARED_ROOT__", self.workingDir(True))
        item_command = item_command.replace("__PDG_DIR__", self.workingDir(True))
        item_command = item_command.replace("__PDG_SCRIPTDIR__", self.scriptDir(True))
        item_command = item_command.replace("__PDG_RESULT_SERVER__", self.workItemResultServerAddr() )
        item_command = item_command.replace("__PDG_PYTHON__", self.pythonBin(sys.platform))
        item_command = item_command.replace("__PDG_HYTHON__", self.hythonBin(sys.platform))
        return item_command
    
    def onScheduleStatic(self, dependency_map, dependent_map, ready_items):
        self.static_loadDependencies(dependency_map, dependent_map, ready_items)

    def onSchedule(self, work_item):
        node = work_item.node

        # we can't run a single task if we are already doing so
        is_single = self.evaluateIntOverride(node, 'local', 'single', work_item, 0) > 0
        use_cpu_slots = self.evaluateIntOverride(node, "local", "is_CPU_number_set", work_item, 0) > 0

        with self.schedule_lock:
            cpu_slots_used = sum([run_item.cpu_slots for run_item in self.run_list])
            logger.debug("onSchedule [{}/{} S:{}]: {}".format(cpu_slots_used, self.max_cpu_slots, self.running_single, work_item.name))

            cpu_slots = 1
            if use_cpu_slots:
                cpu_slots = self.evaluateIntOverride(node, 'local', 'CPUs_to_use', work_item, 1)
                if cpu_slots <= 0:
                    # Use Max-N slots
                    cpu_slots = max(0, self.max_cpu_slots - cpu_slots)

            # if the slots are full we can't run anything else
            if (cpu_slots_used + cpu_slots) > self.max_cpu_slots:
                if cpu_slots_used == self.max_cpu_slots:
                    return scheduleResult.FullDeferred
                return scheduleResult.Deferred

            # only one 'single' task can run at a time
            if self.running_single and is_single:
                return scheduleResult.Deferred

            # for work items in hdaprocessor we have to consider the HARS pool state
            hars_server = None
            is_hdaworkitem = work_item.node.__class__.__name__.lower() == "hdaprocessor"
            if is_hdaworkitem:
                item_requested_pool = self.evaluateIntOverride(node, 'local', 'usepool', work_item, 0) > 0
                global_pool_size = self['hdaprocessorpoolsize'].evaluateInt()
                use_pooled = global_pool_size > 0 and item_requested_pool
                if use_pooled:
                    hda = work_item.data.stringDataMap.get('hda', [''])[0]
                    hda = os.path.basename(hda)
                    if hda not in self.hars_pools:
                        # fist time this pooled hda has come in - create a pool
                        self.addProcessorPool(hda + '_pool', hda, global_pool_size)
                    
                    if hda in self.hars_pools:
                        hars_pool = self.hars_pools[hda]
                        if len(hars_pool) == 0:
                            # pool full, we can't accept this task
                            return scheduleResult.Deferred
                        else:
                            hars_server = hars_pool.pop()

            # Everything looks good - we're going to run it
            #

            # if this is 'single', set the global scheduler state
            if is_single:
                self.running_single = True

            item_name = str(work_item.name)
            item_command = work_item.command

            item_command = self._replaceMagicVars(item_command, work_item)

            ### firehawk on schedule version handling
            index_key = work_item.data.stringData('index_key', 0)
            if index_key is not None:
                print "on schedule version handling", work_item
                rop_path = work_item.data.stringData('rop', 0)
                hou_node = hou.node(rop_path)
                print "hou_node", hou_node
                firehawk_submit.submit(hou_node).onScheduleVersioning(work_item)
            ### end firehawk on schedule version handling

            # Ensure directories exist and serialize the work item
            self.createJobDirsAndSerializeWorkItems(work_item)

            job_env = self._generateEnvironment(work_item, cpu_slots)

            log_dir = self.getLocalLogDir()
            item_log_path = log_dir + '/' + item_name + '.log'

            def open_output_file():
                outf = open(item_log_path, 'w')
                return outf
            
            # replace any job vars, and then any Houdini vars ($HFS etc) 
            item_command = expand_vars(item_command, job_env)
            item_command = os.path.expandvars(item_command)

            # ensure no unicode is in the command, because we want to use shlex
            item_command = item_command.encode('ascii', 'ignore')

            proc = None
            output_file = open_output_file()

            # Actually start the process
            try:
                if hars_server is not None:
                    # this is a pooled hda task - start it!
                    proc = start_hdaprocessor_async(item_command, output_file, hars_server.pipe, job_env)
                else:
                    # not pooling this item - execute as new process
                    proc = start_async(item_command, output_file, job_env)
            except:
                errs = traceback.format_exc()
                # capture the errors in the logfile, we want this failure to be reported
                # via the cook failure instead of schedule failure.
                output_file.write(errs)

            # add a RunItem to the run_list to track the running process
            run_item = RunItem(item_name, proc, output_file, is_single, hars_server, cpu_slots)
            self.run_list.append(run_item)
            logger.debug("Running: " + repr(run_item))
            logger.debug("Command: " + item_command)

        self.workItemStartCook(item_name, -1)
        return scheduleResult.Succeeded
    
    def getStatusURI(self, work_item):
        # no seperate status page for local scheduler
        return ""
    
    def getLogURI(self, work_item):
        log_path = '{}/logs/{}.log'.format(self.tempDir(True), work_item.name)
        uri = 'file:///' + log_path
        return uri

def start_async(item_command, output_file, job_env):
    """
    Executes the given cmd in a non-blocking subprocess
    item_command: the command script (shell)
    output_file: output stream redirection
    job_env: override environment
    return: Popen object
    """
    final_command = item_command
    startupinfo = None
    creationflags = 0
    if os.name == 'nt':
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        creationflags |= subprocess.CREATE_NEW_PROCESS_GROUP
    argv = shlex.split(final_command)
    if find_executable(argv[0]) is None:
        err = "Could not find executable '{}'".format(argv[0])
        output_file.write(err)
        logging.warning(err)
        return None
    proc = subprocess.Popen(argv,
                            stdout=output_file, 
                            stderr=subprocess.STDOUT,
                            stdin=subprocess.PIPE,
                            shell=False, env=job_env,
                            startupinfo=startupinfo,
                            creationflags=creationflags)
    proc.stdin.close()
    return proc

def start_hdaprocessor_async(item_command, output_file, poolname, job_env):
    """
    Executes the given HDAProcessor cmd in a non-blocking subprocess.
    item_command: the command script (shell)
    output_file: output stream redirection
    poolname: Name of the HARS pipe to use
    job_env: override environment
    return: Popen object
    """
    final_command = item_command
    if poolname is not None:
        final_command += ' --pipe ' + poolname

    startupinfo = None
    creationflags = 0
    if os.name == 'nt':
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        creationflags |= subprocess.CREATE_NEW_PROCESS_GROUP
    argv = shlex.split(final_command)
    if find_executable(argv[0]) is None:
        err = "Could not find executable '{}'".format(argv[0])
        output_file.write(err)
        logging.warning(err)
        return None
    proc = subprocess.Popen(argv, 
                            stdout=output_file,
                            stderr=subprocess.STDOUT,
                            stdin=subprocess.PIPE,
                            shell=False, env=job_env,
                            startupinfo=startupinfo,
                            creationflags=creationflags)
    proc.stdin.close()
    return proc
