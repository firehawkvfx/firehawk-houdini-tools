#!/usr/bin/python

# pdg dependencies

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

import pdg
from pdg import scheduleResult
# from pdg import createProcessJob, scheduleResult
from pdg.job.callbackserver import CallbackServerMixin
from pdg.scheduler import PyScheduler, convertEnvMapToUTF8
from pdg.staticcook import StaticCookMixin
from pdg.utils import TickTimer, expand_vars


# firehawk versioning

import hou
import pdg
import sys
import re
import subprocess
import os
import errno
import numpy as np
import datetime

# callbacks
if hou.isUIAvailable():
    import hdefereval

from shutil import copyfile

#####    

class submit():
    def __init__(self, node=''):
        self.node = node
        self.selected_nodes = []
        self.pdg_node = ''
        self.parent = self.node.parent()

        self.preflight_node = None
        self.preflight_path = None
        self.post_target = None
        self.post_target_path = None
        self.parm_group = None
        self.found_folder = None
        self.handler = None
        self.preflight_pdg_node = None

        self.preflight_status = None

        self.hip_path = hou.hipFile.path()
        self.hip_dirname = os.path.split(self.hip_path)[0]
        self.hip_basename = hou.hipFile.basename().strip('.hip')
        self.hip_path_submission = None

        self.source_top_nodes = []
        self.added_workitems = []
        self.added_dependencies = []
        self.output_types = {
            'opengl': {'output': 'picture', 'extension': 'jpg', 'type_path': 'flipbook/frames', 'static_expression': False},
            'rop_geometry': {'output': 'sopoutput', 'extension': 'bgeo.sc', 'type_path': 'cache', 'static_expression': True},
            'vellumio': {'output': 'file', 'extension': 'bgeo.sc', 'type_path': 'cache', 'static_expression': False},
            'rop_alembic': {'output': 'filename', 'extension': 'abc', 'type_path': 'cache', 'static_expression': True},
            'file': {'output': 'file', 'extension': 'bgeo.sc', 'type_path': 'cache', 'static_expression': True},
            'ropcomposite': {'output': 'copoutput', 'extension': 'exr', 'type_path': 'flipbook/frames', 'static_expression': True},
            'ropmantra': {
                'output': 'vm_picture',
                'extension': 'exr',
                'type_path': 'render/frames',
                'static_expression': True,
                'overrides': {
                    'frame': "import hou"+'\n'+"value = '$F4'"+'\n'+"return value"
                }
            },
            'ifd': {
                'output': 'vm_picture',
                'extension': 'exr',
                'type_path': 'render/frames',
                'static_expression': True,
                'overrides': {
                    'frame': "import hou"+'\n'+"value = '$F4'"+'\n'+"return value"
                }
            },
            'ffmpegencodevideo': {
                'output': 'outputfilename',
                'extension': 'mp4',
                'type_path': 'flipbook/videos',
                'static_expression': False,
                'file_template': "`chs('shot_path')`/`chs('output_type')`/`chs('element_name')`/`chs('versionstr')`/`chs('shot')`.`chs('element_name')`.`chs('versionstr')`.`chs('wedge_string')`.`chs('file_type')`",
                'overrides': {
                    'expr': '"{ffmpeg}" -y -r {frames_per_sec}/1 -f concat -safe 0 -apply_trc iec61966_2_1 -i "{frame_list_file}" -c:v libx264 -b:v 10M -vf "fps={frames_per_sec},format=yuv420p" -movflags faststart "{output_file}"'
                }
            }
        }

    def assign_preflight(self):
        self.preflight_node = self.node
        print "assign preflight node", self.preflight_node.path(), "on topnet", self.parent

        # get the template group for the parent top node.
        self.parm_group = self.parent.parmTemplateGroup()
        print "Create folder parm template"

        self.parm_folder = hou.FolderParmTemplate("folder", "Firehawk")
        self.parm_folder.addParmTemplate(hou.StringParmTemplate(
            "preflight_node", "Preflight Node", 1, [""], string_type=hou.stringParmType.NodeReference))

        # Search for the template folder.  if it already exists it will be replaced in order to ensure parms are current.
        self.found_folder = self.parm_group.findFolder("Firehawk")

        print "self.found_folder", self.found_folder
        # If a folder already exists on the node, remove it and add it again.  This deals with any changes to the parm layout if they occur.
        if self.found_folder:
            self.parm_group.remove(self.found_folder)

        # re add the folder to the template group.
        self.parm_group.append(self.parm_folder)

        # update the parm template on the parent node.
        self.parent.setParmTemplateGroup(self.parm_group)

        # set values for path to preflight node.

        self.parent.parm("preflight_node").set(self.preflight_node.path())

        #self.preflight_node.setUserData('post_target', self.preflight_node.path())


    def cook(self):
        # all preview switches set to 0.  switch nodes starting with name "preview" are usefull for interactivve session testing only, but will revert to input 0 upon farm submission.
        print 'cook'
        for node in hou.node('/').allSubChildren():
            if node.name().startswith('preview'):
                print 'disable preview switch', node.path()
                node.parm('input').set(0)
            if node.type().name().startswith('read_wedges'):
                try:
                    print 'disable preview toggle on read node', node.path()
                    parm = node.parm('preview_live')
                    if parm:
                        parm.set(0)
                except:
                    print "didn't disable preview switch", node.path()

        print "Submit", self.node.path()

        if (self.parent.parmTuple("preflight_node") != None):
            self.preflight_path = self.parent.parm("preflight_node").eval()
            print "self.preflight_path", self.preflight_path
            self.preflight_node = self.parent.node(self.preflight_path)

            ### save before preflight ###

            timestamp_submission = False
            
            self.submit_name = self.hip_path

            if timestamp_submission:
                datetime_object = datetime.datetime.now()
                print(datetime_object)
                timestampStr = datetime_object.strftime("%Y-%m-%d.%H-%M-%S-%f")
                print('Current Timestamp : ', timestampStr)

                self.submit_name = "{dir}/{base}.{date}.hip".format(
                    dir=self.hip_dirname, base=self.hip_basename, date=timestampStr)

            print "save", self.submit_name
            hou.hipFile.save(self.submit_name)

            if self.preflight_node:
                print "preflight node path is", self.preflight_node.path()
                self.post_target_path = self.node.path()
                self.preflight_node.setUserData('post_target', self.post_target_path)
                print 'self.post_target_path set', self.post_target_path

                ### refresh workitems on the preflight node ###
                self.preflight_node.executeGraph(False, False, False, True)

                if hasattr(self.preflight_node.getPDGNode(), 'cook'):
                    print "cooking preflight", self.preflight_node.path()

                    def defer_refresh(node):
                        node.executeGraph(False, False, False, True)

                    def defer_dirty(node):
                        node.getPDGNode().dirty(True)

                    def defer_cook(node):
                        node.getPDGNode().cook(False)

                    def cook_done(event):
                        print 'preflight cook done'
                        self.post_target = hou.node( self.preflight_node.userData('post_target') )
                        if self.post_target:
                            print 'self.post_target_path check3', self.post_target.path()
                            print "Cook next task after preflight event", event.node, event.message
                            ### remove handler since the main job is about to execute, and we dont need this anymore. ###
                            execute_node = self.post_target
                            
                            # when the post node is setup to execute, remove it from the var so it wont occur again.
                            self.post_target = None
                            self.preflight_node.setUserData('post_target', '')

                            ### save after preflight ###
                            #hou.hipFile.save(self.submit_name)
                            ### refresh workitems for main job node ###

                            execute_node.executeGraph(False, False, False, True)
                            # hdefereval.executeDeferred(defer_refresh, execute_node)

                            if hasattr(self.node.getPDGNode(), 'cook'):
                                ### cook main job ###
                                # execute_node.getPDGNode().cook(False)
                                hdefereval.executeDeferred(defer_cook, execute_node)
                            else:
                                hou.ui.displayMessage(
                                    "Failed to cook, try initiliasing the node first with a standard cook / generate.")

                            # Save again with restored original name.
                            if timestamp_submission:
                                hou.hipFile.save(self.hip_path)

                            #self.handler = None
                        else:
                            print 'skipping post task.  no post_target path defined in userData for preflight node.  this should have been removed upon completion of the last preflight task'

                    print 'setup handler'
                    ### setup handler before executing preflight ###
                    self.preflight_pdg_node = self.preflight_node.getPDGNode()

                    self.post_target_path = self.preflight_node.userData('post_target')
                    print 'self.post_target_path', self.post_target_path
                    self.post_target = hou.node( self.post_target_path )
                    print 'self.post_target_path check2', self.post_target.path()
                    if self.handler is None:
                        print "Adding handler"
                        self.handler = self.preflight_pdg_node.addEventHandler(cook_done, pdg.EventType.CookComplete)
                    ### cook preflight ###
                    
                    # self.preflight_node.getPDGNode().dirty(True)
                    # self.preflight_node.getPDGNode().cook(False)
                    hdefereval.executeDeferred(defer_dirty, self.preflight_node)
                    hdefereval.executeDeferred(defer_cook, self.preflight_node)

                else:
                    #print "dir(self.preflight_node.getPDGNode())", dir(self.preflight_node.getPDGNode())
                    hou.ui.displayMessage(
                        "Preflight Failed to cook: Node wasn't initialised / cook method not available on this node.")
            else:
                # no preflight
                self.node.executeGraph(False, False, False, True)
                if hasattr(self.node.getPDGNode(), 'cook'):
                    self.node.getPDGNode().cook(False)
                else:
                    hou.ui.displayMessage(
                        "Failed to cook, try initiliasing the node first with a standard cook / generate.")
                # save again with original name.
                if timestamp_submission:
                    hou.hipFile.save(self.hip_path)

    def get_upstream_workitems(self):
        # this will generate the selected workitems
        self.pdg_node = self.node.getPDGNode()
        self.node.executeGraph(False, False, False, True)
        
        added_workitems = []

        added_nodes = []
        added_node_dependencies = []

        def append_node_dependencies(node):
            added_node_dependencies.append(node)
            if len(node.inputs) > 0:
                for input in node.inputs:
                    input_connections = input.connections
                    print "input_connections", input_connections
                    if len(input_connections) > 0:
                        for connection in input_connections:
                            dependency = connection.node
                            if dependency not in added_nodes:
                                added_nodes.append(dependency)

        
        added_nodes.append(self.pdg_node)
        for node in added_nodes:
            append_node_dependencies(node)
        diff_list = np.setdiff1d(added_nodes, added_node_dependencies)
        
        while len(diff_list) > 0:
            for node in diff_list:
                append_node_dependencies(node)
            diff_list = np.setdiff1d(
                added_nodes, added_node_dependencies)

        print "added_nodes", added_nodes

        for node in added_nodes:
            for workitem in node.workItems:
                added_workitems.append(workitem)

        return added_workitems

    def protect_upstream_workitem_directories(self):
        added_workitems = self.get_upstream_workitems()
        # nodes - inputs[0].connections[0].node.inputs[0].connections[0].node

        def touch(path):
            with open(path, 'a'):
                os.utime(path, None)

        def get_size(start_path = None):
            total_size = 0
            for dirpath, dirnames, filenames in os.walk(start_path):
                for f in filenames:
                    fp = os.path.join(dirpath, f)
                    # skip if it is symbolic link
                    if not os.path.islink(fp):
                        total_size += os.path.getsize(fp)
            return total_size

        def sizeof_fmt(num, suffix='B'):
            for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
                if abs(num) < 1024.0:
                    return "%3.1f%s%s" % (num, unit, suffix)
                num /= 1024.0
            return "%.1f%s%s" % (num, 'Yi', suffix)
        
        protect_dirs = []
        sizes = []

        for work_item in added_workitems:
            result_data_list = work_item.resultData
            expected_result_data_list = work_item.expectedResultData

            for result_data in result_data_list:
                path = result_data[0]
                path_dir = os.path.split(path)[0]
                if path_dir not in protect_dirs:
                    protect_dirs.append(path_dir)
                    protect_file = os.path.join(path_dir, '.protect')
                    touch(protect_file)
                    size = get_size(path_dir)
                    print "add .protect file into protect_dir:", path_dir, sizeof_fmt(size)
                    sizes.append( size )
            
            for result_data in expected_result_data_list:
                path = result_data[0]
                path_dir = os.path.split(path)[0]
                if path_dir not in protect_dirs:
                    protect_dirs.append(path_dir)
                    protect_file = os.path.join(path_dir, '.protect')
                    touch(protect_file)
                    size = get_size(path_dir)
                    print "add .protect file into protect_dir:", path_dir, sizeof_fmt(size)
                    sizes.append( size )



        
        total_size = sizeof_fmt(sum(sizes))
        print "total_size", total_size
        
        

    def dirty_upstream_source_nodes(self):
        # this will generate the selected workitems
        print "Dirty Upstream Source Nodes"
        # self.pdg_node = self.node.getPDGNode()

        # self.node.executeGraph(False, False, False, True)

        
        # self.added_workitems = []
        # self.added_dependencies = []

        # def append_workitems(node):
        #     if len(node.workItems) > 0:
        #         for workitem in node.workItems:
        #             if workitem not in self.added_workitems:
        #                 self.added_workitems += [workitem]

        # def append_dependencies(workitem):
        #     self.added_dependencies += [workitem]
        #     if len(workitem.dependencies) > 0:
        #         for dependency in workitem.dependencies:
        #             if dependency not in self.added_workitems:
        #                 self.added_workitems += [dependency]

        # append_workitems(self.pdg_node)

        # for workitem in self.added_workitems:
        #     append_dependencies(workitem)

        # diff_list = np.setdiff1d(self.added_workitems, self.added_dependencies)
        
        # # keep comparing work items for processed nodes with dependencies.  once the two lists are equal then all dependencies are tracked.
        # while len(diff_list) > 0:
        #     for workitem in diff_list:
        #         append_dependencies(workitem)
        #     diff_list = np.setdiff1d(
        #         self.added_workitems, self.added_dependencies)

        added_workitems = self.get_upstream_workitems()

        source_top_nodes = []
        for workitem in added_workitems:
            if len(workitem.dependencies) == 0:
                if workitem.node not in source_top_nodes:
                    source_top_nodes.append(workitem.node)

        for source_top_node in source_top_nodes:
            source_top_node.dirty(False)
            print "Dirtied source_top_node", source_top_node.name

    def update_index(self, node, index_int):
        index_key_parm_name = 'index_key' + str(index_int)
        #print "update node", node, 'index_int', index_int, 'index_key_parm_name', index_key_parm_name
        index_key_parm = node.parm(index_key_parm_name)
        #print 'update index from parm', index_key_parm
        index_key = index_key_parm.eval()
        #version = node.parm('version' + str(index_int) ).eval()
        node.setUserData('verdb_'+index_key, str(index_int))
        #print "Changed parm index_key", index_key, "index_int", index_int

    def multiparm_housecleaning(self, node, multiparm_count):
        print "Validate and clean out old dict. total parms:", multiparm_count
        index_keys = []
        for index_int in range(1, int(multiparm_count)+1):
            index_key_parm_name = 'index_key' + \
                str(index_int)
            print "index_key_parm_name", index_key_parm_name
            
            index_key_parm = node.parm(
                index_key_parm_name)
            print 'update', index_key_parm.name()
            index_key = index_key_parm.eval()

            print "index_key", index_key
            index_keys.append('verdb_'+index_key)
            print 'update index', index_int, 'node', node
            self.update_index(node, index_int)

        # first all items in dict will be checked for existance on node.  if they dont exist they will be destroyed on the dict.
        user_data_total = 0

        keys_to_destroy = []
        for index_key, value in node.userDataDict().items():
            if index_key not in index_keys and 'verdb_' in index_key:
                print "node missing key", index_key, ":", value, 'will remove'
                keys_to_destroy.append(index_key)
            else:
                user_data_total += 1

        # keys must be destroyed after they are known in the last operation or lookup will fail mid loop.
        if len(keys_to_destroy) > 0:
            for index_key in keys_to_destroy:
                node.destroyUserData(index_key)
                print "destroyed key", index_key

    # ensure parameter callbacks exists
    def parm_changed(self, node, event_type, **kwargs):
        parm_tuple = kwargs['parm_tuple']

        if parm_tuple is None:
            parm_warn = int(
                hou.node("/obj").cachedUserData("parm_warn_disable") != '1')
            if parm_warn:
                hou.ui.displayMessage(
                    "Too many parms were changed.  callback hasn't been designed to handle this yet, changes may be needed in cloud_submit.py to handle this.  see shell output")
                print "Too many parms were changed.  callback hasn't been designed to handle this yet, changes may be needed in cloud_submit.py to handle this.  see shell output.  This warning will be displayed once for the current session."
                hou.node(
                    "/obj").setCachedUserData('parm_warn_disable', '1')
            # print "node", node
            # print "event_type", event_type
            # print "parm_tuple", parm_tuple
            # print "kwargs", kwargs
        else:
            name = parm_tuple.name()

            # if a key has changed
            is_multiparm = parm_tuple.isMultiParmInstance()

            if is_multiparm and 'index_key' in name:

                if len(parm_tuple.eval()) > 1:
                    hou.ui.displayMessage(
                        "multiple items in tuple, changes may be needed in cloud_submit.py to handle this")

                index_int = next(re.finditer(
                    r'\d+$', name)).group(0)

                #print 'index_key in name', name, 'update', index_int

                self.update_index(node, index_int)

            # if multiparm instance count has changed, update all and remove any missing.
            # if 'versiondb0' in name:
            #     multiparm_count = parm_tuple.eval()[0]
            #     self.multiparm_housecleaning( node, multiparm_count )

    
    def add_version_db_callback(self, node):
        print "determin if add callback needed"
        parm_callback_applied = False
        for callback in node.eventCallbacks():
            for item in callback:
                if hasattr(item, 'func_name'):
                    func_name = item.func_name
                    if func_name == 'parm_changed':
                        parm_callback_applied = True
                        print "found callback on node"
        if not parm_callback_applied:
            print "add parm changed callback"
            node.addEventCallback((hou.nodeEventType.ParmTupleChanged, ), self.parm_changed)
            # do house cleaning on dict in case drift has occured between the dict and the multiparm state.
            multiparm_count = node.parm("versiondb0").eval()
            self.multiparm_housecleaning(node, multiparm_count)

    def update_rop_output_paths_for_selected_nodes(self, kwargs={}, versiondb=False):
        print "Update Rop Output Paths for Selected SOP/TOP Nodes. Note: currently not handling @attributes $attributes in element names correctly."
        self.selected_nodes = kwargs['items']

        for node in self.selected_nodes:
            print 'Name', node.name()
            if node.type().name() == 'ropfetch':
                node = node.node(node.parm('roppath').eval())
            if node:
                print 'set path', node.path()
                #hou.hscriptExpression("cd "+node.path())
                hou.cd(node.path())

                bake_names = True
                print "evaluate env vars"
                if bake_names:
                    node_name = node.name()

                    show_var = hou.hscriptExpression("${SHOW}")
                    seq_var = hou.hscriptExpression("${SEQ}")
                    shot_var = hou.hscriptExpression("${SHOT}")

                    shot_var = show_var+'.'+seq_var+'.'+shot_var
                    shotpath_var = hou.hscriptExpression("${SHOTPATH}")
                    scene_name = hou.hscriptExpression("${SCENENAME}")
                else:
                    node_name = "${OS}"

                    show_var = "${SHOW}"
                    seq_var = "${SEQ}"
                    shot_var = "${SHOT}"

                    shot_var = "${SHOW}.${SEQ}.${SHOT}"
                    shotpath_var = "${SHOTPATH}"
                    scene_name = "${SCENENAME}"

                print shot_var
                scene_name_default = scene_name
                shot_default = shot_var
                file_template_default = "`chs('shot_path')`/`chs('output_type')`/`chs('element_name')`/`chs('versionstr')`/`chs('shot')`.`chs('scene_name')`.`chs('element_name')`.`chs('versionstr')`.`chs('wedge_string')`.`chs('frame')`.`chs('file_type')`"

                # If target matches node typ in dict, then apply versioning
                print 'node type', node.type().name()
                if node.type().name() in self.output_types:
                    lookup = self.output_types[node.type().name()]
                    extension = lookup['extension']
                    print 'extension', extension
                    static_expression = lookup['static_expression']
                    out_parm_name = lookup['output']
                    # check if overide for default
                    file_template = file_template_default
                    if 'file_template' in lookup:
                        print "overiding file tempalte"
                        file_template = lookup['file_template']
                    print "file_template", file_template

                    try:
                        parm_group = node.parmTemplateGroup()
                        parm_folder = hou.FolderParmTemplate(
                            "folder", "Versioning")

                        parm_folder.addParmTemplate(hou.StringParmTemplate(
                            "element_name_template", "Element Name Template", 1, ["${OS}"]))

                        element_name_parm = hou.StringParmTemplate(
                            "element_name", "Element Name", 1, [node_name])

                        parm_folder.addParmTemplate(element_name_parm)

                        
                        parm_folder.addParmTemplate(hou.ToggleParmTemplate(
                            "auto_version", "Auto Version Set To Hip Version on Execute", 1))

                        parm_folder.addParmTemplate(
                            hou.IntParmTemplate("version_int", "Version", 1))

                        parm_folder.addParmTemplate(hou.StringParmTemplate(
                            "versionstr", "Version String", 1, [""]))

                        parm_folder.addParmTemplate(hou.StringParmTemplate(
                            "wedge_string", "Wedge String", 1, ["w`int(@wedgenum)`"]))

                        parm_folder.addParmTemplate(hou.StringParmTemplate(
                            "output_type", "Output Type", 1, [lookup['type_path']]))

                        parm_folder.addParmTemplate(hou.StringParmTemplate(
                            "shot", "Shot", 1, [shot_default]))

                        parm_folder.addParmTemplate(hou.MenuParmTemplate('location', 'Location', (
                            "submission_location", "cloud", "onsite"), ("Submission Location", "Cloud", "Onsite"), default_value=0))
                        # parm_folder.addParmTemplate(hou.MenuParmTemplate("location", "Location", menu_items=(["submission_location","cloud","onsite"]), menu_labels=(["Submission Location","Cloud","Onsite"]), default_value=0, icon_names=([]), item_generator_script="", item_generator_script_language=hou.scriptLanguage.Python, menu_type=hou.menuType.Normal, menu_use_token=False, is_button_strip=False, strip_uses_icons=False)

                        parm_folder.addParmTemplate(hou.StringParmTemplate(
                            "shot_path_template", "Shot Path Template", 1, ["${SHOTPATH}"]))

                        parm_folder.addParmTemplate(hou.StringParmTemplate(
                            "shot_path", "Shot Path", 1, [shotpath_var]))

                        parm_folder.addParmTemplate(hou.StringParmTemplate(
                            "scene_name", "Scene Name", 1, [scene_name_default]))

                        # default_expression=("hou.frame()"), default_expression_language=(hou.scriptLanguage.Python) ) )
                        parm_folder.addParmTemplate(
                            hou.StringParmTemplate("frame", "Frame", 1, ["$F4"]))
                        
                        parm_folder.addParmTemplate(hou.StringParmTemplate(
                            "file_type", "File Type", 1, [extension]))

                        parm_folder.addParmTemplate(hou.StringParmTemplate(
                            "file_template", "File Template", 1, [file_template]))
                        
                        if versiondb:
                            # if version db is selected then multiparms are created
                            parm_folder.addParmTemplate(hou.SeparatorParmTemplate("sepparm"))

                            # Code for parameter template
                            parm_folder.addParmTemplate(hou.StringParmTemplate("index_key_template", "Index Key Template", 1, default_value=(["`chs('element_name')`_`chs('wedge_string')`"]), naming_scheme=hou.parmNamingScheme.Base1, string_type=hou.stringParmType.Regular, menu_items=([]), menu_labels=([]), icon_names=([]), item_generator_script="", item_generator_script_language=hou.scriptLanguage.Python, menu_type=hou.menuType.Normal))
                            
                            # Code for parameter template
                            version_parm_folder = hou.FolderParmTemplate("versiondb0", "Version DB", folder_type=hou.folderType.MultiparmBlock, default_value=0, ends_tab_group=False)
                            callback_expr = \
                                """
# This allows versioning to be inherited by the multi parm db
import hou
import sys
import os

menu_path = os.environ['FIREHAWK_HOUDINI_TOOLS'] + '/scripts/modules'
sys.path.append(menu_path)
import firehawk_submit as firehawk_submit

node = hou.pwd()
parm = node.parm('versiondb0')

multiparm_count = parm.eval()
firehawk_submit.submit(node).multiparm_housecleaning( node, multiparm_count )
"""
                            version_parm_folder.setScriptCallbackLanguage(hou.scriptLanguage.Python)
                            version_parm_folder.setScriptCallback(callback_expr)



                            #hou_parm_template.addParmTemplate(hou_parm_template2)
                            # Code for parameter template
                            hou_parm_template2 = hou.StringParmTemplate("index_key#", "Index Key", 1, default_value=([""]), naming_scheme=hou.parmNamingScheme.Base1, string_type=hou.stringParmType.Regular, menu_items=([]), menu_labels=([]), icon_names=([]), item_generator_script="", item_generator_script_language=hou.scriptLanguage.Python, menu_type=hou.menuType.Normal)
                            
                            hou_parm_template2.setConditional(hou.parmCondType.DisableWhen, "{ 0 != 1 }")
                            hou_parm_template2.setJoinWithNext(True)

                            version_parm_folder.addParmTemplate(hou_parm_template2)
                            # Code for parameter template
                            hou_parm_template2 = hou.IntParmTemplate("version#", "Version", 1, default_value=([0]), min=0, max=10, min_is_strict=False, max_is_strict=False, naming_scheme=hou.parmNamingScheme.Base1, menu_items=([]), menu_labels=([]), icon_names=([]), item_generator_script="",   item_generator_script_language=hou.scriptLanguage.Python, menu_type=hou.menuType.Normal, menu_use_token=False)
                            version_parm_folder.addParmTemplate(hou_parm_template2)
                            
                            parm_group.append(parm_folder)
                            parm_group.append(version_parm_folder)
                            #parm_folder.addParmTemplate(
                            #hou_parm_template_group.append(hou_parm_template)
                            #hou_node.setParmTemplateGroup(hou_parm_template_group)
                        else:
                            parm_group.append(parm_folder)
                        node.setParmTemplateGroup(parm_group)

                        hou_parm = node.parm("version_int")
                        print "int hou_parm", hou_parm
                        hou_parm.lock(False)
                        hou_parm.setAutoscope(False)

                        if versiondb:
                            # set expression for version to look up db if enabled
                            hou_keyframe = hou.Keyframe()
                            hou_keyframe.setTime(0)
                            ver_expr = \
                                """
    # This allows versioning to be inherited by the multi parm db
    import hou
    import re

    node = hou.pwd()
    parm = hou.evaluatingParm()

    index_key = node.parm('index_key_template').eval()
    multiparm_index = node.userData('verdb_'+index_key)

    version = 0

    if multiparm_index is not None:
        multiparm_index = str(multiparm_index)
        version_parm = node.parm('version'+multiparm_index)
        if version_parm is not None:
            version = version_parm.eval()        

    return version
    """
                            hou_keyframe.setExpression(
                                ver_expr, hou.exprLanguage.Python)
                            hou_parm.setKeyframe(hou_keyframe)

                        hou_parm = node.parm("versionstr")
                        hou_parm.lock(False)
                        hou_parm.setAutoscope(False)
                        hou_keyframe = hou.StringKeyframe()
                        hou_keyframe.setTime(0)
                        ver_expr = \
                            """
# This returns the version as a padded string.
import hou
version = 'v'+str(hou.pwd().parm('version_int').eval()).zfill(3)
return version
"""
                        hou_keyframe.setExpression(
                            ver_expr, hou.exprLanguage.Python)
                        hou_parm.setKeyframe(hou_keyframe)

                        expr = \
                            """
# When multiple sites (cloud) are mounted over vpn, this allows tops to recognise if data exists in a particulr location.
# It means data can be submitted for generation or deleted from multiple locaitons,
# However generation should normally be executed by render nodes that exist at the same site through via a scheduler.
import hou
node = hou.pwd()
lookup = {'submission_location':'$PROD_ROOT', 'cloud':'$PROD_CLOUD_ROOT', 'onsite':'$PROD_ONSITE_ROOT'}
location = node.parm('location').evalAsString()
root = lookup[location]

template = root+'/$SHOW/$SEQ/$SHOT'
return template
"""

                        hou_parm = node.parm("shot_path_template")
                        hou_parm.lock(False)
                        hou_parm.setAutoscope(False)
                        hou_keyframe = hou.StringKeyframe()
                        hou_keyframe.setTime(0)
                        hou_keyframe.setExpression(
                            expr, hou.exprLanguage.Python)
                        hou_parm.setKeyframe(hou_keyframe)

                        parms_added = True
                    except:
                        parms_added = False

                    if static_expression:
                        hou_parm = node.parm("frame")
                        hou_parm.lock(False)
                        hou_parm.setAutoscope(False)
                        hou_keyframe = hou.StringKeyframe()
                        hou_keyframe.setTime(0)

                        if 'overrides' in lookup and 'frame' in lookup['overrides']:
                            print 'has override for static_expression'
                            hou_keyframe.setExpression(
                                lookup['overrides']['frame'], hou.exprLanguage.Python)
                        else:
                            hou_keyframe.setExpression("import hou"+'\n'+"node = hou.pwd()"+'\n'+"step = node.parm('f3').eval()"+'\n'+"if node.parm('trange').evalAsString() == 'off':" +
                                                       '\n'+"    value = 'static'"+'\n'+"elif step != 1:"+'\n'+"    value = '$FF'"+'\n'+"else:"+'\n'+"    value = '$F4'"+'\n'+"return value", hou.exprLanguage.Python)
                        # if node.parm('framegeneration').evalAsString() == '0':
                        hou_parm.setKeyframe(hou_keyframe)

                    # set defaults here if parms already exist and changes are made
                    node.parm("scene_name").set(scene_name_default)
                    node.parm("shot").set(shot_default)

                    element_name_template = node.parm(
                        "element_name_template").evalAsString()
                    try:
                        shot_path_template = hou.expandString(
                            node.parm("shot_path_template").evalAsString())
                        #element_name_template = node.parm("element_name_template").evalAsString()
                    except:
                        shot_path_template = shotpath_var

                    node.parm('element_name').set(element_name_template)
                    node.parm("shot_path").set(shot_path_template)
                    node.parm('file_template').set(file_template)

                    bake_template = False
                    replace_env_vars_for_tops = True
                    auto_version = node.parm("auto_version").eval()
                    print 'auto_version', auto_version

                    # if autoversion tickbox enabled, then update version to hip version on execute of tool.
                    if node.parm("auto_version").eval():
                        set_version = int(
                            hou.hscriptExpression('opdigits($VER)'))
                        print 'set version', set_version
                        node.parm("version_int").set(set_version)

                    if bake_template:
                        file_path_split = node.parm(
                            'file_template').unexpandedString()
                        file_path_split = file_path_split.replace(
                            "`chs('frame')`", "{{ frame }}")
                        file_path_split = file_path_split.replace(
                            "`chs('versionstr')`", "{{ versionstr }}")
                        file_path_split = file_path_split.replace(
                            "`chs('wedge_string')`", "{{ wedge_string }}")
                        file_path_split = file_path_split.replace(
                            "`chs('element_name')`", "{{ element_name }}")

                        # expand any values that we do not wish to be dynamic.
                        file_path = hou.expandString(file_path_split)
                        try:
                            file_path = file_path.replace(
                                "{{ frame }}", node.parm('frame').unexpandedString())
                        except:
                            file_path = file_path.replace(
                                "{{ frame }}", node.parm('frame').eval())
                        file_path = file_path.replace(
                            "{{ versionstr }}", "`chs('versionstr')`")
                        file_path = file_path.replace(
                            "{{ wedge_string }}", "`chs('wedge_string')`")
                        file_path = file_path.replace(
                            "{{ element_name }}", "`chs('element_name')`")

                        # overide, and use template
                    else:
                        file_path_split = node.parm(
                            'file_template').unexpandedString()
                        file_path_split = file_path_split.replace(
                            "`chs('frame')`", "{{ frame }}")
                        file_path_split = file_path_split.replace(
                            "`chs('element_name')`", "{{ element_name }}")
                        file_path = file_path_split
                        try:
                            file_path = file_path.replace(
                                "{{ frame }}", node.parm('frame').unexpandedString())
                        except:
                            file_path = file_path.replace(
                                "{{ frame }}", node.parm('frame').eval())

                        element_name = node.parm(
                            'element_name').unexpandedString()
                        if replace_env_vars_for_tops:
                            print 'replace environment vars in element name with @ lower case version attributes'
                            env_var_matches = re.findall(
                                r'\$\{.*?\}', element_name)
                            print 'element_name', element_name
                            print 'env_var_matches', env_var_matches
                            ignore = ['${OS}']
                            for match in env_var_matches:
                                if match not in ignore:
                                    replacement = match.strip('${}').lower()
                                    element_name = element_name.replace(
                                        match, '`@'+replacement+'`')
                        else:
                            print 'will preserve environment vars in element name'

                        # we bake the element name into the path so that copy of a node will not break refs in the event of a duplicate node existing in the target network to copy to.
                        # element name cannot be ${OS} because if it changes the reader will not function from the template parms schema.
                        file_path = file_path.replace(
                            "{{ element_name }}", element_name)

                    # We bake the full definition of the cached output string into the output file string, except for the frame variable, version and wedge which remains as hscript/chanel refs.
                    # this provides a safe means for copying cache nodes into other scenes without breaking references.
                    # references should be updated on write via a prerender script executing this function to ensure $VER is updated to a current value.

                    print 'out path', file_path

                    node.parm(out_parm_name).set(file_path)
                    print "add defs"

    def onScheduleVersioning(self, work_item=None):
        # This should only be called within the scheduler.
        ### version tracking: write version attr before json file is created.
        # requires hou
        # ensure this is located just before self.createJobDirsAndSerializeWorkItems(work_item)
        # also ensure the current index_key string exists as a top attribute.
        
        # example externals for scheduler import.
        ### Firehawk versioning alterations
        # import hou
        # menu_path = os.environ['FIREHAWK_HOUDINI_TOOLS'] + '/scripts/modules'
        # sys.path.append(menu_path)
        # import firehawk_submit as firehawk_submit
        # ###
        
        # example in onschedule callback
        # ### firehawk on schedule version handling
        # print "on schedule version handling", work_item
        # rop_path = work_item.data.stringData('rop', 0)
        # hou_node = hou.node(rop_path)
        # print "hou_node", hou_node
        # firehawk_submit.submit(hou_node).onScheduleVersioning(work_item)
        # ### end firehawk on schedule version handling
        print "onScheduleVersioning start workitem:", work_item
        if work_item:
            print "set int version"
            hip_path = work_item.data.stringData('hip', 0)
            rop_path = work_item.data.stringData('rop', 0)
            
            print "hip_path", hip_path

            hip_path = os.path.split(hip_path)[1]
            print "hip_path split", hip_path
            version = int(re.search(r"_v([0-9][0-9][0-9])?.?[0-9]?[0-9]?[0-9]_", hip_path).group(1))
            print "setting version for workitem to:", version
            work_item.data.setInt("version", version, 0)
            
            all_attribs = work_item.data.stringDataArray('wedgeattribs')
            print "all_attribs", all_attribs
            all_attribs.append('version')
            print "set string array"
            work_item.data.setStringArray('wedgeattribs', sorted(all_attribs))
            parm_path = rop_path+'/version_int'
            print 'set string channel', parm_path
            work_item.data.setString("{}channel".format('version'), parm_path, 0)
            
            ### Set data on the node db multiparm, uses hou.

            hou_node = hou.node(rop_path)

            # ensure callback exists on node of work item to detect changes to parms and sync dictionary
            self.add_version_db_callback(hou_node)

            index_key = work_item.data.stringData('index_key', 0)
            print "index_key", index_key
            
            # multiparm index is the string to append to parm names to retrive the correct multiparm instance
            
            def sorted_nicely( l ): 
                convert = lambda text: int(text) if text.isdigit() else text 
                alphanum_key = lambda key: [ convert(c) for c in re.split('([0-9]+)', key) ] 
                return sorted(l, key = alphanum_key)

            multiparm_index = hou_node.userData('verdb_'+index_key)
            # if the index doesn't exist in the userDataDict, then a new parm instance must be created in the live hip file. userData should be updated by the parameter callback.
            if multiparm_index is None:
                
                verdb = hou_node.userDataDict()
                
                new_index_key = 'verdb_'+index_key
                # sort list by value indices, and add current item
                verdb_list = sorted(verdb, key=verdb.get) + [new_index_key]
                # clean list if not verdb
                verdb_list = [ x for x in verdb_list if 'verdb_' in x ]
                print 'sorted indexes by current multiparm indices', verdb_list

                # if new entry isn't last, then insert#
                verdb_sorted = sorted_nicely( verdb_list )
                append = True
                
                # determine if insertion is needed
                for idx, val in enumerate( verdb_sorted ):
                    # find entry in list to match, then get index for next item, since current item has no index
                    if val == new_index_key and idx < len(verdb_sorted)-1:
                        next_index_key = verdb_sorted[idx+1]
                        next_index_int = hou_node.userData(next_index_key)
                        # validate a dicitonary entry exists for the next item
                        if next_index_int is not None:
                            next_index_int = int(next_index_int)
                        else:
                            hou.ui.displayMessage('no dict entry for next index: '+next_index_key)

                        hou_node.parm('versiondb0').insertMultiParmInstance(int(next_index_int)-1)
                        multiparm_index = int(next_index_int)
                        append = False
                        break
                        
                if append:
                    # new index
                    multiparm_index = int(hou_node.parm('versiondb0').eval()+1)
                    # increment count
                    hou_node.parm('versiondb0').insertMultiParmInstance(int(multiparm_index-1))
                    # set key string on parm
                    
                if multiparm_index is not None:
                    hou_node.parm('index_key'+str(multiparm_index)).set(index_key)
            else:
                # if multiparm_index exists, ensure it is an int
                multiparm_index = int(multiparm_index)
            
            version_parm_name = 'version' + str(multiparm_index)
            print "eval current version"
            current_multiparm_version = hou_node.parm(version_parm_name).eval()

            if int(current_multiparm_version) != int(version):
                print 'update', version_parm_name, "no match- current_multiparm_version:", current_multiparm_version, "current hip version:", version
                print "setting version on multiparm", version_parm_name, version
                hou_node.parm(version_parm_name).set(version)
            
            print "### end multiversion db block ###"
            # ### end dynamic version db ###