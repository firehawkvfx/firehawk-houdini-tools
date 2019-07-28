#!/usr/bin/python

import hou
import pdg
import sys
import re
import subprocess
import os
import errno
import numpy as np
import datetime

#####

class submit():
  def __init__(self, node=''):
    self.node = node
    self.selected_nodes = []
    self.pdg_node = ''
    self.parent = self.node.parent()

    self.preflight_node = None
    self.preflight_path = None
    self.parm_group = None
    self.found_folder = None
    self.graph_context = None

    self.preflight_status = None

    self.hip_path = hou.hipFile.path()
    self.hip_dirname = os.path.split(self.hip_path)[0]
    self.hip_basename = hou.hipFile.basename().strip('.hip')
    self.hip_path_submission = None

    self.source_top_nodes = []
    self.added_workitems = []
    self.added_dependencies = []

  def assign_preflight(self):
    print "assign preflight node", self.node.path(), "on topnet", self.parent
    
    # get the template group for the parent top node.
    self.parm_group = self.parent.parmTemplateGroup()
    print "Create folder parm template"

    self.parm_folder = hou.FolderParmTemplate("folder", "Firehawk")
    self.parm_folder.addParmTemplate(hou.StringParmTemplate("preflight_node", "Preflight Node", 1, [""], string_type=hou.stringParmType.NodeReference))     
    
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

    self.parent.parm("preflight_node").set(self.node.path())

  def cook(self):
    # all preview switches set to 0.  switch nodes starting with name "preview" are usefull for interactivve session testing only, but will revert to input 0 upon farm submission.
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

      timestamp_submission = True

      self.submit_name = self.hip_path

      if timestamp_submission:
        datetime_object = datetime.datetime.now()
        print(datetime_object)
        timestampStr = datetime_object.strftime("%Y-%m-%d.%H-%M-%S-%f")
        print('Current Timestamp : ', timestampStr)

        self.submit_name = "{dir}/{base}.{date}.hip".format(dir=self.hip_dirname, base=self.hip_basename, date=timestampStr)

      hou.hipFile.save(self.submit_name)

      if self.preflight_node:
        print "preflight node path is", self.preflight_node.path()
        ### refresh workitems on the preflight node ###
        self.preflight_node.executeGraph(False, False, False, True)

        if hasattr(self.preflight_node.getPDGNode(), 'cook'):
          print "cooking preflight", self.preflight_node.path()
          

          def cook_done(event):
            if self.preflight_status=='cooking':
              print "event", event.node, event.message
              self.preflight_status=='done'
              ### remove handler since the main job is about to execute, and we dont need this anymore. ###
              self.graph_context.removeEventHandler(self.handler)

              ### save after preflight ###
              hou.hipFile.save(self.submit_name)
              ### refresh workitems for main job node ###
              self.node.executeGraph(False, False, False, True)

              if hasattr(self.node.getPDGNode(), 'cook'):
                ### cook main job ###
                self.node.getPDGNode().cook(False)
              else:
                hou.ui.displayMessage("Failed to cook, try initiliasing the node first with a standard cook / generate.")
              
              # Save again with restored original name.
              hou.hipFile.save(self.hip_path)
            else:
              print 'error preflight_status is not "cooking", this function should not be called', self.preflight_status

          ### setup handler before executing preflight ###
          self.graph_context = self.preflight_node.getPDGGraphContext()
          self.handler = self.graph_context.addEventHandler(cook_done, pdg.EventType.CookComplete)
          ### cook preflight ###
          self.preflight_status='cooking'
          self.preflight_node.getPDGNode().cook(False)

        else:
          #print "dir(self.preflight_node.getPDGNode())", dir(self.preflight_node.getPDGNode())
          hou.ui.displayMessage("Preflight Failed to cook: Node wasn't initialised / cook method not available on this node.")
      else:
        # no preflight
        self.node.executeGraph(False, False, False, True)
        if hasattr(self.node.getPDGNode(), 'cook'):
          self.node.getPDGNode().cook(False)
        else:
          hou.ui.displayMessage("Failed to cook, try initiliasing the node first with a standard cook / generate.")
        # save again with original name.
        hou.hipFile.save(self.hip_path)

    


  def dirty_upstream_source_nodes(self):
    # this will generate the selected workitems
    print "Dirty Upstream Source Nodes"
    self.pdg_node = self.node.getPDGNode()
    
    self.node.executeGraph(False, False, False, True)

    self.source_top_nodes = []
    self.added_workitems = []
    self.added_dependencies = []
    
    def append_workitems(node):
        if len(node.workItems) > 0:
            for workitem in node.workItems:
                if workitem not in self.added_workitems:
                    self.added_workitems += [workitem]

    def append_dependencies(workitem):
        self.added_dependencies += [workitem]
        if len(workitem.dependencies) > 0:
            for dependency in workitem.dependencies:
                if dependency not in self.added_workitems:
                    self.added_workitems += [dependency]

    append_workitems(self.pdg_node)
    
    for workitem in self.added_workitems:
        append_dependencies(workitem)
    
    diff_list = np.setdiff1d(self.added_workitems,self.added_dependencies)
    
    while len(diff_list) > 0:
        for workitem in diff_list:
            append_dependencies(workitem)
        diff_list = np.setdiff1d(self.added_workitems,self.added_dependencies)
    
    for workitem in self.added_workitems:
        if len(workitem.dependencies) == 0:
            if workitem.node not in self.source_top_nodes:
                self.source_top_nodes += [workitem.node]
    
    for source_top_node in self.source_top_nodes:
        source_top_node.dirty(False)
        print "Dirtied source_top_node", source_top_node.name

  def update_rop_output_paths_for_selected_nodes(self, kwargs={}):
    print "Update Rop Output Paths for Selected SOP/TOP Nodes. Note: currently not handling @attributes $attributes in element names correctly."
    self.selected_nodes = kwargs['items']

    for node in self.selected_nodes:
        print 'Name', node.name()
        if node.type().name()=='ropfetch':
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
            
            output_types = {
                'opengl':{'output':'picture','extension':'jpg','type_path':'flipbook/frames','static_expression':False},
                'rop_geometry':{'output':'sopoutput','extension':'bgeo.sc','type_path':'cache','static_expression':True},
                'vellumio':{'output':'file','extension':'bgeo.sc','type_path':'cache','static_expression':False},
                'rop_alembic':{'output':'filename','extension':'abc','type_path':'cache','static_expression':True},
                'file':{'output':'file','extension':'bgeo.sc','type_path':'cache','static_expression':True},
                'ropcomposite':{'output':'copoutput','extension':'exr','type_path':'flipbook/frames','static_expression':True},
                'ropmantra':{
                    'output':'vm_picture',
                    'extension':'exr',
                    'type_path':'render/frames',
                    'static_expression':True,
                    'overrides':{
                        'frame':"import hou"+'\n'+"value = '$F4'"+'\n'+"return value"
                    }
                },
                'ifd':{
                    'output':'vm_picture',
                    'extension':'exr',
                    'type_path':'render/frames',
                    'static_expression':True,
                    'overrides':{
                        'frame':"import hou"+'\n'+"value = '$F4'"+'\n'+"return value"
                    }
                },
                'ffmpegencodevideo':{
                    'output':'outputfilename',
                    'extension':'mp4',
                    'type_path':'flipbook/videos',
                    'static_expression':False,
                    'file_template':"`chs('shot_path')`/`chs('output_type')`/`chs('element_name')`/`chs('versionstr')`/`chs('shot')`.`chs('element_name')`.`chs('versionstr')`.`chs('wedge_string')`.`chs('file_type')`",
                    'overrides':{
                        'expr':'"{ffmpeg}" -y -r {frames_per_sec}/1 -f concat -safe 0 -apply_trc iec61966_2_1 -i "{frame_list_file}" -c:v libx264 -b:v 10M -vf "fps={frames_per_sec},format=yuv420p" -movflags faststart "{output_file}"'
                    }
                },
            }
            
            # If target matches node typ in dict, then apply versioning
            print 'node type', node.type().name()
            if node.type().name() in output_types:
                lookup = output_types[node.type().name()]
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
                    parm_folder = hou.FolderParmTemplate("folder", "Versioning")
                    #parm_folder.addParmTemplate(hou.StringParmTemplate("element_name_template", "Element Name Template", 1, ["${OS}"]))
                    parm_folder.addParmTemplate(hou.StringParmTemplate("element_name_template", "Element Name Template", 1, ["${OS}"]))

                    parm_folder.addParmTemplate(hou.StringParmTemplate("element_name", "Element Name", 1, [node_name]))
                    
                    parm_folder.addParmTemplate(hou.ToggleParmTemplate("auto_version", "Auto Version Set To Hip Version on Execute", 1))
                    parm_folder.addParmTemplate(hou.IntParmTemplate("version_int", "Version", 1))
                    parm_folder.addParmTemplate(hou.StringParmTemplate("versionstr", "Version String", 1, [""]))
                    
                    parm_folder.addParmTemplate(hou.StringParmTemplate("wedge_string", "Wedge String", 1, ["w`@wedgenum`"]))
                    
                    parm_folder.addParmTemplate(hou.StringParmTemplate("output_type", "Output Type", 1, [lookup['type_path']]))
                    
                    parm_folder.addParmTemplate(hou.StringParmTemplate("shot", "Shot", 1, [shot_default]))
                    
                    parm_folder.addParmTemplate(hou.MenuParmTemplate('location', 'Location', ("submission_location","cloud","onsite"), ("Submission Location","Cloud","Onsite"), default_value=0))
                    #parm_folder.addParmTemplate(hou.MenuParmTemplate("location", "Location", menu_items=(["submission_location","cloud","onsite"]), menu_labels=(["Submission Location","Cloud","Onsite"]), default_value=0, icon_names=([]), item_generator_script="", item_generator_script_language=hou.scriptLanguage.Python, menu_type=hou.menuType.Normal, menu_use_token=False, is_button_strip=False, strip_uses_icons=False)
            
                    parm_folder.addParmTemplate(hou.StringParmTemplate("shot_path_template", "Shot Path Template", 1, ["${SHOTPATH}"]))
                    
                    parm_folder.addParmTemplate(hou.StringParmTemplate("shot_path", "Shot Path", 1, [shotpath_var]))
                    
                    parm_folder.addParmTemplate(hou.StringParmTemplate("scene_name", "Scene Name", 1, [scene_name_default]))
                    
                    
                    #default_expression=("hou.frame()"), default_expression_language=(hou.scriptLanguage.Python) ) )
                    parm_folder.addParmTemplate(hou.StringParmTemplate("frame", "Frame", 1, ["$F4"]))
                    parm_folder.addParmTemplate(hou.StringParmTemplate("file_type", "File Type", 1, [extension]))
                    
                    
                    parm_folder.addParmTemplate(hou.StringParmTemplate("file_template", "File Template", 1, [file_template]))
                    #parm_folder.addParmTemplate(hou.FloatParmTemplate("amp", "Amp", 2))
                        
                    parm_group.append(parm_folder)
                    node.setParmTemplateGroup(parm_group)
                    
                    hou_parm = node.parm("versionstr")
                    hou_parm.lock(False)
                    hou_parm.setAutoscope(False)
                    hou_keyframe = hou.StringKeyframe()
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
                    hou_keyframe.setExpression(ver_expr, hou.exprLanguage.Python)
                    hou_parm.setKeyframe(hou_keyframe)

                    expr = \
"""
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
                    hou_keyframe.setExpression(expr, hou.exprLanguage.Python)
                    hou_parm.setKeyframe(hou_keyframe)
                    
                    
                    parms_added=True
                except:
                    parms_added=False
                
                if static_expression:
                    hou_parm = node.parm("frame")
                    hou_parm.lock(False)
                    hou_parm.setAutoscope(False)
                    hou_keyframe = hou.StringKeyframe()
                    hou_keyframe.setTime(0)
                    
                    if 'overrides' in lookup and 'frame' in lookup['overrides']:
                        print 'has override for static_expression'
                        hou_keyframe.setExpression(lookup['overrides']['frame'], hou.exprLanguage.Python)
                    else:
                        hou_keyframe.setExpression("import hou"+'\n'+"node = hou.pwd()"+'\n'+"step = node.parm('f3').eval()"+'\n'+"if node.parm('trange').evalAsString() == 'off':"+'\n'+"    value = 'static'"+'\n'+"elif step != 1:"+'\n'+"    value = '$FF'"+'\n'+"else:"+'\n'+"    value = '$F4'"+'\n'+"return value", hou.exprLanguage.Python)
                    # if node.parm('framegeneration').evalAsString() == '0':
                    hou_parm.setKeyframe(hou_keyframe)  
                    
                #set defaults here if parms already exist and changes are made
                node.parm("scene_name").set(scene_name_default)
                node.parm("shot").set(shot_default)
                
                element_name_template = node.parm("element_name_template").evalAsString()
                try:
                    shot_path_template = hou.expandString( node.parm("shot_path_template").evalAsString() )
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
                
                #if autoversion tickbox enabled, then update version to hip version on execute of tool.
                if node.parm("auto_version").eval():
                    set_version = int(hou.hscriptExpression('opdigits($VER)'))
                    print 'set version', set_version
                    node.parm("version_int").set( set_version )
                    
                if bake_template:
                    file_path_split = node.parm('file_template').unexpandedString()
                    file_path_split = file_path_split.replace("`chs('frame')`", "{{ frame }}")
                    file_path_split = file_path_split.replace("`chs('versionstr')`", "{{ versionstr }}")
                    file_path_split = file_path_split.replace("`chs('wedge_string')`", "{{ wedge_string }}")
                    file_path_split = file_path_split.replace("`chs('element_name')`", "{{ element_name }}")
                    
                    # expand any values that we do not wish to be dynamic.
                    file_path = hou.expandString(file_path_split)
                    try:
                        file_path = file_path.replace("{{ frame }}", node.parm('frame').unexpandedString())
                    except:
                        file_path = file_path.replace("{{ frame }}", node.parm('frame').eval())
                    file_path = file_path.replace("{{ versionstr }}", "`chs('versionstr')`")
                    file_path = file_path.replace("{{ wedge_string }}", "`chs('wedge_string')`")
                    file_path = file_path.replace("{{ element_name }}", "`chs('element_name')`")
                    
                    # overide, and use template
                else:
                    file_path_split = node.parm('file_template').unexpandedString()
                    file_path_split = file_path_split.replace("`chs('frame')`", "{{ frame }}")
                    file_path_split = file_path_split.replace("`chs('element_name')`", "{{ element_name }}")
                    file_path = file_path_split
                    try:
                        file_path = file_path.replace("{{ frame }}", node.parm('frame').unexpandedString())
                    except:
                        file_path = file_path.replace("{{ frame }}", node.parm('frame').eval())
                    
                    element_name = node.parm('element_name').unexpandedString()
                    if replace_env_vars_for_tops:
                        print 'replace environment vars in element name with @ lower case version attributes'
                        env_var_matches = re.findall(r'\$\{.*?\}', element_name)
                        print 'element_name', element_name
                        print 'env_var_matches', env_var_matches
                        ignore = ['${OS}']
                        for match in env_var_matches:
                            if match not in ignore:
                                replacement = match.strip('${}').lower()
                                element_name = element_name.replace(match, '`@'+replacement+'`')
                    else:
                        print 'will preserve environment vars in element name'
                    
                    # we bake the element name into the path so that copy of a node will not break refs in the event of a duplicate node existing in the target network to copy to.
                    # element name cannot be ${OS} because if it changes the reader will not function from the template parms schema.
                    file_path = file_path.replace("{{ element_name }}", element_name)
                
                # We bake the full definition of the cached output string into the output file string, except for the frame variable, version and wedge which remains as hscript/chanel refs.
                # this provides a safe means for copying cache nodes into other scenes without breaking references.
                # references should be updated on write via a prerender script executing this function to ensure $VER is updated to a current value.
                
                print 'out path', file_path
                
                node.parm(out_parm_name).set(file_path)

                def update_index(node, index_int):
                    index_key_parm_name = 'index_key' + str(index_int)
                    #print "update node", node, 'index_int', index_int, 'index_key_parm_name', index_key_parm_name
                    index_key_parm = node.parm(index_key_parm_name)
                    #print 'update index from parm', index_key_parm
                    index_key = index_key_parm.eval()
                    #version = node.parm('version' + str(index_int) ).eval()
                    node.setUserData( 'verdb_'+index_key, str(index_int) )
                    #print "Changed parm index_key", index_key, "index_int", index_int

                # ensure parameter callbacks exists
                def parm_changed(node, event_type, **kwargs):
                  
                  parm_tuple=kwargs['parm_tuple']

                  if parm_tuple is None:
                    hou.ui.displayMessage("Too many parms were changed.  callback hasn't been designed to handle this yet, changes may be needed in cloud_submit.py to handle this")
                  
                  name = parm_tuple.name()

                  # if a key has changed
                  is_multiparm = parm_tuple.isMultiParmInstance()

                  if is_multiparm and 'index_key' in name:
                    
                    if len(parm_tuple.eval()) > 1:
                      hou.ui.displayMessage("multiple items in tuple, changes may be needed in cloud_submit.py to handle this")
                    
                    index_int = next(re.finditer(r'\d+$', name)).group(0)
                    
                    print 'index_key in name', name, 'update', index_int
                    update_index(node, index_int)

                  # if multiparm instance count has changed, update all and remove any missing.
                  if 'versiondb0' in name:
                    multiparm_count = parm_tuple.eval()[0]
                    print "Total parms changed.  validate and clean out old dict. total parms:", multiparm_count
                    index_keys = []
                    for index_int in range(1, int(multiparm_count)+1):
                      index_key_parm_name = 'index_key'+str(index_int)
                      index_key_parm = node.parm(index_key_parm_name)
                      print 'update', index_key_parm.name()
                      index_key = index_key_parm.eval()
                      index_keys.append('verdb_'+index_key)
                      print 'update index', index_int, 'node', node
                      update_index(node, index_int)
                    
                    # first all items in dict will be checked for existance on node.  if they dont exist they will be destroyed on the dict.
                    user_data_total = 0
                    
                    keys_to_destroy = []
                    for index_key, value in node.userDataDict().items(): 
                      if index_key not in index_keys and 'verdb_' in index_key:
                        print "node missing key", index_key, ":", value, 'will remove'
                        keys_to_destroy.append(index_key)
                      else:
                        user_data_total += 1
                    
                    if len(keys_to_destroy) > 0:
                      for index_key in keys_to_destroy:
                        node.destroyUserData(index_key)
                        print "destroyed key", index_key
                      

                    # all lookups and validation needs to double check the data is correct.  if incorrect, trigger cleanup.
                    # if number of entries dont match, trigger cleanup. this can occur if a wedge is entered in as an index manually, and then altered. we locked parms to avoid this.
                    # new indexes should be automated.
                    



                
                #remove callback to replace
                #removeEventCallback((hou.nodeEventType.ParmTupleChanged, ), parm_changed) 
                
                #add callback
                node.addEventCallback((hou.nodeEventType.ParmTupleChanged, ), parm_changed)
