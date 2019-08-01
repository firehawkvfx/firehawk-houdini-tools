            ### version tracking: write version attr before json file is created, and set version on multiparm for rop.

            # requires hou
            # ensure this is located just before self.createJobDirsAndSerializeWorkItems(work_item) in a scheduler.  Tested with the local scheduler and deadline.

            # Provided the correct multiparm and callback is initialised on a rop, this will set the version on the multiparm when the workitem is scheduled based on the key template.
            # ensure the node callback from firehawk_submit.py is functioning as this is required for house cleaning of the userDataDict to lookup the versions.
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