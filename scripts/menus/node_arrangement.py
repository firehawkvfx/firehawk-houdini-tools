selected_nodes = hou.selectedNodes()

if len(selected_nodes):
    y_positions = [ x.position()[1] for x in selected_nodes ]
    x_positions = [ x.position()[0] for x in selected_nodes ]
    
    selected_miny = min( y_positions )
    selected_miny_node = [ x for x in selected_nodes if x.position()[1] == selected_miny ][0]

    selected_minx = min( x_positions )
    selected_minx_node = [ x for x in selected_nodes if x.position()[0] == selected_minx ][0]
    
    selected_maxy = max( y_positions )
    selected_maxy_node = [ x for x in selected_nodes if x.position()[1] == selected_maxy ][0]

    selected_maxx = max( x_positions )
    selected_maxx_node = [ x for x in selected_nodes if x.position()[0] == selected_maxx ][0]
    
    nodes_to_arrange = [x for x in selected_miny_node.inputAncestors() if x in selected_nodes]
    nodes_to_arrange.insert( 0, selected_miny_node )
    
    def node_and_ancestors(node):
        list = node.ancestors()
        list.insert( 0, node )
        return list  
    
    def get_y(node):
        return node.position()[1]
    
    def sort_y(node_list):
        node_list.sort(key=get_y)
        return node_list
        
    def stack_nodes_downward_from_top_node(node_list, distance):
        node_list.reverse() # order from top most ancestor to bottom
        node_list.remove( selected_maxy_node )
        node_list.insert( 0, selected_maxy_node ) # ensure top most node stays at top
        
        offset = 0
        for node in node_list:
            x_pos = node.position()[0]
            y_pos = node_list[0].position()[1] - offset * distance
            new_position = hou.Vector2( (x_pos, y_pos) )
            node.setPosition( new_position )
            offset += 1

    def stack_nodes_upward_from_bottom_node(node_list, distance):
        node_list.remove( selected_maxy_node )
        node_list.append( selected_maxy_node ) # ensure top most node stays at top
        
        offset = 0
        for node in node_list:
            x_pos = node.position()[0]
            y_pos = node_list[0].position()[1] + offset * distance
            new_position = hou.Vector2( (x_pos, y_pos) )
            node.setPosition( new_position )
            offset += 1
            
    def align_left(node_list):
        for node in node_list:
            x_pos = selected_minx
            y_pos = node.position()[1]
            new_position = hou.Vector2( (x_pos, y_pos) )
            node.setPosition( new_position )

    def align_right(node_list):
        for node in node_list:
            x_pos = selected_maxx
            y_pos = node.position()[1]
            new_position = hou.Vector2( (x_pos, y_pos) )
            node.setPosition( new_position )
        
    #stack_nodes_downward_from_top_node(nodes_to_arrange, 1.0)
    #stack_nodes_upward_from_bottom_node(nodes_to_arrange, 1.0)
    #align_left(nodes_to_arrange)
    #align_right(nodes_to_arrange)        