#!/usr/bin/python

import os

class mounts():
    def __init__(self):
        mount_paths = {}

    def get_mounts(self):
        mount_paths = {}
        for l in file('/proc/mounts'):
            if l[0] == '/':
                l_split = l.split()
                mount_paths[l_split[1]] = l_split[0]
            elif 'nfs4' in l:
                l_split = l.split()
                mount_paths[l_split[1]] = l_split[0]
        return mount_paths

    def check_mounted(self, path):
        mounted = False
        if path in self.get_mounts():
            mounted = True
        return mounted