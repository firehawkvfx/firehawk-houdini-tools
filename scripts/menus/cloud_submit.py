#!/usr/bin/python

import hou
import sys
import re
import subprocess
import os
import errno

#####


# Copy function with progress display using a callback function.
# The callback function used here mimics the output of
# 'rsync --progress'.

# Copyright (C) 2012 Thomas Hipp

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

# import datetime
# import os

# def copy(src, dst, callback=None):
#   blksize = 1048576 # 1MiB
#   try:
#     s = open(src, 'rb')
#     d = open(dst, 'wb')
#   except (KeyboardInterrupt, Exception) as e:
#     if 's' in locals():
#         s.close()
#     if 'd' in locals():
#         d.close()
#     raise
#   try:
#     total = os.stat(src).st_size
#     pos = 0
#     start_elapsed = datetime.datetime.now()
#     start_update = datetime.datetime.now()
#     while True:
#       buf = s.read(blksize)
#       bytes_written = d.write(buf)
#       end = datetime.datetime.now()
#       print "bytes_written", bytes_written
#       pos += 0
#       #pos += bytes_written
#       diff = end - start_update
#       if callback and diff.total_seconds() >= 0.2:
#         callback(pos, total, end - start_elapsed)
#         start_update = datetime.datetime.now()
#       if bytes_written < len(buf) or bytes_written == 0:
#         break
#   except (KeyboardInterrupt, Exception) as e:
#     s.close()
#     d.close()
#     raise
#   else:
#     callback(total, total, end - start_elapsed)
#     s.close()
#     d.close()

# # def tmstr(t):
# #   days, rest = divmod(t, 86400)
# #   hours, rest = divmod(rest, 3600)
# #   minutes, seconds = divmod(rest, 60)
# #   return '{0:4d}:{1:02d}:{2:02d}'.format(int(days * 24 + hours), int(minutes), round(seconds))

# mod = 101
# speed = [0] * mod
# index = 0

# def progress(pos, total, elapsed):
#   global speed
#   global index
#   global mod
#   elapsed_ = elapsed.total_seconds()
#   speed[index % mod] = pos / elapsed_
#   index = (index + 1) % mod
#   out = '{0:12} {1:4.0%} {2:7.2f}{unit}B/s {3}'
#   unit = ('Mi', 1048576) if total > 999999 else ('ki', 1024)
#   # complete
#   if pos == total:
#     print "done"
#     #print(out.format(pos, pos / total, sum(speed) / mod / unit[1], tmstr(elapsed_), unit=unit[0]))
#   # in progress
#   else:
#     print "copying"
#     #print(out.format(pos, pos / total, sum(speed) / mod / unit[1], tmstr((total - pos) / sum(speed) * mod), unit=unit[0]), end='')
#     print('\r')

# #######

# # def progress(bytescopied):
# #   print "update", bytescopied

# def copyfileobj(fsrc, fdst, callback, length=16*1024):
#   copied = 0
#   while True:
#     buf = fsrc.read(length)
#     if not buf:
#       break
#     fdst.write(buf)
#     copied += len(buf)
#     callback(copied)

class submit():
  def __init__(self, node=''):
    self.node = node
    print "Submit", self.node.path()

    if hasattr(node.getPDGNode(), 'dirty'):
      self.node.getPDGNode().dirty(True)

    self.hip_path = hou.hipFile.path()
    hou.hipFile.save(self.hip_path)

    # self.hip_path_cloud = re.sub('^\/prod\/?', '/tmp/', self.hip_path)
    # print "Source hip", self.hip_path
    
    # print "Copy hip file to", self.hip_path_cloud
    
    # ensure path exists to write
    # if not os.path.exists(os.path.dirname(self.hip_path_cloud)):
    #     try:
    #         os.makedirs(os.path.dirname(self.hip_path_cloud))
    #     except OSError as exc: # Guard against race condition
    #         if exc.errno != errno.EEXIST:
    #             raise

    # copy(self.hip_path, self.hip_path_cloud, progress)

    # self.source_file = open(self.hip_path,"r")
    # self.dest_file = open(self.hip_path_cloud,"w+")

    # copyfileobj(self.source_file, self.dest_file, progress)


    # self.source_file.close()
    # self.dest_file.close()

    #subprocess.call(["rsync", "-avzh", "DJStatic", "username@website"])
  def assign_preflight(self):
    print "assign preflight node", self.node.path()

  def cook(self):
    node.getPDGNode().cook(True)
    if hasattr(node.getPDGNode(), 'cook'):
      node.getPDGNode().cook(True)
    else:
      hou.ui.displayMessage("Failed to cook, try initiliasing the node first with a standard cook / generate.")

    #def local_push(self):