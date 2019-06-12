
debug = False
if debug:
    boto3.set_stream_logger('')


import os
import sys

sys.path.append('/usr/lib64/python2.7/site-packages')
sys.path.append('/home/deadlineuser/.local/lib/python2.7/site-packages')
sys.path.append('/usr/lib/python2.7/site-packages')

from awscli.clidriver import create_clidriver
import boto3

class syncfile():
  def __init__(self, fullpath='', bucketname=''):
    self.fullpath = fullpath
    
    self.dirname = os.path.split(self.fullpath)[0]
    self.filename = os.path.split(self.fullpath)[1]
    
    self.bucketname = bucketname
    self.bucketdirname = 's3://' + self.bucketname + os.path.split(self.fullpath)[0]
    
    self.s3_client = boto3.client('s3')
    self.s3_resource = boto3.resource('s3')
    self.s3_client_result = None

    self.force = False
    self.pushed = False
    self.pulled = False

  def aws_cli(self, *cmd):
    self.old_env = dict(os.environ)
    try:

        # Environment
        self.env = os.environ.copy()
        self.env['LC_CTYPE'] = u'en_US.UTF'
        os.environ.update(self.env)

        # Run awscli in the same process
        exit_code = create_clidriver().main(*cmd)

        # Deal with problems
        if exit_code > 0:
            raise RuntimeError('AWS CLI exited with code {}'.format(exit_code))
    finally:
        os.environ.clear()
        os.environ.update(self.old_env)

  def local_push(self):
    if self.pushed==False:
      print 'upload', self.fullpath
      if self.force:
        self.s3_client_result = self.s3_client.upload_file(self.fullpath, self.bucketname, self.fullpath)
        # upload to s3 with boto is prefereable to the cli.  However the cli proivdes the sync function below which will only transfer if the files don't match which is a better default behaviour.
      else:
        self.cli_operation = self.aws_cli(['s3', 'sync', self.dirname, self.bucketdirname, '--exclude', '*', '--include', self.filename])
        #print self.cli_operation
    self.pushed = True

  def local_pull(self):
    if self.pulled==False:
      print 'download', self.fullpath
      if self.force:
        self.s3_client_result = self.s3_client.download_file(self.bucketname, self.fullpath, self.fullpath)
      else:
        self.cli_operation = self.aws_cli(['s3', 'sync', self.bucketdirname, self.dirname, '--exclude', '*', '--include', self.filename])
        #print self.cli_operation
    # upload to s3
    self.pulled = True
