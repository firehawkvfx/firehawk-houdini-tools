# Syncronise work items to or from s3.

# Example
# python /home/deadlineuser/houdini17.5/scripts/s3_sync/s3_sync_shell.py --file "/prod/tst/s3sync/upload/cache/sphere/v014/tst.s3sync.upload.uploadtest.sphere.v014.w0.*.bgeo.sc" --direction "push" --bucket "man.firehawkfilm.com"
# Changed files will take precedence
# If no data has changed, then no download/upload will take place.
# You must have aws cli installed and run aws configure to setup with your secret key.

import os, sys, argparse

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--file', type=str, help='file path')
parser.add_argument('-d', '--direction', type=str, help='direction: push/pull')
parser.add_argument('-b', '--bucket', type=str, help='bucket: mys3bucket.example.com')
parser.add_argument('-p', '--pdg', type=str, help='pdg command: True/False')

_args, other_args = parser.parse_known_args()
file = _args.file
direction = _args.direction
bucket = _args.bucket
if _args.pdg:
    pdg_command = _args.pdg
    print_log = False
else:
    pdg_command = False
    print_log = True

if print_log:
    print "sync", file


import logging
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

def setup_logger(name, log_file, level=logging.INFO):
    """Function setup as many loggers as you want"""

    handler = logging.FileHandler(log_file)        
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger

# first file logger
logger = setup_logger('first_logger', '/var/tmp/pre_job_logfile.log')
logger.info('start log')

# second file logger
super_logger = setup_logger('second_logger', '/var/tmp/second_logfile.log')

sys.path.append('/usr/lib64/python2.7/site-packages')
home_site_packages = os.path.expanduser('~/.local/lib/python2.7/site-packages')
sys.path.append(home_site_packages)
sys.path.append('/usr/lib/python2.7/site-packages')
s3_sync_path = os.environ['FIREHAWK_HOUDINI_TOOLS'] + '/scripts/s3_sync'
sys.path.append(s3_sync_path)
#sys.path.append('/home/deadlineuser/houdini17.5/scripts/s3_sync')

if direction != 'push' and direction != 'pull':
    logger.info('error no push/pull direction selected')
    super_logger.error('error no push/pull direction selected')
    sys.exit('error no push/pull direction selected')

### import s3_sync and aws cli libraries to push and pull from AWS S3
    
from awscli.clidriver import create_clidriver
import s3_sync as s3

display_output = True

syncfiles=[]
syncfiles.append(
    s3.syncfile(file,bucket)
    )

for index, syncfile in enumerate(syncfiles):
  if direction=='push':
    if display_output: logger.info("push sync file %s %s up" % (syncfile.dirname, syncfile.filename))
    syncfile.local_push()
  elif direction=='pull':
    if display_output: logger.info("pull sync file %s %s down" % (syncfile.dirname, syncfile.filename))
    syncfile.local_pull()

#return 'complete'

# clone upstream workitem data. this is for within the pdg context.
# import pdgcmd

# itemname = None
# callbackserver = None

# try:
#     if not itemname:
#         itemname = os.environ['PDG_ITEM_NAME']
#     if not callbackserver:
#         callbackserver = os.environ['PDG_RESULT_SERVER']

# except KeyError as exception: 
#     print "ERROR: {} must be in environment or specified via argument flag.".format(exception.message)
#     exit(1)

# work_item = WorkItem(getWorkItemJsonPath(itemname))

# print work_item.inputResults

# for result in work_item.inputResults[0]:
#     pdgcmd.reportResultData(result['data'], work_item.name, callbackserver, result['tag'])