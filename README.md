# openfirehawk-houdini-tools

A collection of useful hda's and scripts used in the Open Firehawk VFX pipeline.  I've you find this useful and would like to support its evolution reach out to contribute to the repo or you can support the work on Patreon - https://www.patreon.com/openfirehawk

Many of these tools are still a work in progress and not ready for production.  they represent the current state in my own workflows.

If you commit any changes please commit to the dev branch.

To use this you will need some environment variables in your ~/houdini17.5/houdini.env file.

Here is an example of what the current houdini.env file looks like for my usage.
Point FIREHAWK_HOUDINI_TOOLS to the path where you clone this repository.
```
# The path where you cloned the openfirehawk-houdini-tools repository
FIREHAWK_HOUDINI_TOOLS = "/prod/assets/openfirehawk-houdini-tools"

# The root volume name of your production volume.  This volume path also exists ofsite in 
# AWS for syncing data.
PROD_ROOT = "/prod"

# This is a bind mount/link that is identical to the path above, but is an absolute reference 
# so that volumes at different locations can be referenced with absolute paths over vpn.
PROD_ONSITE_ROOT = "/cairns_prod"

# This is an absolute reference to an offsite storage volume that is avaiable 
# to any cloud based systems / onsite via vpn. if you dont intend to use any cloud functions, 
# it can be set to the same value as PROD_ONSITE_ROOT
PROD_CLOUD_ROOT = "/aws_sydney_prod"

# Deadline is only required if cloud submission to AWS is to be used, 
# otherwise deadline paths can be removed.
HOUDINI_PATH = "$HOUDINI_PATH; ~/Thinkbox/Deadline10/submitters/HoudiniSubmitter; $FIREHAWK_HOUDINI_TOOLS; &"
HOUDINI_MENU_PATH = "$HOUDINI_MENU_PATH; /home/deadlineuser/houdini17.5/scripts/menus; $FIREHAWK_HOUDINI_TOOLS/scripts/menus; ~/Thinkbox/Deadline10/submitters/HoudiniSubmitter;&"

HOUDINI_OTLSCAN_PATH = "@/otls:/prod/assets/hda:$FIREHAWK_HOUDINI_TOOLS/hda"

PYTHON = "/opt/hfs17.5/python/bin/python"
```
