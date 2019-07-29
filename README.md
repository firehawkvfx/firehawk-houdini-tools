# openfirehawk-houdini-tools

A collection of useful hda's and scripts used in the Open Firehawk VFX pipeline.

To use this you will need some environment variables in your ~/houdini17.5/houdini.env file.

Here is an example of what the current houdini.env file looks like for my usage.
Point FIREHAWK_HOUDINI_TOOLS to the path where you clone this repository.
```
# The path where you cloned the openfirehawk-houdini-tools repository
FIREHAWK_HOUDINI_TOOLS = "/prod/assets/openfirehawk-houdini-tools"
# The root volume name of your production volume.  This volume path also exists ofsite in AWS for syncing data.
PROD_ROOT = "/prod"
# This is a bind mount/link that is identical to the path above, but is an absolute reference so that volumes can be referenced with absolute paths over vpn.
PROD_ONSITE_ROOT = "/cairns_prod"
# This is an absolute reference to an offsite storage volume that is avaiable to any cloud based systems / onsite via vpn.
PROD_CLOUD_ROOT = "/aws_sydney_prod"

HOUDINI_PATH = "$HOUDINI_PATH; ~/Thinkbox/Deadline10/submitters/HoudiniSubmitter; $FIREHAWK_HOUDINI_TOOLS; &"
# Deadline is only required if cloud submission to AWS is to be used, otherwise it can be removed.
HOUDINI_MENU_PATH = "$HOUDINI_MENU_PATH; /home/deadlineuser/houdini17.5/scripts/menus; $FIREHAWK_HOUDINI_TOOLS/scripts/menus; ~/Thinkbox/Deadline10/submitters/HoudiniSubmitter;&"

HOUDINI_OTLSCAN_PATH = "@/otls:/prod/assets/hda:$FIREHAWK_HOUDINI_TOOLS/hda"

PYTHON = "/opt/hfs17.5/python/bin/python"
```
