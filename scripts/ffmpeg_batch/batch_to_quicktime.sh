#!/bin/bash

# Convert a bunch of mov / mp4 files in the current dir to quciktime compatible mp4
# not great for large numbers of files that will max out ram/cores, but will get the job done quicker than one by one, and great if you have a machine with a high core count and gobs of ram.
# Consider this post for thread limiting https://gist.github.com/Brainiarc7/2afac8aea75f4e01d7670bc2ff1afad1

shopt -s nullglob;
for file in "$arg"*.{mov,mp4,MOV,MP4} ; do
    echo "convert $file"
    ffmpeg -i "$file" -vcodec h264 -acodec aac -pix_fmt yuv420p "mp4_${file%.*}.mp4" </dev/null > /dev/null 2>&1 &
    # this is used to background output to shell when threading multiple files - </dev/null > /dev/null 2>&1 &
done