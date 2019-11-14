#!/bin/bash

# Convert a bunch of mov / mp4 files in the current dir to prores format for editting in an NLE (FCP X, Davinci Resolve etc).
# not great for large numbers of files that will max out cores.
# Consider this post for thread limiting https://gist.github.com/Brainiarc7/2afac8aea75f4e01d7670bc2ff1afad1

shopt -s nullglob;
for file in "$arg"*.{mov,mp4,MOV,MP4} ; do
    echo "convert $file"
    ffmpeg -i "$file" -vcodec prores -acodec pcm_s16le "prores_${file%.*}.mov" </dev/null > /dev/null 2>&1 &
done