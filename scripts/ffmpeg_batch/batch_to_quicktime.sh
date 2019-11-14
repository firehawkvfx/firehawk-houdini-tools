#!/bin/bash

# Convert a bunch of mov / mp4 files in the current dir to quciktime compatible mp4
# not great for large numbers of files that will max out ram/cores, but will get the job done quicker than one by one, and great if you have a machine with a high core count and gobs of ram.
# Consider this post for thread limiting https://gist.github.com/Brainiarc7/2afac8aea75f4e01d7670bc2ff1afad1

pass the pattern to match as a string '*.mp4'

shopt -s nullglob;

args=$arg
match=$1

list () {
    for file in "$args"$match ; do
        echo "will convert ./$file"
    done
}

convert () {
    echo 'converting files in background'
    mkdir -p mp4
    for file in "$args"$match ; do
        echo "convert ./$file"
        ffmpeg -i "$file" -vcodec h264 -acodec aac -pix_fmt yuv420p "mp4_${file%.*}.mp4" </dev/null > /dev/null 2>&1 &
        # this is used to background output to shell when threading multiple files - </dev/null > /dev/null 2>&1 &
    done
}

list

echo "Do you wish to batch convert the listed files?"
select yn in "Yes" "No"; do
    case $yn in
        Yes ) convert; break;;
        No ) exit;;
    esac
done