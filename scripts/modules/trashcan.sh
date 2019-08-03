#!/bin/bash

# trash everything below the current path that does not have a .protect file in the folder.  it should normally only be run from the folder such as 'job/seq/shot/cache' to trash all data below this path.

current_dir=$(pwd)
echo "current dir $current_dir"
base_dir=$(pwd | cut -d/ -f1-2)
echo "base_dir $base_dir"

source=$(realpath --relative-to=$base_dir $current_dir)/
echo "source $source"
target=trash/
echo "target $target"

find . -name .protect -print0 |
    while IFS= read -r -d '' line; do
        path=$(realpath --relative-to=. "$line")
        dirname $path
    done > exclude_list.txt

cd $base_dir
# run this command from the drive root, eg /prod.
rsync -a --remove-source-files --prune-empty-dirs --inplace --relative --exclude-from="$current_dir/exclude_list.txt" --include='*' --include='*/' $source $target --dry-run -v
cd $current_dir