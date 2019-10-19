#!/bin/bash

# trash everything below the current path that does not have a .protect file in the folder.  it should normally only be run from the folder such as 'job/seq/shot/cache' to trash all data below this path.
# see opmenu and firehawk_submit.py for tools to add protect files based on a top net tree for any given hip file.
# after creating .protect files in cache paths you wish to keep, we would generally run this script in the root cache or render output path since it will create a trash can there.

argument="$1"

echo ""
ARGS=''

if [[ -z $argument ]] ; then
  echo "DRY RUN. To move files to trash, use argument -m after reviewing the exclude_list.txt and you are sure it lists everything you wish to protect from being moved to the trash."
  echo ""
  ARGS1='--remove-source-files'
  ARGS2='--dry-run'
else
  case $argument in
    -m|--move)
      echo "MOVING FILES TO TRASH."
      echo ""
      ARGS1='--remove-source-files'
      ARGS2=''
      ;;
    *)
      raise_error "Unknown argument: ${argument}"
      return
      ;;
  esac
fi

current_dir=$(pwd)
echo "current dir $current_dir"
base_dir=$(pwd | cut -d/ -f1-2)
echo "base_dir $base_dir"


source=$(realpath --relative-to=$base_dir $current_dir)/
echo "source $source"
target=trash/
echo "target $target"

# ensure trash exists at base dir.
mkdir -p $base_dir/$target
echo ""
echo "Build exclude_list.txt contents with directories containing .protect files"
find . -name .protect -print0 |
    while IFS= read -r -d '' line; do
        path=$(realpath --relative-to=. "$line")
        dirname $path
    done > exclude_list.txt

path_to_list=$(realpath --relative-to=. exclude_list.txt)
echo $path_to_list >> exclude_list.txt

cat exclude_list.txt

cd $base_dir

# run this command from the drive root, eg /prod.
rsync -a $ARGS1 --prune-empty-dirs --inplace --relative --exclude-from="$current_dir/exclude_list.txt" --include='*' --include='*/' $source $target $ARGS2 -v
cd $current_dir
