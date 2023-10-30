#!/usr/bin/env bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Construct 4096 lists of AWS S3 URLs from yfcc100m_subset_data.tsv unless the files are already present
# or can be hardlinked from a local src dir that already contains the files (e.g. the full 100M files dir).
# These 4096 URL lists will later be fed into multiple rclone processes that download in parallel from S3.
# If the process is cancelled (or fails) it can be restarted and will continue where it left off without
# redoing prior completed downloads.

#set -x # Print a trace of commands for debugging
set -e # Exit immediately if a pipeline cmd returns a non-zero status

trace() {
  echo $(date '+%Y:%m:%d-%H:%M:%S') "#$j:" ${1+"$@"}
}

rclone=${rclone:-../rclone}
parallel_transfers=${parallel_transfers:-8}
parallel_procs=${parallel_procs:-32}
s3_region=${s3_region:-us-west-2}
root1=multimedia-commons/data/images
root2=multimedia-commons/data/videos/keyframes
root3=multimedia-commons/data/videos/mp4
if [ "$roots" == "" ]; then
  roots="$root1"
  #roots="$root1 $root2 $root3"
fi

mkdir -p clip-yfcc15m
cd clip-yfcc15m
wget -c https://proceedings.mlr.press/v139/radford21a/radford21a.pdf
wget -c https://github.com/openai/CLIP/raw/main/data/yfcc100m.md # see https://github.com/openai/CLIP/blob/main/data/yfcc100m.md
wget -c https://openaipublic.azureedge.net/clip/data/yfcc100m_subset_data.tsv.bz2
for src in yfcc100m_subset_data.tsv; do
  if [[ -f $src.bz2 && ! -f $src.zst ]]; then # convert to faster compression format
    bzip2 -dc $src.bz2 | zstd $paramsForZstd -o $src.zst.tmp
    mv -f $src.zst.tmp $src.zst
  fi
done

rm -fr tmp/logs
mkdir -p tmp/logs
j=0
hashes=yfcc100m_subset_data_sorted_hashes.txt
if [ ! -d tmp/$root1/$hashes ]; then
  trace "Sorting file checksums for $root1 ..."
  mkdir -p tmp/$root1
  if [ "$subsubset" == "" ]; then
    subsubset="^"
    #subsubset="^80[0-1]"
    #subsubset="^[0-7]"
    #subsubset="^[89abcdef]"
  fi
  rm -fr tmp/$root1/tmp
  mkdir -p tmp/$root1/tmp
  time zstdcat yfcc100m_subset_data.tsv.zst | cut -f 3 | grep $subsubset | sort --buffer-size=800M | \
  ( # split a file containing text lines into N separate files such that all lines that start with the same
    # leading two characters end up in the same split file, where the file name of the split file is the leading
    # two characters:
    cd tmp/$root1/tmp
    awk '{ fname=substr($0, 1, 2); print > fname }'
  )
  mv tmp/$root1/tmp tmp/$root1/$hashes
fi

for root in $roots; do
  if [ ! -d tmp/$root/$hashes ]; then
    echo "File not found: tmp/$root/$hashes"
    exit 1
  fi
done

for root in $roots; do
  j=0
  num_checksums=$(cat tmp/$root/$hashes/* | wc -l)
  while true; do
    trace "Preparing download file lists for $num_checksums files for $root and writing them into tmp/file_lists/ ..."
    logname=$j-$(basename $root)
    rm -fr tmp/logs/$logname
    mkdir -p tmp/logs/$logname
    filelists=tmp/file_lists-$logname
    rm -fr $filelists
    mkdir -p $filelists
    progname=download-yfcc-15m.py
    inputlists=tmp/$root/$hashes
    find $inputlists -type f -exec basename {} \; | parallel -I % -P $parallel_procs \
      "../$progname $inputlists/% $root $filelists &> tmp/logs/$progname-$logname.%"

    trace "Done preparing."
    jobs=$(find $filelists -type f | wc -l)
    if [ "$jobs" == "0" ]; then
      trace "Nothing to download anymore into $root ..."
      break
    fi
    todo=$(cat $filelists/* | wc -l)
    trace "Downloading $todo files into $root ..."
    find $filelists -type f -exec basename {} \; | parallel -I % -P $parallel_procs \
      "$rclone sync :s3:$root $root --files-from-raw=$filelists/% --s3-region=$s3_region --transfers=$parallel_transfers -v &> tmp/logs/$logname/rclone.%"

    logfiles=tmp/logs/$logname/rclone.*
    num_copied=$(cat $logfiles | grep -E '.(jpg|mp4): Copied ' | wc -l)
    trace "Files copied: $num_copied"
    if false; then
      trace "Counting partial downloads in $root ..."
      time find $root -type f -name '*.partial' | wc -l # too slow b/c there are so many (16M=4096^2) subdirs
    fi
    nothingToTransfer=$(cat $logfiles | grep -F 'There was nothing to transfer' | wc -l)
    if [ "$nothingToTransfer" == "$jobs" ]; then
      missing=$(cat $filelists/* | wc -l)
      trace "Ignoring $missing files as they do not exist in S3 src - for details run: cat $filelists/*"
      break # nothing to download anymore
    fi
    j=$(( j + 1 ))
  done
done
