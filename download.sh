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

#set -x # Print a trace of commands for debugging
set -e # Exit immediately if a pipeline cmd returns a non-zero status

# see http://mmcommons.org/
# and https://github.com/chi0tzp/YFCC100M-Downloader

#sudo apt -y install python3 rclone wget tar unzip bzip2 zstd parallel # awscli

# check if download file is already present needs recent rclone version per
# https://github.com/rclone/rclone/pull/6913 (download to tmp file followed by atomic rename)
if [ ! -f rclone ]; then
  wget -c https://github.com/rclone/rclone/releases/download/v1.64.2/rclone-v1.64.2-linux-386.zip
  unzip rclone-*.zip
  rm rclone-*.zip
  mv rclone-*/rclone rclone
  rm -fr rclone-*
fi

parallel_transfers=${parallel_transfers:-8}
parallel_procs=${parallel_procs:-32}
s3_region=${s3_region:-us-west-2}
root=mmcommons
mkdir -p $root
cd $root
#aws s3 sync s3://$root ./ --no-sign-request
../rclone sync :s3:$root ./ --s3-region=$s3_region --transfers=$parallel_transfers -v --progress
paramsForZstd="-1 -f --quiet --no-progress"
for src in yfcc100m_dataset yfcc100m_autotags-v1 yfcc100m_lines yfcc100m_hash yfcc100m_exif; do
  if [[ -f $src.tgz && ! -f $src.zst ]]; then # convert to faster compression format
    time tar -xzf $src.tgz -O | zstd $paramsForZstd -o $src.zst.tmp
    mv -f $src.zst.tmp $src.zst
  fi
done
cd ..

#aws s3 sync s3://multimedia-commons/data/images/fff/ multimedia-commons/data/images/fff --no-sign-request
root1=multimedia-commons/data/images
root2=multimedia-commons/data/videos/keyframes
root3=multimedia-commons/data/videos/mp4
if [ "$roots" == "" ]; then
  roots="$root1"
  #roots="$root1 $root2 $root3"
fi
rclone=${rclone:-./rclone}
#rclone=${rclone:-../yfcc-100m/rclone}
for root in $roots; do
  for i in {0..4095}; do
    hex=$(printf "%03x" "$i") # 3-digit hex number padded with leading zeroes
    echo "$hex"
  done | parallel -I % -P $parallel_procs \
    "$rclone sync :s3:$root/% $root/% --s3-region=$s3_region --transfers=$parallel_transfers -v &> /tmp/rclone.%"
done
echo multimedia-commons/data/videos/metadata | parallel -I % -P $parallel_procs \
    "$rclone sync :s3:% %/ --s3-region=$s3_region --transfers=$parallel_transfers -v"
