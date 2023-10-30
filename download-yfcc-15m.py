#!/usr/bin/env python3
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

"""
Construct 4096 lists of AWS S3 URLs from yfcc100m_subset_data_sorted_hashes.txt unless the files are already present
or can be hard-linked from a local src dir that already contains the files (e.g. the full 100M files dir).
These 4096 URL lists will later be fed into multiple rclone processes that download in parallel from S3.
"""

import logging
import os
import sys

checksums_file = sys.argv[1]  # 'yfcc100m_subset_data_sorted_hashes.txt'
if not os.path.isfile(checksums_file):
    raise FileNotFoundError(f"File '{checksums_file}' does not exist.")
int_root_dir = sys.argv[2]  # 'multimedia-commons/data/images'
ext_root_dir = os.path.join('..', int_root_dir)
file_lists_dir = sys.argv[3]  # 'tmp/file_lists'
if not os.path.isdir(file_lists_dir):
    raise FileNotFoundError(f"Dir '{file_lists_dir}' does not exist.")

logging.basicConfig(filename=None, filemode='w', encoding='utf-8', level=logging.DEBUG)
prev_dir = None
file = None
num_skipped = 0
num_linked = 0
num_appended = 0
num_lines = 0
with open(checksums_file, 'r') as f:
    for checksum in f:
        if num_lines % 10 == 0:
            logging.debug("num_skipped: %s, num_linked: %s, num_appended: %s, num_lines: %s",
                  num_skipped, num_linked, num_appended, num_lines)
        num_lines += 1
        checksum = checksum.rstrip()
        dir = checksum[0:3]
        subdir = checksum[3:6]
        if dir != prev_dir:
            prev_dir = dir
            if file is not None:
                file.close()
                file = None

        file_name = dir + '/' + subdir + '/' + checksum + '.jpg'
        dst = os.path.join(int_root_dir, file_name)
        if os.path.isfile(dst):
            logging.debug('skipping existing %s', dst)
            num_skipped += 1
            continue

        src = os.path.join(ext_root_dir, file_name)
        if os.path.isfile(src):
            logging.debug('linking os.link(%s, %s)', src, dst)
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            os.link(src, dst)
            num_linked += 1
            continue

        if file is None:
            file = open(os.path.join(file_lists_dir, dir), "a+");

        logging.debug('appending %s', file_name)
        file.write(file_name + '\n')
        num_appended += 1

if file is not None:
    file.close()

print("num_skipped: %s, num_linked: %s, num_appended: %s, num_lines: %s"
      % (num_skipped, num_linked, num_appended, num_lines))
