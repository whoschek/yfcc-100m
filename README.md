
# yfcc-100m

Simple, fast scripts for parallel downlading of yffc-100m and clip-yfcc-15m AI training datasets
from official S3 bucket to local disk. Fully utilize all available network bandwidth. That's it.

Because the code is so simple, it is very easy to adjust to your needs.

## install

```
apt -y install python3 rclone wget tar unzip bzip2 zstd parallel
```

## quick start

```
$ ./download.sh
$ ./download-yfcc-15m.sh
```
