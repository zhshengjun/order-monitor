#!/bin/sh
set -eu
path="${DATABASE_PATH:-/data/monitor.db}"
dir=$(dirname "$path")
mkdir -p "$dir"
case "$dir/" in
  /data/*)
    current=/data
    rest=${dir#/data}
    chown 10001:10001 "$current"
    chmod 0750 "$current"
    oldifs=$IFS
    IFS=/
    for part in $rest; do
      [ -n "$part" ] || continue
      current="$current/$part"
      [ ! -L "$current" ] || { echo "数据目录不能是符号链接" >&2; exit 1; }
      chown 10001:10001 "$current"
      chmod 0750 "$current"
    done
    IFS=$oldifs
    for suffix in '' -wal -shm -journal; do
      file="$path$suffix"
      [ ! -L "$file" ] || { echo "数据库文件不能是符号链接" >&2; exit 1; }
      if [ -e "$file" ]; then chown 10001:10001 "$file"; chmod 0600 "$file"; fi
    done
    ;;
esac
exec "$@"
