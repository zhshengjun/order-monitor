"""Prepare the mounted data directory, then run the service as an unprivileged user."""
import os
import sys
from pathlib import Path


def main():
    path = Path(os.environ.get('DATABASE_PATH', '/data/monitor.db')).absolute()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        if os.geteuid() == 0:
            # Limit automatic ownership changes to the application's data mount.
            if path.parent == Path('/data') or Path('/data') in path.parent.parents:
                directories = [Path('/data')]
                directories += list(reversed([p for p in path.parent.parents if p != Path('/data') and Path('/data') in p.parents]))
                if path.parent != Path('/data'):
                    directories.append(path.parent)
                for directory in directories:
                    if directory.is_symlink():
                        raise ValueError('数据目录不能是符号链接')
                    os.chown(directory, 10001, 10001)
                    os.chmod(directory, 0o750)
                for suffix in ('', '-wal', '-shm', '-journal'):
                    file = Path(str(path) + suffix)
                    if file.is_symlink():
                        raise ValueError('数据库文件不能是符号链接')
                    if file.exists():
                        os.chown(file, 10001, 10001)
                        os.chmod(file, 0o600)
            os.setgroups([])
            os.setgid(10001)
            os.setuid(10001)
        # Test directory write access as the actual service user, not as root.
        import tempfile
        with tempfile.TemporaryFile(dir=path.parent):
            pass
    except (OSError, ValueError) as exc:
        raise SystemExit('数据目录初始化失败：%s；请将挂载目录设为 UID/GID 10001 可写，且不要使用只读挂载。原因：%s' % (path.parent, exc))
    os.execvp(sys.argv[1], sys.argv[1:])


if __name__ == '__main__':
    main()
