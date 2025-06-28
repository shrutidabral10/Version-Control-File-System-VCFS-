import os
import sys
import errno
from fuse import FUSE, Operations, FuseOSError


class VCFSBase(Operations):
    def __init__(self, root):
        self.root = os.path.realpath(root)
        self.meta_dir = os.path.join(self.root, ".vcfs_meta")
        os.makedirs(self.meta_dir, exist_ok=True)

    def _full_path(self, path):
        rel = path.lstrip("/")
        return os.path.join(self.root, rel)

    def getattr(self, path, fh=None):
        full = self._full_path(path)
        try:
            st = os.lstat(full)
        except FileNotFoundError:
            raise FuseOSError(errno.ENOENT)
        return {
            'st_mode':  st.st_mode,
            'st_size':  st.st_size,
            'st_atime': st.st_atime,
            'st_mtime': st.st_mtime,
            'st_ctime': st.st_ctime,
        }

    def readdir(self, path, fh):
        full = self._full_path(path)
        yield '.'
        yield '..'
        if os.path.isdir(full):
            for name in os.listdir(full):
                yield name

    def open(self, path, flags):
        full = self._full_path(path)
        try:
            return os.open(full, flags)
        except OSError:
            raise FuseOSError(errno.ENOENT)

    def create(self, path, mode, fi=None):
        full = self._full_path(path)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        return os.open(full, os.O_WRONLY | os.O_CREAT, mode)

    def read(self, path, size, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        return os.read(fh, size)

    def truncate(self, path, length, fh=None):
        full = self._full_path(path)
        with open(full, 'r+') as f:
            f.truncate(length)

    def unlink(self, path):
        full = self._full_path(path)
        try:
            os.unlink(full)
        except FileNotFoundError:
            raise FuseOSError(errno.ENOENT)

    def mkdir(self, path, mode):
        full = self._full_path(path)
        try:
            os.makedirs(full, mode=mode, exist_ok=False)
        except FileExistsError:
            raise FuseOSError(errno.EEXIST)

    def rmdir(self, path):
        full = self._full_path(path)
        try:
            os.rmdir(full)
        except OSError as e:
            raise FuseOSError(e.errno)

    def rename(self, old, new):
        full_old = self._full_path(old)
        full_new = self._full_path(new)
        try:
            os.rename(full_old, full_new)
        except OSError as e:
            raise FuseOSError(e.errno)

    def utimens(self, path, times=None):
        full = self._full_path(path)
        os.utime(full, times)

    def flush(self, path, fh):
        return os.fsync(fh)

    def release(self, path, fh):
        return os.close(fh)
