"""UNQUALIFIED Linux primitives; SOURCE-PARSED ONLY on Alien.

No default roots or factory. Caller must supply independently admitted and pinned
directory descriptors with exclusive writer ownership, mount and ancestor proof.
This is not the complete Adapter kernel: receipt verification, protected observer,
service capability, transfer sink and actual bundle/native admission remain absent.
The four production entrypoints intentionally do not import or invoke this file.
"""
import json
import os
import stat
import uuid


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


class Directory:
    """Owned already-pinned directory; no pathname traversal/open of a target."""
    def __init__(self, fd, uid, identity):
        metadata = os.fstat(fd)
        if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != uid:
            raise ValueError("directory ownership/type")
        if (metadata.st_dev, metadata.st_ino) != identity or metadata.st_mode & 0o022:
            raise ValueError("directory identity/write boundary")
        self.fd, self.uid, self.device = os.dup(fd), uid, metadata.st_dev

    @staticmethod
    def leaf(name):
        if not isinstance(name, str) or name in ("", ".", "..") or any(x in name for x in ("/", "\\", "\0")):
            raise ValueError("single leaf required")
        return name

    def regular(self, name, flags, exclusive=False):
        # Validate BEFORE open: O_TRUNC mutates before fstat can reject a hardlink.
        # Caller cannot request append/create/temporary/unknown semantics. Read
        # existing members; read/write ONLY the already adopted stable lock;
        # write-only ONLY with exclusive creation. Add safe flags ourselves.
        allowed = ((flags == os.O_RDONLY and not exclusive) or
                   (flags == os.O_RDWR and not exclusive and name == "executor.lock") or
                   (flags == os.O_WRONLY and exclusive))
        if not allowed:
            raise ValueError("unpermitted regular-open flags")
        name = self.leaf(name)
        flags |= os.O_NOFOLLOW | os.O_CLOEXEC
        if exclusive: flags |= os.O_CREAT | os.O_EXCL
        descriptor = os.open(name, flags, 0o600, dir_fd=self.fd)
        try:
            s = os.fstat(descriptor)
            if (not stat.S_ISREG(s.st_mode) or s.st_nlink != 1 or s.st_uid != self.uid
                    or s.st_dev != self.device or s.st_mode & 0o022):
                raise ValueError("regular private same-device member required")
            return descriptor
        except BaseException:
            os.close(descriptor)
            raise

    def read(self, name, limit=1024*1024):
        fd = self.regular(name, os.O_RDONLY)
        try:
            before = os.fstat(fd)
            if before.st_size > limit: raise ValueError("read limit")
            blocks, size = [], 0
            while True:
                block = os.read(fd, min(65536, limit-size+1))
                if not block: break
                size += len(block)
                if size > limit: raise ValueError("read limit")
                blocks.append(block)
            after = os.fstat(fd)
            if (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (after.st_size, after.st_mtime_ns, after.st_ctime_ns):
                raise ValueError("read changed")
            return b"".join(blocks)
        finally:
            os.close(fd)

    def write_new(self, name, payload):
        fd = self.regular(name, os.O_WRONLY, exclusive=True)
        try:
            remaining = memoryview(payload)
            while remaining:
                count = os.write(fd, remaining)
                if count <= 0: raise OSError("short write")
                remaining = remaining[count:]
            os.fsync(fd)
        finally:
            os.close(fd)
        os.fsync(self.fd)

    def replace_record(self, name, expected, value, lease):
        """Expected bytes checked under lifetime lock, durable replacement.

Do not remove failed temp records: they are evidence for explicit recovery.
"""
        if name not in ("executor-record.json", "journal.json", "terminal.json"):
            raise ValueError("reserved control record")
        lease.assert_held()
        try: observed = self.read(name)
        except FileNotFoundError: observed = None
        if observed != expected: raise ValueError("record CAS mismatch")
        temp = ".pending-" + uuid.uuid4().hex
        self.write_new(temp, canonical(value))
        lease.assert_held()
        os.replace(temp, self.leaf(name), src_dir_fd=self.fd, dst_dir_fd=self.fd)
        os.fsync(self.fd)

    def replace_alias(self, expected, target, allowed, lease):
        """Only adopted old/new absolute targets; never change shared aliases.

Adoption must prove this directory has no other writers. flock does not fence
arbitrary same-uid writers; exclusive ownership is an external prerequisite.
"""
        lease.assert_held()
        if expected not in allowed or target not in allowed or expected == target:
            raise ValueError("unbound alias transition")
        info = os.stat("current", dir_fd=self.fd, follow_symlinks=False)
        if not stat.S_ISLNK(info.st_mode) or info.st_uid != self.uid or info.st_nlink != 1:
            raise ValueError("current alias type/owner")
        if os.readlink("current", dir_fd=self.fd) != expected: raise ValueError("old alias mismatch")
        temp = ".alias-" + uuid.uuid4().hex
        os.symlink(target, temp, dir_fd=self.fd)
        lease.assert_held()
        os.replace(temp, "current", src_dir_fd=self.fd, dst_dir_fd=self.fd)
        os.fsync(self.fd)


class Lease:
    """Persistent pre-created lock inode, never created/replaced/unlinked here."""
    def __init__(self, directory, expected_identity):
        import fcntl  # native-only; never imported in source validation
        self.fcntl, self.directory = fcntl, directory
        self.fd = directory.regular("executor.lock", os.O_RDWR)
        try:
            s = os.fstat(self.fd)
            if (s.st_dev, s.st_ino) != expected_identity: raise ValueError("lock inode changed")
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException:
            os.close(self.fd)
            self.fd = None
            raise
        self.identity = expected_identity

    def assert_held(self):
        if self.fd is None: raise ValueError("released lease")
        s = os.stat("executor.lock", dir_fd=self.directory.fd, follow_symlinks=False)
        if (s.st_dev, s.st_ino) != self.identity or s.st_nlink != 1: raise ValueError("lock path changed")

    def release_after_durable_terminal(self):
        self.assert_held()
        self.fcntl.flock(self.fd, self.fcntl.LOCK_UN)
        os.close(self.fd)
        self.fd = None
