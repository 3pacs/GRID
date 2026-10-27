"""Linux descriptor-tree and receiving sink SOURCE; native fixtures UNRUN.

No path-based adoption factory or production entrypoint. The caller supplies
admitted pinned directories, UID/device/protected inode identities and a held
executor lease. Exclusive writer custody is required: these checks do not sandbox
an adversarial same-UID writer or establish mount/ancestor/ACL admission.
"""
import ctypes
import errno
import hashlib
import json
import os
import stat
import uuid
from copy import deepcopy
from api_isolation_posix import Directory, canonical
from api_bundle_contract import relative, require, sha


def components(name):
    parts=relative(name).parts
    require(parts and all(p not in ('','.','..') for p in parts), 'empty/traversal path')
    return parts


def directory_at(parent, name, create=False):
    Directory.leaf(name)
    if create:
        try:
            os.mkdir(name,0o700,dir_fd=parent.fd)
            os.fsync(parent.fd)
        except FileExistsError:
            pass  # the existing object MUST still pass no-follow directory open
    fd=os.open(name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=parent.fd)
    try:
        s=os.fstat(fd)
        require(s.st_dev==parent.device,'cross-device directory')
        return Directory(fd,parent.uid,(s.st_dev,s.st_ino))
    finally:
        os.close(fd)


def rename_no_replace(source, source_name, dest, dest_name):
    """Linux renameat2 RENAME_NOREPLACE, no unsafe compatibility fallback."""
    Directory.leaf(source_name); Directory.leaf(dest_name)
    require(source.device==dest.device,'cross-device publish')
    libc=ctypes.CDLL(None,use_errno=True)
    function=getattr(libc,'renameat2',None)
    require(function is not None,'renameat2 unavailable')
    function.argtypes=[ctypes.c_int,ctypes.c_char_p,ctypes.c_int,ctypes.c_char_p,ctypes.c_uint]
    function.restype=ctypes.c_int
    result=function(source.fd,os.fsencode(source_name),dest.fd,os.fsencode(dest_name),1)
    if result!=0:
        code=ctypes.get_errno()
        raise OSError(code,os.strerror(code))
    os.fsync(source.fd); os.fsync(dest.fd)


class Tree:
    def __init__(self,directory,protected_inodes=()):
        self.root=directory; self.protected=set(protected_inodes)

    def parent(self,name,create=False):
        parts=components(name); opened=[]; current=self.root
        try:
            for part in parts[:-1]:
                current=directory_at(current,part,create); opened.append(current)
            return current,parts[-1],opened
        except BaseException:
            for d in reversed(opened): os.close(d.fd)
            raise

    def write_exclusive(self,name,mode,data):
        require(mode in ('100644','100755') and isinstance(data,bytes),'payload mode/type')
        parent,leaf,opened=self.parent(name,True)
        fd=None
        try:
            fd=parent.regular(leaf,os.O_WRONLY,exclusive=True)
            metadata=os.fstat(fd)
            require((metadata.st_dev,metadata.st_ino) not in self.protected,'protected inode')
            rest=memoryview(data)
            while rest:
                count=os.write(fd,rest)
                require(count>0,'short write'); rest=rest[count:]
            os.fchmod(fd,int(mode[-3:],8)); os.fsync(fd); os.fsync(parent.fd)
        finally:
            if fd is not None: os.close(fd)
            for d in reversed(opened): os.close(d.fd)

    def observations(self,max_bytes=1024**3,max_files=100000):
        result={}; identities=set(); total=0
        def walk(directory,prefix):
            nonlocal total
            for name in sorted(os.listdir(directory.fd)):
                Directory.leaf(name)
                path=prefix+name
                info=os.stat(name,dir_fd=directory.fd,follow_symlinks=False)
                require(info.st_dev==self.root.device and info.st_uid==self.root.uid,'member owner/device')
                if stat.S_ISDIR(info.st_mode):
                    child=directory_at(directory,name)
                    try: walk(child,path+'/')
                    finally: os.close(child.fd)
                    continue
                require(stat.S_ISREG(info.st_mode),'nonregular tree member')
                require(len(result)<max_files and info.st_size<=max_bytes-total,'tree limits')
                fd=directory.regular(name,os.O_RDONLY)
                try:
                    before=os.fstat(fd); identity=(before.st_dev,before.st_ino)
                    require(identity==(info.st_dev,info.st_ino),'member changed during open')
                    require(identity not in self.protected and identity not in identities,'shared/protected inode')
                    require(stat.S_IMODE(before.st_mode) in (0o644,0o755),'member mode')
                    chunks=[]; size=0
                    while True:
                        block=os.read(fd,min(65536,max_bytes-total-size+1))
                        if not block: break
                        size+=len(block); require(size<=max_bytes-total,'tree byte limit'); chunks.append(block)
                    after=os.fstat(fd)
                    require((before.st_size,before.st_mtime_ns,before.st_ctime_ns,before.st_nlink)==
                            (after.st_size,after.st_mtime_ns,after.st_ctime_ns,after.st_nlink),'member changed during read')
                    require(size==before.st_size,'partial read')
                    result[path]={'kind':'regular','nlink':after.st_nlink,'uid':after.st_uid,
                                  'device':after.st_dev,'inode':after.st_ino,
                                  'mode':'100'+oct(stat.S_IMODE(after.st_mode))[2:],'content':b''.join(chunks)}
                    identities.add(identity); total+=size
                finally: os.close(fd)
        walk(self.root,'')
        return result

    def inventory(self):
        return {name:{'mode':v['mode'],'size':len(v['content']),
                      'sha256':hashlib.sha256(v['content']).hexdigest()}
                for name,v in self.observations().items()}


class ReceivingSink:
    """Exclusive owned upload subtree; publish only exact verified content.

Requires caller-held executor lease throughout. No automatic deletion on failure.
Crash recovery must re-admit retained transfer metadata and bytes explicitly;
this object will not resume an existing upload or reuse a token.
"""
    def __init__(self,inbox,lease,protected_inodes=()):
        self.inbox,self.lease=inbox,lease
        self.protected=set(protected_inodes); self.sessions={}

    def begin_exclusive(self,commit,manifest):
        self.lease.assert_held()
        require(sha(commit,40) and sha(manifest),'transfer identities')
        try: os.stat(commit,dir_fd=self.inbox.fd,follow_symlinks=False)
        except FileNotFoundError: pass
        else: raise FileExistsError('destination already exists')
        token=uuid.uuid4().hex; staging='.upload-'+token
        os.mkdir(staging,0o700,dir_fd=self.inbox.fd); os.fsync(self.inbox.fd)
        root=directory_at(self.inbox,staging)
        payload=directory_at(root,'payload',True)
        root.write_new('transfer.json',canonical({'commit':commit,'manifest':manifest,'token':token,'state':'PREPARED'}))
        self.sessions[token]={'root':root,'payload':payload,'commit':commit,'manifest':manifest,
                              'tree':Tree(payload,self.protected),'verified':None,'published':False}
        return token

    def session(self,token):
        self.lease.assert_held()
        require(token in self.sessions and not self.sessions[token]['published'] and not self.sessions[token].get('closed'),'foreign/spent transfer token')
        return self.sessions[token]

    def write_exclusive(self,token,name,mode,data):
        s=self.session(token)
        require(components(name)[0] in ('source','runtime'),'unowned transfer namespace')
        require(s['verified'] is None,'cannot write after inventory')
        s['tree'].write_exclusive(name,mode,data)

    def inventory(self,token):
        s=self.session(token); result=s['tree'].inventory(); s['verified']=deepcopy(result)
        return result

    def publish_no_replace(self,token,manifest):
        s=self.session(token)
        require(manifest==s['manifest'] and s['verified'] is not None,'unverified transfer')
        require(s['tree'].inventory()==s['verified'],'transfer changed after readback')
        s['root'].write_new('publish-intent.json',canonical({'commit':s['commit'],'manifest':manifest,'inventory':s['verified']}))
        self.lease.assert_held()
        rename_no_replace(s['root'],'payload',self.inbox,s['commit'])
        s['published']=True
        try:
            s['root'].write_new('published.json',canonical({'commit':s['commit'],'manifest':manifest}))
        finally:
            os.close(s['payload'].fd); os.close(s['root'].fd); s['closed']=True

    def close(self):
        """Close owned descriptors only; retain every failed/staged file."""
        for s in self.sessions.values():
            if not s.get('closed'):
                os.close(s['payload'].fd); os.close(s['root'].fd)
            s['closed']=True
