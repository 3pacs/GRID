"""Pinned Linux interpreter/cwd launcher adapter source; not natively executed.

No adoption factory or default bootstrap program. The exact authority must admit
ELF/loader/imports and a state-binding bootstrap before exec can be reached.
"""
import os
import stat
import threading
from api_bundle_contract import require, sha
from api_isolation_native_fs import directory_at
from api_isolation_bootstrap import render_bound_program


class PinnedLauncher:
    def __init__(self,api_root,releases,commit,target,authority):
        require(sha(commit,40) and target=='/data/grid_v4/api-isolated/releases/'+commit,'unbound launch target')
        self.api_root,self.releases=api_root,releases
        self.commit,self.target,self.authority=commit,target,authority
        self.claimed=False; self.tokens={}; self.closed=False
        self._lock=threading.RLock()
    def _live(self,token):
        pinned=self.tokens.get(token)
        require(not self.closed and pinned is not None and pinned['phase']=='LIVE',
                'revoked, consumed or unknown launcher token')
        return pinned
    def pin_current(self):
        with self._lock:
            require(not self.closed,'launcher closed')
            return self._pin_current()
    def _pin_current(self):
        require(not self.claimed,'current already resolved')
        self.claimed=True
        meta=os.stat('current',dir_fd=self.api_root.fd,follow_symlinks=False)
        require(stat.S_ISLNK(meta.st_mode) and meta.st_uid==self.api_root.uid and meta.st_nlink==1,'launch alias owner/type')
        require(os.readlink('current',dir_fd=self.api_root.fd)==self.target,'current target mismatch')
        opened=[]
        try:
            release=directory_at(self.releases,self.commit); opened.append(release)
            source=directory_at(release,'source'); opened.append(source)
            runtime=directory_at(release,'runtime'); opened.append(runtime)
            binary=directory_at(runtime,'bin'); opened.append(binary)
            interpreter=binary.regular('python3.10',os.O_RDONLY)
        except BaseException:
            for directory in reversed(opened): os.close(directory.fd)
            raise
        token=object()
        self.tokens[token]={'release':release,'source':source,'runtime':runtime,'binary':binary,
                            'interpreter':interpreter,'verified':False,'state':False,'program':None,
                            'phase':'LIVE','closed':False,
                            'cleanup_fds':[interpreter,binary.fd,runtime.fd,source.fd,release.fd]}
        return token,self.target
    def verify_release(self,token,commit,spec):
        with self._lock:
            pinned=self._live(token)
            require(commit==self.commit and self.authority.verify_launch_descriptors(pinned,spec) is True,
                    'launch descriptor/runtime authority refused')
            self._live(token)  # An injected authority may have revoked reentrantly.
            pinned['verified']=True
            pinned['spec']=dict(spec)
    def state_bound_program(self,token,spec,binding):
        with self._lock:
            pinned=self._live(token)
            require(pinned['verified'] and spec==pinned['spec'],'unverified or changed launch')
            # Rebinding cannot leave an earlier successful program executable.
            pinned['state']=False; pinned['program']=None
            program=render_bound_program(spec,binding)
            self._live(token)
            require(isinstance(program,str) and program and
                    self.authority.verify_bootstrap_program(program,binding) is True,'state bootstrap authority refused')
            self._live(token)
            pinned['program']=program; pinned['state']=True
            return program
    def exec_pinned(self,token,argv,environment,cwd):
        with self._lock:
            p=self._live(token)
            require(p['verified'] and p['state'] and len(argv)==6 and argv[-1]==p['program'],'unbound bootstrap execution')
            require(argv[0]==p['spec']['interpreter'] and argv[1:5]==['-I','-S','-B','-c'] and
                    cwd==p['spec']['cwd'],'exec arguments changed')
            require(os.execve in os.supports_fd,'descriptor exec unavailable')
            # Consume BEFORE any OS operation. Failure/return cannot grant retry.
            p['phase']='CONSUMED'; p['verified']=False; p['state']=False
            os.fchdir(p['source'].fd)
            os.execve(p['interpreter'],argv,environment)

    def close(self):
        """Close owned descriptors after refused exec; no files are removed."""
        with self._lock:
            self.closed=True
            # Revoke every capability before releasing even the first descriptor.
            for p in self.tokens.values():
                p['phase']='REVOKED'; p['closed']=True
                p['verified']=False; p['state']=False; p['program']=None
            first_error=None
            for p in self.tokens.values():
                while p['cleanup_fds']:
                    fd=p['cleanup_fds'].pop(0)
                    # A failed close may already have released the number. Never
                    # retry it: a subsequent close could affect a reused fd.
                    try: os.close(fd)
                    except OSError as error:
                        if first_error is None: first_error=error
            if first_error is not None: raise first_error
