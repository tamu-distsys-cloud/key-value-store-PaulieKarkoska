import random
import threading
from typing import Any, List

from labrpc.labrpc import ClientEnd
from server import GetArgs, GetReply, PutAppendArgs, PutAppendReply

def nrand() -> int:
    return random.getrandbits(62)

class Clerk:
    def __init__(self, servers: List[Any], cfg):
        self.servers = servers
        self.cfg = cfg
        self.client_id = nrand()
        self.seq = 0
        self.lock = threading.Lock()

    def _next_seq(self):
        with self.lock:
            self.seq += 1
            return self.seq

    def _shard_for_key(self, key):
        nshards = getattr(self.cfg, 'nservers', len(self.servers))
        try:
            return int(key) % nshards
        except Exception:
            return 0  # fallback, but tests expect int(key)

    # Fetch the current value for a key.
    # Returns "" if the key does not exist.
    # Keeps trying forever in the face of all other errors.
    #
    # You can send an RPC with code like this:
    # reply = self.server[i].call("KVServer.Get", args)
    # assuming that you are connecting to the i-th server.
    #
    # The types of args and reply (including whether they are pointers)
    # must match the declared types of the RPC handler function's
    # arguments in server.py.
    def get(self, key: str) -> str:
        seq = self._next_seq()
        nshards = getattr(self.cfg, 'nservers', len(self.servers))
        nreplicas = getattr(self.cfg, 'nreplicas', 1)
        shard = self._shard_for_key(key)
        while True:
            for r in range(nreplicas):
                idx = (shard + r) % len(self.servers)
                srv = self.servers[idx]
                args = GetArgs(key)
                args.client_id = self.client_id
                args.seq = seq
                try:
                    reply = srv.call("KVServer.Get", args)
                    if reply is not None and hasattr(reply, 'err'):
                        if getattr(reply, 'err', None) == "":
                            return str(reply.value) if reply.value is not None else ""
                        elif getattr(reply, 'err', None) == "ErrWrongGroup":
                            continue  # Only retry on ErrWrongGroup
                        else:
                            continue  # retry on all errors
                except Exception:
                    continue
        return ""

    # Shared by Put and Append.
    #
    # You can send an RPC with code like this:
    # reply = self.servers[i].call("KVServer."+op, args)
    # assuming that you are connecting to the i-th server.
    #
    # The types of args and reply (including whether they are pointers)
    # must match the declared types of the RPC handler function's
    # arguments in server.py.
    def put_append(self, key: str, value: str, op: str) -> str:
        seq = self._next_seq()
        nshards = getattr(self.cfg, 'nservers', len(self.servers))
        nreplicas = getattr(self.cfg, 'nreplicas', 1)
        shard = self._shard_for_key(key)
        while True:
            for r in range(nreplicas):
                idx = (shard + r) % len(self.servers)
                srv = self.servers[idx]
                args = PutAppendArgs(key, value)
                args.client_id = self.client_id
                args.seq = seq
                try:
                    reply = srv.call(f"KVServer.{op}", args)
                    if reply is not None and hasattr(reply, 'err'):
                        if getattr(reply, 'err', None) == "":
                            return str(reply.value) if reply.value is not None else ""
                        elif getattr(reply, 'err', None) == "ErrWrongGroup":
                            continue  # Only retry on ErrWrongGroup
                        else:
                            continue  # retry on all errors
                except Exception:
                    continue
        return ""

    def put(self, key: str, value: str):
        self.put_append(key, value, "Put")

    # Append value to key's value and return that value
    def append(self, key: str, value: str) -> str:
        return self.put_append(key, value, "Append")
