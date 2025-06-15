import logging
import threading
from typing import Tuple, Any

debugging = False

# Use this function for debugging
def debug(format, *args):
    if debugging:
        logging.info(format % args)

# Put or Append
class PutAppendArgs:
    # Add definitions here if needed
    def __init__(self, key, value):
        self.key = key
        self.value = value

class PutAppendReply:
    # Add definitions here if needed
    def __init__(self, value):
        self.value = value

class GetArgs:
    # Add definitions here if needed
    def __init__(self, key):
        self.key = key

class GetReply:
    # Add definitions here if needed
    def __init__(self, value):
        self.value = value

class KVServer:
    def __init__(self, cfg):
        self.mu = threading.Lock()
        self.cfg = cfg
        self.kv = {}

    def Get(self, args: GetArgs):
        with self.mu:
            value = self.kv.get(args.key, "")
        return GetReply(value)

    def Put(self, args: PutAppendArgs):
        with self.mu:
            self.kv[args.key] = args.value
        return PutAppendReply(args.value)

    def Append(self, args: PutAppendArgs):
        with self.mu:
            prev = self.kv.get(args.key, "")
            self.kv[args.key] = prev + args.value
        return PutAppendReply(prev)
