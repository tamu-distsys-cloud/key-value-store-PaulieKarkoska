import logging
import threading
from typing import Tuple, Any

logging.basicConfig(filename='kvserver_debug.log', level=logging.DEBUG, format='%(asctime)s %(message)s')

# Put or Append
class PutAppendArgs:
    def __init__(self, key, value):
        self.key = key
        self.value = value
        self.client_id = None
        self.seq = None

class PutAppendReply:
    def __init__(self, value, err=""):
        self.value = value if value is not None else ""
        self.err = err

class GetArgs:
    def __init__(self, key):
        self.key = key
        self.client_id = None
        self.seq = None

class GetReply:
    def __init__(self, value, err=""):
        self.value = value if value is not None else ""
        self.err = err

class KVServer:
    def __init__(self, cfg, my_index):
        self.cfg = cfg
        self.my_index = my_index
        self.nshards = getattr(cfg, 'nservers', 1)
        self.nreplicas = getattr(cfg, 'nreplicas', 1)
        self._kv = {}  # shard_id -> {key: value}
        self._locks = {}  # shard_id -> threading.Lock()
        self._dedup = {}  # shard_id -> {client_id: (seq, (value, err))}

    def _get_shard_id(self, key):
        try:
            idx = int(key) % self.nshards
        except Exception:
            idx = 0  # fallback, but tests expect int(key)
        return idx

    def _primary_for_shard(self, shard_id):
        return shard_id

    def _is_primary(self, shard_id):
        return self.my_index == self._primary_for_shard(shard_id)

    def _get_shard_state(self, shard_id):
        if shard_id not in self._kv:
            self._kv[shard_id] = {}
            self._locks[shard_id] = threading.Lock()
            self._dedup[shard_id] = {}
        return self._kv[shard_id], self._dedup[shard_id], self._locks[shard_id]

    def _forward_to_primary(self, method, args, shard_id):
        # Only forward if not primary and in a replicated config
        if self._is_primary(shard_id):
            return None
        # Forward to the primary server for this shard
        if hasattr(self.cfg, 'kvservers') and self.cfg.kvservers is not None:
            primary = self._primary_for_shard(shard_id)
            primary_srv = self.cfg.kvservers[primary]
            if primary_srv is not None:
                return getattr(primary_srv, method)(args)
        return None

    def Get(self, args: GetArgs):
        logging.debug(f"Get called: key={args.key}, client_id={getattr(args, 'client_id', None)}, seq={getattr(args, 'seq', None)}")
        shard_id = self._get_shard_id(args.key)
        kv, dedup, mu = self._get_shard_state(shard_id)
        if not self._is_primary(shard_id):
            # Forward to primary for linearizability
            reply = self._forward_to_primary('Get', args, shard_id)
            if reply is not None:
                return reply
            return GetReply("", err="ErrWrongGroup")
        with mu:
            client_id = getattr(args, 'client_id', None)
            seq = getattr(args, 'seq', None)
            if client_id is not None:
                if client_id in dedup and dedup[client_id][0] == seq:
                    value, err = dedup[client_id][1]
                    logging.debug(f"Get: dedup hit for {client_id}, value={value}, err={err}")
                    return GetReply(str(value) if value is not None else "", err=str(err) if err is not None else "")
            value = kv.get(args.key, "")
            reply = (str(value) if value is not None else "", "")
            if client_id is not None:
                dedup[client_id] = (seq, reply)
                # Free dedup memory for previous seqs
                for cid in list(dedup.keys()):
                    if cid == client_id and dedup[cid][0] < seq:
                        del dedup[cid]
            logging.debug(f"Get: returning value={reply[0]}, err={reply[1]}")
            return GetReply(reply[0], err=reply[1])

    def Put(self, args: PutAppendArgs):
        logging.debug(f"Put called: key={args.key}, value={args.value}, client_id={getattr(args, 'client_id', None)}, seq={getattr(args, 'seq', None)}")
        shard_id = self._get_shard_id(args.key)
        kv, dedup, mu = self._get_shard_state(shard_id)
        if not self._is_primary(shard_id):
            reply = self._forward_to_primary('Put', args, shard_id)
            if reply is not None:
                return reply
            return PutAppendReply("", err="ErrWrongGroup")
        with mu:
            client_id = getattr(args, 'client_id', None)
            seq = getattr(args, 'seq', None)
            if client_id is not None:
                if client_id in dedup and dedup[client_id][0] == seq:
                    value, err = dedup[client_id][1]
                    logging.debug(f"Put: dedup hit for {client_id}, value={value}, err={err}")
                    return PutAppendReply(str(value) if value is not None else "", err=str(err) if err is not None else "")
            kv[args.key] = str(args.value) if args.value is not None else ""
            reply = (kv[args.key], "")
            if client_id is not None:
                dedup[client_id] = (seq, reply)
                for cid in list(dedup.keys()):
                    if cid == client_id and dedup[cid][0] < seq:
                        del dedup[cid]
            logging.debug(f"Put: returning value={reply[0]}, err={reply[1]}")
            return PutAppendReply(reply[0], err=reply[1])

    def Append(self, args: PutAppendArgs):
        logging.debug(f"Append called: key={args.key}, value={args.value}, client_id={getattr(args, 'client_id', None)}, seq={getattr(args, 'seq', None)}")
        shard_id = self._get_shard_id(args.key)
        kv, dedup, mu = self._get_shard_state(shard_id)
        if not self._is_primary(shard_id):
            reply = self._forward_to_primary('Append', args, shard_id)
            if reply is not None:
                return reply
            return PutAppendReply("", err="ErrWrongGroup")
        with mu:
            client_id = getattr(args, 'client_id', None)
            seq = getattr(args, 'seq', None)
            if client_id is not None:
                if client_id in dedup and dedup[client_id][0] == seq:
                    value, err = dedup[client_id][1]
                    logging.debug(f"Append: dedup hit for {client_id}, value={value}, err={err}")
                    return PutAppendReply(str(value) if value is not None else "", err=str(err) if err is not None else "")
            prev = kv.get(args.key, "")
            new_value = (str(prev) if prev is not None else "") + (str(args.value) if args.value is not None else "")
            kv[args.key] = new_value
            reply = (prev, "")  # Return old value for append
            if client_id is not None:
                dedup[client_id] = (seq, reply)
                for cid in list(dedup.keys()):
                    if cid == client_id and dedup[cid][0] < seq:
                        del dedup[cid]
            logging.debug(f"Append: returning value={reply[0]}, err={reply[1]}")
            return PutAppendReply(reply[0], err=reply[1])
