#!/usr/bin/env python
"""

Copyright (c) 2007  Dustin Sallings <dustin@spy.net>
"""

import struct

# Command constants
CMD_GET = 0
CMD_SET = 1
CMD_SETQ = 0x11
CMD_ADD = 2
CMD_REPLACE = 3
CMD_DELETE = 4
CMD_INCR = 5
CMD_DECR = 6
CMD_QUIT = 7
CMD_FLUSH = 8
CMD_GETQ = 9
CMD_NOOP = 10
CMD_VERSION = 11
CMD_STAT = 0x10
CMD_APPEND = 0x0e
CMD_PREPEND = 0x0f
CMD_TOUCH = 0x1c
CMD_GAT = 0x1d
CMD_GET_REPLICA = 0x83
CMD_OBSERVE = 0x92

# UPR command opcodes
CMD_OPEN             = 0x50
CMD_ADD_STREAM       = 0x51
CMD_CLOSE_STREAM     = 0x52
CMD_STREAM_REQ       = 0x53
CMD_GET_FAILOVER_LOG = 0x54
CMD_STREAM_END       = 0x55
CMD_SNAPSHOT_MARKER  = 0x56
CMD_MUTATION         = 0x57
CMD_DELETION         = 0x58
CMD_EXPIRATION       = 0x59
CMD_FLUSH            = 0x5a
CMD_SET_VB_STATE     = 0x5b
CMD_UPRNOOP          = 0x5c
CMD_ACK              = 0x5d
CMD_FLOW_CONTROL     = 0x5e
CMD_UPR_NOOP         = 0x5c
CMD_UPR_ACK          = 0x5d
CMD_CONTROL     = 0x5e

# SASL stuff
CMD_SASL_LIST_MECHS = 0x20
CMD_SASL_AUTH = 0x21
CMD_SASL_STEP = 0x22

# Bucket extension
CMD_CREATE_BUCKET = 0x85
CMD_DELETE_BUCKET = 0x86
CMD_LIST_BUCKETS = 0x87
CMD_EXPAND_BUCKET = 0x88
CMD_SELECT_BUCKET = 0x89

CMD_STOP_PERSISTENCE = 0x80
CMD_START_PERSISTENCE = 0x81
CMD_SET_FLUSH_PARAM = 0x82

CMD_SET_TAP_PARAM = 0x92
CMD_EVICT_KEY = 0x93

CMD_RESTORE_FILE = 0x98
CMD_RESTORE_ABORT = 0x99
CMD_RESTORE_COMPLETE = 0x9a

#Online update
CMD_START_ONLINEUPDATE = 0x9b
CMD_COMPLETE_ONLINEUPDATE = 0x9c
CMD_REVERT_ONLINEUPDATE = 0x9d

# TAP client registration
CMD_DEREGISTER_TAP_CLIENT = 0x9e

# Reset replication chain
CMD_RESET_REPLICATION_CHAIN = 0x9f

CMD_GET_META = 0xa0
CMD_GETQ_META = 0xa1

# Replication
CMD_TAP_CONNECT = 0x40
CMD_TAP_MUTATION = 0x41
CMD_TAP_DELETE = 0x42
CMD_TAP_FLUSH = 0x43
CMD_TAP_OPAQUE = 0x44
CMD_TAP_VBUCKET_SET = 0x45
CMD_TAP_CHECKPOINT_START = 0x46
CMD_TAP_CHECKPOINT_END = 0x47

# vbucket stuff
CMD_SET_VBUCKET_STATE = 0x3d
CMD_GET_VBUCKET_STATE = 0x3e
CMD_DELETE_VBUCKET = 0x3f

CMD_GET_LOCKED = 0x94

CMD_SYNC = 0x96


CMD_SET_DRIFT_COUNTER_STATE = 0xc1
CMD_GET_ADJUSTED_TIME = 0xc2


# event IDs for the SYNC command responses
CMD_SYNC_EVENT_PERSISTED = 1
CMD_SYNC_EVENT_MODIFED = 2
CMD_SYNC_EVENT_DELETED = 3
CMD_SYNC_EVENT_REPLICATED = 4
CMD_SYNC_INVALID_KEY = 5
CMD_SYNC_INVALID_CAS = 6

VB_STATE_ACTIVE = 1
VB_STATE_REPLICA = 2
VB_STATE_PENDING = 3
VB_STATE_DEAD = 4
VB_STATE_NAMES = {'active': VB_STATE_ACTIVE,
                'replica': VB_STATE_REPLICA,
                'pending': VB_STATE_PENDING,
                'dead': VB_STATE_DEAD}

# Parameter types of CMD_SET_PARAM command.
ENGINE_PARAM_FLUSH = 1
ENGINE_PARAM_TAP = 2
ENGINE_PARAM_CHECKPOINT = 3

COMMAND_NAMES = dict(((globals()[k], k) for k in globals() if k.startswith("CMD_")))

# TAP_OPAQUE types
TAP_OPAQUE_ENABLE_AUTO_NACK = 0
TAP_OPAQUE_INITIAL_VBUCKET_STREAM = 1
TAP_OPAQUE_ENABLE_CHECKPOINT_SYNC = 2
TAP_OPAQUE_OPEN_CHECKPOINT = 3

# TAP connect flags
TAP_FLAG_BACKFILL = 0x01
TAP_FLAG_DUMP = 0x02
TAP_FLAG_LIST_VBUCKETS = 0x04
TAP_FLAG_TAKEOVER_VBUCKETS = 0x08
TAP_FLAG_SUPPORT_ACK = 0x10
TAP_FLAG_REQUEST_KEYS_ONLY = 0x20
TAP_FLAG_CHECKPOINT = 0x40
TAP_FLAG_REGISTERED_CLIENT = 0x80

TAP_FLAG_TYPES = {TAP_FLAG_BACKFILL: ">Q",
                  TAP_FLAG_REGISTERED_CLIENT: ">B"}

# TAP per-message flags
TAP_FLAG_ACK = 0x01
TAP_FLAG_NO_VALUE = 0x02 # The value for the key is not included in the packet

# UPR per-message flags
FLAG_OPEN_CONSUMER = 0x00
FLAG_OPEN_PRODUCER = 0x01
FLAG_OPEN_NOTIFIER = 0x02

#CCCP
CMD_SET_CLUSTER_CONFIG = 0xb4
CMD_GET_CLUSTER_CONFIG = 0xb5

# Flags, expiration
SET_PKT_FMT = ">II"

# flags
GET_RES_FMT = ">I"

# How long until the deletion takes effect.
DEL_PKT_FMT = ""

## TAP stuff
# eng-specific length, flags, ttl, [res, res, res]; item flags, exp
TAP_MUTATION_PKT_FMT = ">HHbxxxII"
TAP_GENERAL_PKT_FMT = ">HHbxxx"

# amount, initial value, expiration
INCRDECR_PKT_FMT = ">QQI"
# Special incr expiration that means do not store
INCRDECR_SPECIAL = 0xffffffff
INCRDECR_RES_FMT = ">Q"

# Time bomb
FLUSH_PKT_FMT = ">I"

# Touch commands
# expiration
TOUCH_PKT_FMT = ">I"
GAT_PKT_FMT = ">I"
GETL_PKT_FMT = ">I"

# 2 bit integer.  :/
VB_SET_PKT_FMT = ">I"

SET_DRIFT_COUNTER_STATE_REQ_FMT = '>qB'
GET_ADJUSTED_TIME_RES_FMT = '>Q'

MAGIC_BYTE = 0x80
REQ_MAGIC_BYTE = 0x80
RES_MAGIC_BYTE = 0x81

# magic, opcode, keylen, extralen, datatype, vbucket, bodylen, opaque, cas
REQ_PKT_FMT = ">BBHBBHIIQ"
# magic, opcode, keylen, extralen, datatype, status, bodylen, opaque, cas
RES_PKT_FMT = ">BBHBBHIIQ"
# min recv packet size
MIN_RECV_PACKET = struct.calcsize(REQ_PKT_FMT)
# The header sizes don't deviate
assert struct.calcsize(REQ_PKT_FMT) == struct.calcsize(RES_PKT_FMT)

EXTRA_HDR_FMTS = {
    CMD_SET: SET_PKT_FMT,
    CMD_ADD: SET_PKT_FMT,
    CMD_REPLACE: SET_PKT_FMT,
    CMD_INCR: INCRDECR_PKT_FMT,
    CMD_DECR: INCRDECR_PKT_FMT,
    CMD_DELETE: DEL_PKT_FMT,
    CMD_FLUSH: FLUSH_PKT_FMT,
    CMD_TAP_MUTATION: TAP_MUTATION_PKT_FMT,
    CMD_TAP_DELETE: TAP_GENERAL_PKT_FMT,
    CMD_TAP_FLUSH: TAP_GENERAL_PKT_FMT,
    CMD_TAP_OPAQUE: TAP_GENERAL_PKT_FMT,
    CMD_TAP_VBUCKET_SET: TAP_GENERAL_PKT_FMT,
    CMD_SET_VBUCKET_STATE: VB_SET_PKT_FMT,
}

EXTRA_HDR_SIZES = dict(
    [(k, struct.calcsize(v)) for (k, v) in EXTRA_HDR_FMTS.items()])

ERR_NOT_FOUND = 0x01
ERR_EXISTS = 0x02
ERR_2BIG = 0x03
ERR_EINVAL = 0x04
ERR_NOT_STORED = 0x05
ERR_BAD_DELTA = 0x06
ERR_NOT_MY_VBUCKET = 0x07
ERR_AUTH = 0x20
ERR_AUTH_CONTINUE = 0x21
ERR_UNKNOWN_CMD = 0x81
ERR_ENOMEM = 0x82
ERR_NOT_SUPPORTED = 0x83
ERR_EINTERNAL = 0x84
ERR_EBUSY = 0x85
ERR_ETMPFAIL = 0x86

META_REVID = 0x01
