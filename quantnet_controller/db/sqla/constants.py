# -*- coding: utf-8 -*-

from datetime import datetime
from enum import Enum

# Individual constants

OBSOLETE = datetime(year=1970, month=1, day=1)  # Tombstone value to mark obsolete replicas


# The enum values below are the actual strings stored in the database -- these must be string types.
# This is done explicitly via values_callable to SQLAlchemy enums in models.py and alembic scripts,
# as overloading/overriding Python internal enums is discouraged.


class NodeStatus(Enum):
    ACTIVE = "ACTIVE"
    SUSPENDED = "SUSPENDED"
    DELETED = "DELETED"


class NodeType(Enum):
    NORMAL = 'NORMAL'
    QUANTUM = "QNode"
    BSM = "BSMNode"
    M = "MNode"
    SWITCH = 'OpticalSwitch'
    QR = 'QRepeater'
