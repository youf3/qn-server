"""
Calibration
"""

from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING

from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import exc
from sqlalchemy.sql.expression import or_

from quantnet_controller.common import exception
from quantnet_controller.db.sqla import models
from quantnet_controller.db.sqla.constants import NodeStatus, NodeType
from quantnet_controller.db.sqla.session import read_session, transactional_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class Calibration:
    def add(self, data, **kwargs):
        return add_calibration(**data, **kwargs)

    def list(self, **kwargs):
        return list_calibrations(**kwargs)

    def get(self, id, **kwargs):
        return get_calibration(id, **kwargs)

    def update(self, id, key, value, **kwargs):
        return update_calibration(id, key, value, **kwargs)

    def delete(self, id, **kwargs):
        return del_calibration(id, **kwargs)

    def exist(self, id, **kwargs):
        return calibration_exists(id, **kwargs)


@transactional_session
def add_calibration(id, src=None, dst=None, power=0.0, light=None, *, session: "Session"):
    """ Add a calibration record with the given src, dst and power.

    :param id: the name of the new calibration.
    :param name: the name of the new calibration.
    :param type_: the type of the new calibration.
    :param email: The Email address associated with the calibration.
    :param session: the database session in use.
    """

    try:
        new_node = models.Calibration(
            id=id,
            status=NodeStatus.ACTIVE,
            src=src,
            dst=dst,
            power=power,
            light=light,
            phase="init")
    except Exception as e:
        raise e

    try:
        new_node.save(session=session)
    except IntegrityError:
        raise exception.Duplicate(f'Calibration ID {id} already exists!')
    except OperationalError as e:
        raise exception.DatabaseException(e.args)
    except Exception as e:
        raise e

    return id


@read_session
def calibration_exists(id, include_deleted=False, *, session: "Session"):
    """ Checks to see if a calibration exists.

    :param id: ID of the calibration.
    :param session: the database session in use.

    :returns: True if found, otherwise false.
    """

    if include_deleted is True:
        query = session.query(models.Calibration).filter_by(id=id)
    else:
        query = session.query(models.Calibration).filter_by(id=id, status=NodeStatus.ACTIVE)

    try:
        ret = query.first()
    except Exception:
        ret = None
    finally:
        return True if ret else False


@read_session
def get_calibration(id, *, session: "Session"):
    """ Returns an calibration for the given id.

    :param id: the id of the calibration.
    :param session: the database session in use.

    :returns: a dict with all information for the calibration.
    """

    query = session.query(models.Calibration).filter_by(id=id)

    try:
        result = query.one()
    except exc.NoResultFound:
        raise exception.NodeNotFound(f'calibration with ID {id} cannot be found')

    calibration = {"src": result.src,
                   "dst": result.dst,
                   "phase": result.phase,
                   "power": result.power,
                   "light": result.light,
                   "cal_id": result.id,
                   "created_at": str(result.created_at) if "created_at" in result.keys() else "",
                   "deleted_at": str(result.deleted_at) if "deleted_at" in result.keys() else "",
                   "updated_at": str(result.updated_at) if "updated_at" in result.keys() else "",
                   }

    return calibration


@transactional_session
def del_calibration(id, *, session: "Session"):
    """ Disable a calibration with the given id.

    :param id: the calibration id.
    :param session: the database session in use.
    """
    query = session.query(models.Calibration).filter_by(id=id).filter_by(status=NodeStatus.ACTIVE)
    try:
        calibration = query.one()
    except exc.NoResultFound:
        raise exception.NodeNotFound(f'calibration with ID {id} cannot be found')

    calibration.update({'status': NodeStatus.DELETED, 'deleted_at': datetime.utcnow()})

    return True


@transactional_session
def update_calibration(id, key, value, *, session: "Session"):
    """ Update a property of a calibration.

    :param id: ID of the calibration.
    :param key: calibration property like status.
    :param value: Property value.
    :param session: the database session in use.
    """
    query = session.query(models.Calibration).filter_by(id=id)
    try:
        query = query.one()
    except exc.NoResultFound:
        raise exception.NodeNotFound(f'calibration with ID {id} cannot be found')
    if key == 'status':
        if isinstance(value, str):
            value = NodeStatus[value]
        if value == NodeStatus.SUSPENDED:
            query.update({'status': value, 'suspended_at': datetime.utcnow()})
        elif value == NodeStatus.ACTIVE:
            query.update({'status': value, 'suspended_at': None})
    else:
        query.update({key: value})

    return key, value


@read_session
def list_calibrations(filter_=None, include_deleted=False, order=False, *, session: "Session"):
    """ Returns a list of all calibration names.

    :param filter_: Dictionary of attributes by which the input data should be filtered
    :param session: the database session in use.

    returns: a list of all calibration names.
    """
    if filter_ is None:
        filter_ = {}

    if include_deleted:
        query = session.query(models.Calibration)
    else:
        query = session.query(models.Calibration).filter_by(status=NodeStatus.ACTIVE)

    if order:
        condition = []
        query.filter(or_(*condition)).order_by(models.Calibration.created_at.asc())

    for filter_type in filter_:
        if filter_type == 'calibration_type':
            if isinstance(filter_['calibration_type'], str):
                query = query.filter_by(calibration_type=NodeType[filter_['calibration_type']])
            elif isinstance(filter_['calibration_type'], Enum):
                query = query.filter_by(calibration_type=filter_['calibration_type'])

        elif filter_type == 'id':
            if '*' in filter_['id'].internal:
                account_str = filter_['id'].internal.replace('*', '%')
                query = query.filter(models.calibration.id.like(account_str))
            else:
                query = query.filter_by(id=filter_['id'])
        else:
            query = query.join(models.calibrationAttr, models.calibration.id == models.calibrationAttr.calibration_id).\
                filter(models.calibrationAttr.key == filter_type).\
                filter(models.calibrationAttr.value == filter_[filter_type])

    calibration_list = []
    for row in query:
        calibration = get_calibration(row.id)
        calibration_list.append(calibration)

    return calibration_list
