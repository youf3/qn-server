"""
Pingpong
"""

from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING

from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import exc
from sqlalchemy.sql.expression import or_  # , and_, false, func, case, select

from quantnet_controller.common import exception
from quantnet_controller.db.sqla import models
from quantnet_controller.db.sqla.constants import NodeStatus, NodeType
from quantnet_controller.db.sqla.session import read_session, transactional_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class PingPong:
    def add(self, data, **kwargs):
        return add_pingpong(**data, **kwargs)

    def list(self, **kwargs):
        return list_pingpongs(**kwargs)

    def get(self, id, **kwargs):
        return get_pingpong(id, **kwargs)

    def update(self, id, key, value, **kwargs):
        return update_pingpong(id, key, value, **kwargs)

    def delete(self, id, **kwargs):
        return del_pingpong(id, **kwargs)

    def exist(self, id, **kwargs):
        return pingpong_exists(id, **kwargs)


@transactional_session
def add_pingpong(id, remote=None, phase=None, reason=None, iterations=None, *, session: "Session"):
    """ Add a pingpong record with the given src, dst and power.

    :param id: the name of the new pingpong.
    :param name: the name of the new pingpong.
    :param type_: the type of the new pingpong.
    :param email: The Email address associated with the pingpong.
    :param session: the database session in use.
    """

    try:
        new_node = models.PingPong(
            id=id,
            status=NodeStatus.ACTIVE,
            remote=remote,
            reason=reason,
            iterations=iterations,
            phase="start")
    except Exception as e:
        raise e

    try:
        new_node.save(session=session)
    except IntegrityError:
        raise exception.Duplicate(f'PingPong ID {id} already exists!')
    except OperationalError as e:
        raise exception.DatabaseException(e.args)
    except Exception as e:
        raise e

    return id, remote, phase, reason


@read_session
def pingpong_exists(id, include_deleted=False, *, session: "Session"):
    """ Checks to see if a pingpong exists.

    :param id: ID of the pingpong.
    :param session: the database session in use.

    :returns: True if found, otherwise false.
    """

    if include_deleted is True:
        query = session.query(models.PingPong).filter_by(id=id)
    else:
        query = session.query(models.PingPong).filter_by(id=id, status=NodeStatus.ACTIVE)

    try:
        ret = query.first()
    except Exception:
        ret = None
    finally:
        return True if ret else False


@read_session
def get_pingpong(id, *, session: "Session", **kwargs):
    """ Returns an pingpong for the given id.

    :param id: the id of the pingpong.
    :param session: the database session in use.

    :returns: a dict with all information for the pingpong.
    """

    query = session.query(models.PingPong).filter_by(id=id) if id else session.query(models.PingPong)
    for k, v in kwargs.items():
        query = query.filter_by(**{k: kwargs.get(k)})

    try:
        result = query.one()
    except exc.NoResultFound:
        raise exception.NodeNotFound(f'pingpong with ID {id} cannot be found')

    pingpong = {"remote": result.remote,
                "phase": result.phase,
                "reason": result.reason,
                "iterations": result.iterations,
                "id": result.id,
                "created_at": str(result.created_at),
                "updated_at": str(result.updated_at)
                }

    return pingpong


@transactional_session
def del_pingpong(id, *, session: "Session"):
    """ Disable a pingpong with the given id.

    :param id: the pingpong id.
    :param session: the database session in use.
    """
    query = session.query(models.PingPong).filter_by(id=id).filter_by(status=NodeStatus.ACTIVE)
    try:
        pingpong = query.one()
    except exc.NoResultFound:
        raise exception.NodeNotFound(f'pingpong with ID {id} cannot be found')

    pingpong.update({'status': NodeStatus.DELETED, 'deleted_at': datetime.utcnow()})


@transactional_session
def update_pingpong(id, key, value, *, session: "Session", **kwargs):
    """ Update a property of a pingpong.

    :param id: ID of the pingpong.
    :param key: pingpong property like status.
    :param value: Property value.
    :param session: the database session in use.
    """

    query = session.query(models.PingPong).filter_by(id=id) if id else session.query(models.PingPong)
    for k, v in kwargs.items():
        query = query.filter_by(**{k: kwargs.get(k)})
    try:
        query = query.one()
    except exc.NoResultFound:
        raise exception.NodeNotFound(f'pingpong with ID {id} cannot be found')
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
def list_pingpongs(filter_=None, include_deleted=False, order=False, *, session: "Session"):
    """ Returns a list of all pingpong names.

    :param filter_: Dictionary of attributes by which the input data should be filtered
    :param session: the database session in use.

    returns: a list of all pingpong names.
    """
    if filter_ is None:
        filter_ = {}

    if include_deleted:
        query = session.query(models.PingPong)
    else:
        query = session.query(models.PingPong).filter_by(status=NodeStatus.ACTIVE)

    if order:
        condition = []
        query.filter(or_(*condition)).order_by(models.PingPong.created_at.asc())

    for filter_type in filter_:
        if filter_type == 'pingpong_type':
            if isinstance(filter_['pingpong_type'], str):
                query = query.filter_by(pingpong_type=NodeType[filter_['pingpong_type']])
            elif isinstance(filter_['pingpong_type'], Enum):
                query = query.filter_by(pingpong_type=filter_['pingpong_type'])

        elif filter_type == 'id':
            if '*' in filter_['id'].internal:
                account_str = filter_['id'].internal.replace('*', '%')
                query = query.filter(models.pingpong.id.like(account_str))
            else:
                query = query.filter_by(id=filter_['id'])
        else:
            query = query.join(models.pingpongAttr, models.pingpong.id == models.pingpongAttr.pingpong_id).\
                filter(models.pingpongAttr.key == filter_type).\
                filter(models.pingpongAttr.value == filter_[filter_type])

    pingpong_list = []
    for row in query:
        pingpong = get_pingpong(row.id)
        pingpong_list.append(pingpong)

    return pingpong_list
