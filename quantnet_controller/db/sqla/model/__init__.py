
"""
blob_listpyright ESnet 2023 -

"""
import json
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import exc
from sqlalchemy.sql.expression import or_  

from quantnet_controller.common import exception
from quantnet_controller.db.sqla import models
from quantnet_controller.db.sqla.constants import NodeStatus
from quantnet_controller.db.sqla.session import read_session, transactional_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class Blob:
    def __init__(self, model="blob"):
        self._blob_name = model

    def add(self, data, **kwargs):
        return add_blob(None, data)

    def find(self, **kwargs):
        return find_blobs(**kwargs)

    def get(self, id, **kwargs):
        """ Get the records from the table. It allows filtering based on "id"

        parameters:
        -----------
        id: str or dict
            either id string or filter dict

        """
        return get_blob(id, **kwargs)

    def update(self, id, key, value, **kwargs):
        return update_blob(id, key, value)

    def upsert(self, id, *args, **kwargs):
        return upsert_blob(id, *args, **kwargs)

    def delete(self, id, **kwargs):
        return del_blob(id)

    def exist(self, id, **kwargs):
        return blob_exists(id)

    def drop(self, **kwargs):
        return drop_blobs(**kwargs)


@transactional_session
def add_blob(id, blob: dict, *, session: "Session"):
    """ Add a blob record with the given src, dst and power.

    :param id: index string or None.
    :param session: the database session in use.
    """

    if not isinstance(blob, dict):
        raise Exception(f"Type error: {blob} is not dict")

    json_string = json.dumps(blob)
    byte_array = json_string.encode('utf-8')

    try:
        new_blob = models.Blob(
            id=id,
            status=NodeStatus.ACTIVE,
            data=byte_array)
    except Exception as e:
        raise e

    try:
        new_blob.save(session=session)
    except IntegrityError:
        raise exception.Duplicate(f'Blob ID {id} already exists!')
    except OperationalError as e:
        raise exception.DatabaseException(e.args)
    except Exception as e:
        raise e

    return blob


@read_session
def blob_exists(id, include_deleted=False, *, session: "Session"):
    """ Checks to see if a blob exists.

    :param id: index string or filter dict.
    :param session: the database session in use.

    :returns: True if found, otherwise false.
    """

    if include_deleted is True:
        query = session.query(models.Blob)
    else:
        query = session.query(models.Blob).filter_by(status=NodeStatus.ACTIVE)

    if isinstance(id, dict):
        criteria = id
        items = query.all()

        filtered_items = [item for item in items if all(
            json.loads(item.data).get(k) == v for k, v in criteria.items())]

        return True if filtered_items else False
    elif isinstance(id, str):
        result = query.filter_by(id=id).first()
        return True if result else False
    else:
        raise Exception("invalide arguments")


@read_session
def get_blob(id, *, session: "Session", **kwargs):
    """ Returns an blob for the given id.

    :param id: either the index string or filter dict
    :param session: the database session in use.

    :returns: a dict or list of dicts with all information.
    """

    if isinstance(id, dict):
        criteria = id
        query = session.query(models.Blob)
        items = query.all()

        filtered_items = [item for item in items if all(
            json.loads(item.data).get(k) == v for k, v in criteria.items())]

        data_set = []
        for item in filtered_items:
            data_dict = json.loads(item.data)
            data_set.append(data_dict)
        return data_set
    elif isinstance(id, str):
        query = session.query(models.Blob).filter_by(id=id)

        try:
            result = query.one()
        except exc.NoResultFound:
            raise exception.NodeNotFound(f'blob with ID {id} cannot be found')

        # Convert the byte array to a string
        json_string = result.data.decode('utf-8')

        # Convert the JSON string to a dictionary
        data_dict = json.loads(json_string)

        return data_dict
    else:
        raise Exception(f"{id} must be either dict or str")


@transactional_session
def del_blob(id, *, session: "Session"):
    """ Disable a blob with the given id.

    :param id: index string or dict.
    :param session: the database session in use.
    """

    if isinstance(id, str):
        query = session.query(models.Blob).filter_by(id=id).filter_by(status=NodeStatus.ACTIVE)
        filtered_items = query.all()
    elif isinstance(id, dict):
        criteria = id
        query = session.query(models.Blob).filter_by(status=NodeStatus.ACTIVE)
        items = query.all()

        filtered_items = [item for item in items if all(
            json.loads(item.data).get(k) == v for k, v in criteria.items())]
    else:
        raise Exception("invalid arguments")

    count = 0
    for item in filtered_items:
        item.update({'status': NodeStatus.DELETED, 'deleted_at': datetime.utcnow()})
        count = count + 1

    return count


def combine(*args):
    combined_dict = {}
    for d in args:
        combined_dict.update(d)
    return combined_dict


@transactional_session
def update_blob(id, key, value, *, session: "Session"):
    """ Update blobs with data in args based on the id.

    :param id: index str or filter dict.
    :param args: data to be updated.
    :param session: the database session in use.
    :return
    """

    delta_dict = {key: value} if key and value else {}
    if not delta_dict:
        return

    if isinstance(id, dict):
        criteria = id
        query = session.query(models.Blob)
        items = query.all()

        filtered_items = [item for item in items if all(
            json.loads(item.data).get(k) == v for k, v in criteria.items())]
    elif isinstance(id, str):
        query = session.query(models.Blob).filter_by(id=id)
        filtered_items = query.all()
    else:
        raise Exception("invalid arguments")

    if not filtered_items:
        return

    for item in filtered_items:
        data_dict = json.loads(item.data)

        data_dict.update(delta_dict)
        json_string = json.dumps(data_dict)
        byte_array = json_string.encode('utf-8')
        item.data = byte_array

        try:
            item.save(session=session)
        except IntegrityError:
            raise exception.Duplicate(f'Blob ID {id} already exists!')
        except OperationalError as e:
            raise exception.DatabaseException(e.args)
        except Exception as e:
            raise e

    return


@transactional_session
def upsert_blob(id, *args, session: "Session", **kwargs):
    """ Update or insert blobs with data in args based on the id.

    :param id: index str or filter dict.
    :param args: data to be updated.
    :param session: the database session in use.
    :return
    """

    delta_dict = combine(*args) if args else {}
    if not delta_dict:
        return

    if isinstance(id, dict):
        criteria = id
        query = session.query(models.Blob)
        items = query.all()

        filtered_items = [item for item in items if all(
            json.loads(item.data).get(k) == v for k, v in criteria.items())]
    elif isinstance(id, str):
        query = session.query(models.Blob).filter_by(id=id)
        filtered_items = query.all()
    else:
        raise Exception("invalid arguments")

    if not filtered_items:
        json_string = json.dumps(delta_dict)
        byte_array = json_string.encode('utf-8')

        try:
            new_blob = models.Blob(
                id=None,
                status=NodeStatus.ACTIVE,
                data=byte_array)
            new_blob.save(session=session)
        except IntegrityError:
            raise exception.Duplicate(f'Blob ID {id} already exists!')
        except OperationalError as e:
            raise exception.DatabaseException(e.args)
        except Exception as e:
            raise e

        return new_blob.id, delta_dict

    for item in filtered_items:
        data_dict = json.loads(item.data)

        data_dict.update(delta_dict)
        json_string = json.dumps(data_dict)
        byte_array = json_string.encode('utf-8')
        item.data = byte_array

        try:
            item.save(session=session)
        except IntegrityError:
            raise exception.Duplicate(f'Blob ID {id} already exists!')
        except OperationalError as e:
            raise exception.DatabaseException(e.args)
        except Exception as e:
            raise e

    return


@read_session
def find_blobs(filter=None, include_deleted=False, order=False, *, session: "Session"):
    """ Returns a list of all blobs that match filter

    :param filter_: Dictionary of attributes by which the input data should be filtered
    :param session: the database session in use.

    returns: a list of all blob names.
    """
    if filter is None:
        filter = {}

    if include_deleted:
        query = session.query(models.Blob)
    else:
        query = session.query(models.Blob).filter_by(status=NodeStatus.ACTIVE)

    if order:
        condition = []
        query.filter(or_(*condition)).order_by(models.Blob.created_at.asc())

    blob_list = []
    for row in query:
        blob = get_blob(row.id)
        blob_list.append(blob)

    return blob_list


@transactional_session
def drop_blobs(*args, session: "Session", **kwargs):
    session.query(models.Blob).delete()
