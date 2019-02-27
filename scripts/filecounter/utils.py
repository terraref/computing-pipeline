import datetime
import functools

import flask

# Stores a dict of users, passwords and roles. Expected data is:
# {
#     'bob': {
#         'password': 'secret',
#         'roles': ['admin', 'viewer']
#     }
# }
users = {
    'admin': {
        'password': 'secret',
        'roles': ['admin', 'viewer']
    },
    'viewer': {
        'password': 'secret',
        'roles': ['viewer']
    }
}


def find_item(where, what):
    """
    Tries to locate the item with either the id, or checks to see if it matches the name in the dict.

    :param where: dict to to be searched
    :param what: what to look for
    :return: a tuple of the key and item found, or (None, None)
    """
    value = where.get(what, None)
    if value:
        return what, value
    if len(what) > 10:
        key = what[:10]
        if key in where:
            return key, where[key]
    for k, v in where.items():
        if 'name' in v and v['name'] == what:
            return k, v
    return None, None


def get_item(where, key, defaultvalue=None):
    """
    Finds the key in the dict. The key can be made up of multiple pieces seperated with a period.

    :param where: dict to search
    :param key: multiple keywords combined with a period
    :param defaultvalue: if the key is not found return this value
    :return: either the defaultvalue or the value found
    """
    x = where
    for k in key.split("."):
        if k in x:
            x = x[k]
        else:
            return defaultvalue
    return x


def get_timestamp():
    """
    Generates a consistent timestamp. Timestamp is in ISO-8601 at UTC

    :return: 8601 timestamp formated in UTC.
    """
    return datetime.datetime.utcnow().isoformat() + "Z"


def check_auth(username, password):
    """
    Checks if the given username and password match the know list of users and passwords.

    :param username: user to be checked
    :param password: password to be checked
    :return: true if the user exists and their password matches
    """
    found = users.get(username, None)
    if found and 'password' in found and found['password'] == password:
        flask.g.user = username
        flask.g.roles = found['roles']
        return True
    else:
        flask.g.user = None
        flask.g.roles = None
        return False


def requires_user(*users):
    """
    Annotation to be added to functions to see if there is given a user and matches the list of users.

    :param users: the list of acceptable users
    """
    def wrapper(f):
        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            auth = flask.request.authorization
            if not auth or not check_auth(auth.username, auth.password):
                return flask.Response(headers={'WWW-Authenticate': 'Basic realm="swarmstats"'}, status=401)
            elif auth.username not in users:
                return flask.abort(403)
            else:
                return f(*args, **kwargs)
        return wrapped
    return wrapper