"""
    Exceptions used with Quantnet.

    The base exception class is :class:`. QuantnetException`.
    Exceptions which are raised are all subclasses of it.

"""


class QuantnetException(Exception):
    """
    To correctly use this class, inherit from it and define
    a 'message' property. That message will get printf'd
    with the keyword arguments provided to the constructor.
    """

    def __init__(self, *args, **kwargs):
        super(QuantnetException, self).__init__(*args, **kwargs)
        self._message = "An unknown exception occurred."
        self.args = args
        self.kwargs = kwargs
        self.error_code = 1
        self._error_string = None

    def __str__(self):
        try:
            self._error_string = self._message % self.kwargs
        except Exception:
            self._error_string = self._message
        if len(self.args) > 0:
            args = [f"{arg}" for arg in self.args if arg]
            self._error_string = (self._error_string + f"\nDetails: \n {args}")
        return self._error_string.strip()


class InvalidType(QuantnetException):
    """
    QuantnetException
    """

    def __init__(self, *args, **kwargs):
        super(InvalidType, self).__init__(*args, **kwargs)
        self._message = "Provided type is considered invalid."
        self.error_code = 1034


class NodeNotFound(QuantnetException):
    """
    QuantnetException
    """

    def __init__(self, *args, **kwargs):
        super(NodeNotFound, self).__init__(*args, **kwargs)
        self._message = "Provided node is not found."
        self.error_code = 1035


class Duplicate(QuantnetException):
    """
    QuantnetException
    """

    def __init__(self, *args, **kwargs):
        super(Duplicate, self).__init__(*args, **kwargs)
        self._message = "Provided object is duplicate."
        self.error_code = 1036


class DatabaseException(QuantnetException):
    """
    QuantnetException
    """

    def __init__(self, *args, **kwargs):
        super(DatabaseException, self).__init__(*args, **kwargs)
        self._message = "Database exception."
        self.error_code = 1037
