"""Module contains exceptions for dag-factory"""


class DagFactoryException(Exception):
    """
    Base class for all dag-factory errors.
    """


class DagFactoryConfigException(DagFactoryException):
    """
    Raise for dag-factory config errors.
    """
