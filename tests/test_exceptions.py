from dagfactory.exceptions import DagFactoryConfigException, DagFactoryException


def test_config_exception_is_a_dagfactory_exception():
    # DagFactoryException documents itself as the base class for all
    # dag-factory errors, so `except DagFactoryException` has to catch
    # config errors too.
    assert issubclass(DagFactoryConfigException, DagFactoryException)

    try:
        raise DagFactoryConfigException("boom")
    except DagFactoryException as exc:
        assert str(exc) == "boom"
