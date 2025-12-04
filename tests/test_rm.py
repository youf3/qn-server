import pytest
from quantnet_controller.core import ResourceManager, DBmodel
from . import QuantnetTest


class TestRM(QuantnetTest):

    @pytest.fixture(autouse=True)
    def setup(self):
        super().setup()
        self.rm = ResourceManager()

    @pytest.fixture
    def nodes(self):
        yield self.add_nodes()
        self.db.drop(DBmodel.Node)

    def test_get_no_nodes(self):
        with pytest.raises(Exception) as context:
            self.rm.get_nodes("LBNL-Q")
        assert ('Node not found' in str(context))

    def test_get_nodes(self, nodes):
        result = self.rm.get_nodes("LBNL-Q", "LBNL-BSM")
        assert (len(result) == 2)

    def test_find_no_nodes(self):
        result = self.rm.find_nodes()
        assert (result == [])

    def test_find_nodes(self, nodes):
        results = self.rm.find_nodes()
        assert (isinstance(results, list) and len(results) > 0)
