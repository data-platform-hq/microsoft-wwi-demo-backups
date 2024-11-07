import pytest

from data_product_microsoft_wwi.loaders import DeltaReader
from data_product_microsoft_wwi.loaders.readers import DataReaderAbstract


def test_reader_create():
    reader0 = DeltaReader('', '', '', '')
    reader1 = DeltaReader('', '', '', '')
    assert reader0 == reader1


def test_try_to_create_abstract_instance():
    with pytest.raises(TypeError):
        _ = DataReaderAbstract()
