import wurmstore
from wurmstore import MemoryWurm

def test_thing():
    assert True

def no():
    assert False

def test_other():
    a = 1
    b = 3
    assert a == a

def test_imported():
    assert MemoryWurm