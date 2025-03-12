import json
import datetime
import pickle
import dill
import pytest
from pathlib import Path

from PersistDict import PersistDict


@pytest.fixture
def db_path():
    """Fixture to provide a test database path."""
    return Path("test_db")


@pytest.fixture
def clean_db(db_path):
    """Fixture to ensure a clean database for each test."""
    if db_path.exists():
        for f in db_path.iterdir():
            f.unlink(missing_ok=True)
        db_path.rmdir()
    yield db_path
    # Cleanup after test
    if db_path.exists():
        for f in db_path.iterdir():
            f.unlink(missing_ok=True)
        db_path.rmdir()


def delete_dir(dbp):
    if not dbp.exists():
        return
    assert dbp.is_dir()
    for f in dbp.iterdir():
        f.unlink(missing_ok=True)
    dbp.rmdir()


@pytest.mark.parametrize(
    "reset,key_serializers,value_serializers",
    [
        (reset, key_ser, val_ser)
        for reset in ["clear", "delete", "clear"]
        for key_ser in [(None, None), (json.dumps, json.loads)]
        for val_ser in [(pickle.dumps, pickle.loads), (dill.dumps, dill.loads)]
    ]
)
def test_persistdict_scenarios(clean_db, reset, key_serializers, value_serializers):
    """Test PersistDict with various serialization options."""
    key_serializer, key_unserializer = key_serializers
    value_serializer, value_unserializer = value_serializers
    
    kwargs = {
        "database_path": clean_db,
        "verbose": True,
        "key_serializer": key_serializer,
        "key_unserializer": key_unserializer,
        "value_serializer": value_serializer,
        "value_unserializer": value_unserializer,
    }
    
    if reset == "delete":
        delete_dir(clean_db)
        inst = PersistDict(**kwargs)
    elif reset == "clear":
        inst = PersistDict(**kwargs)
        inst.clear()

    assert inst.info_db["already_called"] is False, dict(inst.info_db)
    assert len(inst) == 0, inst
    d = inst(a=1, b="b", c=str)

    assert len(d) == 3, d
    assert d["a"] == 1, d
    assert d["b"] == "b", d
    assert d["c"] is str, d
    assert sorted(list(d.keys())) == ["a", "b", "c"], d

    del d["b"]
    assert list(d.keys()) == ["a", "c"], d
    assert len(d) == 2, d
    assert d.__repr__() == {"a": 1, "c": str}.__repr__(), d.__repr__()
    assert d.__str__() == {"a": 1, "c": str}.__str__(), d.__str__()

    assert isinstance(d, dict)

    d["d"] = None

    assert [d["c"], d["d"], d["a"]] == [str, None, 1]

    d["a"] = 1
    d["b"] = 2
    d["c"] = 3
    d["d"] = 4
    assert [d["c"], d["d"], d["a"], d["b"]] == [3, 4, 1, 2]

    del d["c"], d["a"]
    assert [d["b"], d["d"]] == [2, 4], d
    assert len(d) == 2, d

    d2 = PersistDict(**kwargs)
    assert list(d.keys()) == list(d2.keys()), d2

    d2["0"] = None
    assert d["0"] is None
    del d["0"]
    assert "0" not in d2, d2


def test_call_once(clean_db):
    """Test that __call__ can only be called once."""
    inst = PersistDict(database_path=clean_db, verbose=True)
    first = inst()
    assert len(first) == 0
    
    with pytest.raises(Exception) as excinfo:
        inst()()
    assert str(excinfo.value) == "The __call__ method of PersistDict can only be called once. Just like a regular dict."


def test_expiration_and_serialization(clean_db):
    """Test expiration functionality and custom serialization."""
    inst = PersistDict(
        database_path=clean_db,
        verbose=True,
        expiration_days=7,
        value_serializer=None,
        value_unserializer=None,
    )
    assert len(inst) == 0
    inst["test"] = "value"
    assert len(inst) == 1
    assert inst["test"] == "value"
    assert inst.key_serializer(inst.hash_and_crop("test")) in inst.metadata_db
    
    # Test item not expired after 1 day
    inst.metadata_db[inst.key_serializer(inst.hash_and_crop("test"))]["atime"] = datetime.datetime.now() - datetime.timedelta(days=1)
    assert len(inst) == 1
    inst.__expire__()
    assert len(inst) == 1
    
    # Test item expired after 14 days
    inst.metadata_db[inst.key_serializer(inst.hash_and_crop("test"))]["atime"] = datetime.datetime.now() - datetime.timedelta(days=14)
    assert len(inst) == 1
    inst.__expire__()
    assert len(inst) == 0
