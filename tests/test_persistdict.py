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


def test_background_thread_disabled(clean_db):
    """Test PersistDict with background thread disabled."""
    # Create instance with background_thread="disabled"
    inst = PersistDict(
        database_path=clean_db,
        verbose=True,
        background_thread="disabled"
    )
    
    # Verify that background thread attributes are properly set
    assert inst.background_thread == "disabled"
    assert inst._bg_thread is None
    
    # Add some data and verify it works normally
    inst["key1"] = "value1"
    assert inst["key1"] == "value1"
    assert len(inst) == 1


def test_background_thread_foreground(clean_db):
    """Test PersistDict with background tasks running in foreground."""
    # Create instance with background_thread=False (runs in current thread)
    inst = PersistDict(
        database_path=clean_db,
        verbose=True,
        background_thread=False
    )
    
    # Verify that background thread attributes are properly set
    assert inst.background_thread is False
    assert inst._bg_thread is None
    
    # Add some data and verify it works normally
    inst["key1"] = "value1"
    assert inst["key1"] == "value1"
    assert len(inst) == 1


def test_background_thread_enabled(clean_db):
    """Test PersistDict with background thread enabled."""
    # Create instance with background_thread=True
    inst = PersistDict(
        database_path=clean_db,
        verbose=True,
        background_thread=True,
        background_timeout=5
    )
    
    # Verify that background thread attributes are properly set
    assert inst.background_thread is True
    
    # Wait for background thread to complete
    if inst._bg_thread and inst._bg_thread.is_alive():
        inst._bg_task_complete.wait(timeout=10)
    
    # Add some data and verify it works normally
    inst["key1"] = "value1"
    assert inst["key1"] == "value1"
    assert len(inst) == 1
    
    # Test cleanup
    inst._stop_background_thread()
    assert inst._bg_thread is None


def test_minimal_locking(clean_db):
    """Test PersistDict with minimal locking enabled and disabled."""
    # Test with minimal_locking=True (default)
    inst1 = PersistDict(
        database_path=clean_db,
        verbose=True,
        minimal_locking=True
    )
    assert inst1.minimal_locking is True
    
    # Add and retrieve data
    inst1["key1"] = "value1"
    assert inst1["key1"] == "value1"
    assert len(inst1) == 1
    
    # Clear and recreate with minimal_locking=False
    inst1.clear()
    
    inst2 = PersistDict(
        database_path=clean_db,
        verbose=True,
        minimal_locking=False
    )
    assert inst2.minimal_locking is False
    
    # Add and retrieve data
    inst2["key2"] = "value2"
    assert inst2["key2"] == "value2"
    assert len(inst2) == 1


def test_custom_name(clean_db):
    """Test PersistDict with custom name identifier."""
    # Create instance with custom name
    custom_name = "TestDictInstance"
    inst = PersistDict(
        database_path=clean_db,
        verbose=True,
        name=custom_name
    )
    
    # Verify name is set correctly
    assert inst.name == custom_name
    
    # Add some data and verify it works normally
    inst["key1"] = "value1"
    assert inst["key1"] == "value1"
    assert len(inst) == 1


def test_default_name(clean_db):
    """Test PersistDict with default name (database path name)."""
    # Create instance without specifying name
    inst = PersistDict(
        database_path=clean_db,
        verbose=True
    )
    
    # Verify name defaults to database path name
    assert inst.name == clean_db.name
    
    # Add some data and verify it works normally
    inst["key1"] = "value1"
    assert inst["key1"] == "value1"
    assert len(inst) == 1


def test_background_timeout(clean_db):
    """Test PersistDict background_timeout parameter."""
    # Create instance with custom background_timeout
    custom_timeout = 60
    inst = PersistDict(
        database_path=clean_db,
        verbose=True,
        background_timeout=custom_timeout
    )
    
    # Verify timeout is set correctly
    assert inst.background_timeout == custom_timeout
    
    # Test with minimum timeout enforcement
    min_inst = PersistDict(
        database_path=clean_db,
        verbose=True,
        background_timeout=1  # Less than minimum (5)
    )
    
    # Should enforce minimum of 5
    assert min_inst.background_timeout >= 5


def test_thread_safety(clean_db):
    """Test thread safety of PersistDict operations."""
    import threading
    import random
    
    # Create instance
    inst = PersistDict(
        database_path=clean_db,
        verbose=True
    )
    
    # Number of operations per thread
    num_ops = 50
    # Number of threads
    num_threads = 5
    
    # Function to run in threads
    def thread_func(thread_id):
        for i in range(num_ops):
            key = f"key_{thread_id}_{i}"
            value = f"value_{thread_id}_{i}"
            
            # Randomly choose an operation
            op = random.choice(["set", "get", "contains", "len"])
            
            if op == "set":
                inst[key] = value
            elif op == "get":
                # Only try to get if we've set it
                if i > 0:
                    prev_key = f"key_{thread_id}_{i-1}"
                    if prev_key in inst:
                        _ = inst[prev_key]
            elif op == "contains":
                _ = key in inst
            elif op == "len":
                _ = len(inst)
    
    # Create and start threads
    threads = []
    for i in range(num_threads):
        t = threading.Thread(target=thread_func, args=(i,))
        threads.append(t)
        t.start()
    
    # Wait for all threads to complete
    for t in threads:
        t.join()
    
    # Verify data integrity
    for thread_id in range(num_threads):
        for i in range(num_ops):
            key = f"key_{thread_id}_{i}"
            expected_value = f"value_{thread_id}_{i}"
            
            if key in inst:
                assert inst[key] == expected_value
