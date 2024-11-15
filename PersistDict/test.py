import json
import datetime
import pickle
import dill
from pathlib import Path

from PersistDict import PersistDict

def delete_dir(dbp):
    if not dbp.exists():
        return
    assert dbp.is_dir()
    for f in dbp.iterdir():
        f.unlink(missing_ok=True)
    dbp.rmdir()

def do_one_test_scenario(extra, **kwargs):
    print(f"Now testing with extra '{extra}' and kwargs '{kwargs}'")

    dbp = kwargs["database_path"]
    reset = extra["reset"]

    if reset == "delete":
        delete_dir(dbp)
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
    list(d.keys()) == list(d2.keys()), d2

    d2["0"] = None
    assert d["0"] is None
    del d["0"]
    assert "0" not in d2, d2


def do_test_expiration_and_serialization():
    dbp = Path("test_db")
    delete_dir(dbp)
    inst = PersistDict(
        database_path=dbp,
        verbose=True,
        expiration_days=7,
        value_serializer=None,
        value_unserializer=None,
    )
    assert len(inst) == 0, inst
    inst["test"] = "value"
    assert len(inst) == 1, inst
    assert inst["test"] == "value"
    assert inst.key_serializer(inst.hash_and_crop("test")) in inst.metadata_db, inst.key_serializer("test")
    inst.metadata_db[inst.key_serializer(inst.hash_and_crop("test"))]["atime"] = datetime.datetime.now() - datetime.timedelta(days=1)
    assert len(inst) == 1, inst
    inst.__expire__()
    assert len(inst) == 1, inst
    inst.metadata_db[inst.key_serializer(inst.hash_and_crop("test"))]["atime"] = datetime.datetime.now() - datetime.timedelta(days=14)
    assert len(inst) == 1, inst
    inst.__expire__()
    assert len(inst) == 0, inst


def test_all():
    dbp = Path("test_db")

    delete_dir(dbp)

    inst = PersistDict(database_path=dbp, verbose=True)
    first = inst()
    assert len(first) == 0, first

    try:
        inst()()
    except Exception as e:
        assert str(e) == "The __call__ method of PersistDict can only be called once. Just like a regular dict."

    for reset in ["clear", "delete", "clear"]:
        for key_serializer, key_unserializer in [(None, None), (json.dumps, json.loads)]:
            for value_serializer, value_unserializer in [(pickle.dumps, pickle.loads), (dill.dumps, dill.loads)]:
                do_one_test_scenario(
                    database_path=dbp,
                    verbose=True,
                    key_serializer=key_serializer,
                    key_unserializer=key_unserializer,
                    value_serializer=value_serializer,
                    value_unserializer=value_unserializer,
                    extra = {"reset": reset},
                )

    do_test_expiration_and_serialization()

if __name__ == "__main__":
    test_all()
    import code
    code.interact(local=locals())

