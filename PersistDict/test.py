from pathlib import Path

from .PersistDict import PersistDict

def test_all():
    dbp = Path("test_db.sqlite")
    dbp.unlink(missing_ok=True)

    inst = PersistDict(database_path=dbp)
    first = inst()
    assert len(first) == 0, first

    try:
        inst()()
    except Exception as e:
        assert str(e) == "The __call__ method of PersistDict can only be called once. Just like a regular dict."

    for doclear in [False, True]:
        for pw in [None, "testtesttest"]:
            for compr in [True, False]:
                dbp.unlink(missing_ok=True)
                inst = PersistDict(
                    database_path=dbp,
                    compression=compr,
                    password=pw,
                    verbose=True,
                )
                d = inst(a=1, b="b", c=str)
                assert len(d) == 3, d
                assert d["a"] == 1, d
                assert d["b"] == "b", d
                assert d["c"] is str, d
                if doclear:
                    d.clear_cache()
                assert sorted(list(d.keys())) == ["a", "b", "c"], d
                print(d)
                del d["b"]
                assert list(d.keys()) == ["a", "c"], d
                assert len(d) == 2, d
                assert d.__repr__() == {"a": 1, "c": str}.__repr__()
                assert d.__str__() == {"a": 1, "c": str}.__str__()

                assert isinstance(d, dict)

                d["d"] = None

                assert d.__getitems__(["c", "d", "a"]) == [str, None, 1]

                d.__setitems__(( ("a", 1), ("b", 2), ("c", 3), ('d', 4)))
                assert d.__getitems__(["c", "d", "a", "b"]) == [3, 4, 1, 2], d.__getitems__(["c", "d", "a", "b"])

                d.__delitems__(["c", "a"])
                assert d.__getitems__(["b", "d"]) == [2, 4], d
                assert len(d) == 2, d

                d2 = PersistDict(
                    database_path=dbp,
                    compression=compr,
                    password=pw,
                    verbose=True,
                )
                list(d.keys()) == list(d2.keys()), d2

                d2["0"] = None
                assert d["0"] is None
                del d["0"]
                assert "0" not in d2, d2
                if doclear:
                    d2.clear_cache()
                assert "0" not in d2, d2
    dbp.unlink(missing_ok=True)

if __name__ == "__main__":
    test_all()
    import code
    code.interact(local=locals())

