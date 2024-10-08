# PersistDict

Just a DIY version [sqldict](https://github.com/piskvorky/sqlitedict): looks like a dict and acts like a dict but is persistent via an sqlite3 db.

## Why?

I ran into issue with langchain's caches when developping [wdoc](https://github.com/thiswillbeyourgithub/WDoc) (my RAG lib, optimized for my use) and after months of waiting I decided to fix it myself. And instead of trusting sqldict's implementation with langchain's concurrency I made my own.
This makes it very easy to add persistent cache to anything.

## Features:
- **threadsafe**: if several threads try to access the same db it won't be a
  problem. Even if multiple other threads use also another db. Thanks to
  a singleton class. And if several python scripts run at the same time
  and try to access the same db, sqlite3 should make them wait appropriately.
- **atime and ctime**: each entry includes a creation time and a last access time.
- **expiration**: won't grow too large because old keys are automatically removed.
- **cached**: an actual python dict is used to cache the access to the db.
  This cache is shared among instances, and dropped if another scripts uses
  the same db.
- **compression**: using the builtin sqlite3 compression.
- **customizable serializer for the value**: by default pickle is used, but could
  be numpy.npz, joblib.dumps, dill.dumps etc
- **encryption**: unsing the UNMAINTAINED library pysqlcipher3, because it was
  very easy to add. In the future will use an up to date library and encrypt
  value in place directly.
- **no dependencies needed** If you have [beartype](https://github.com/beartype/beartype/) installed it will be used, same with [loguru](https://loguru.readthedocs.io/). Encryption comes from the UNMAINTAINED [pysqlcipher3](https://github.com/rigglemania/pysqlcipher3) lib. For now as I plan to move on to a simple in
place encryption instead.

## Differences with python dict:
- keys have to be str, that's what the sqlite db table is expecting.
- an object stored at self.__missing_value__ is used to designate a MISSING value,
  so you can't pickle this object. By default it's dataclasses.MISSING.
- .clear() will throw a NotImplementedError to avoid erasing the db. If you
  just want to clear the cache use self.clear_cache()
- add 3 methods to 'slice' the dict with multiple key/values:
    * .__getitems__
    * .__setitems__
    * .__delitems__
    - Note that calling __getitems__ with some keys missing will not return
      a KeyError but a self.__missing_value__ for those keys, which by default is
      dataclasses.MISSING.


## Usage:
* `git clone https://github.com/thiswillbeyourgithub/PersistDict`
* `cd PersistDict`
* `pip install -e .`
* To test that the code woks: `cd PersistDict ; python PersistDict.py`

``` python
# create the object
d = PersistDict(
    database_path=a_path,
    compression=True,
    password="J4mesB0nd",
    verbose=True,
)
# then treat it like a dict:
d["a"] = 1

# You can even create it via __call__, like a dict:
# d = d(a=1, b="b", c=str)  # this actually calls __call__ but is only
# allowed once per SqlDict, just like regular dict

# it's a child from dict
assert isinstance(d, dict)

# prints like a dict
print(d)
# {'a': 1, 'b': 'b', 'c': str}

# Supports the same methodas dict
assert sorted(list(d.keys())) == ["a", "b", "c"], d
assert "b" in d
del d["b"]
assert list(d.keys()) == ["a", "c"], d
assert len(d) == 2, d
assert d.__repr__() == {"a": 1, "c": str}.__repr__()
assert d.__str__() == {"a": 1, "c": str}.__str__()

# supports all the same types as value as pickle (or more if you change
# the serializer)
d["d"] = None

# new method to get and set multiple elements at the same time
assert d.__getitems__(["c", "d", "a"]) == [str, None, 1]

d.__setitems__(( ("a", 1), ("b", 2), ("c", 3), ('d', 4)))
assert d.__getitems__(["c", "d", "a", "b"]) == [3, 4, 1, 2], d.__getitems__(["c", "d", "a", "b"])

d.__delitems__(["c", "a"])
assert d.__getitems__(["b", "d"]) == [2, 4], d
assert len(d) == 2, d

# If you create another object pointing at the same db, they will share the
# same cache and won't corrupt the db:
d2 = SQLiteDict(
database_path=dbp,
compression=compr,
password=pw,
verbose=True,
)
list(d.keys()) == list(d2.keys()), d2
```
