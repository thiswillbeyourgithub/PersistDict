# PersistDict

Just a DIY version [sqldict](https://github.com/piskvorky/sqlitedict): looks like a dict and acts like a dict but is persistent via an [LMDB database](https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database). Makes heavy use of [lmdb-dict](https://github.com/uchicago-dsi/lmdb-dict) behind the scenes.

## Why?

I ran into issue with langchain's caches when developping [wdoc](https://github.com/thiswillbeyourgithub/WDoc) (my RAG lib, optimized for my use) and after months of waiting I decided to fix it myself. And instead of trusting sqldict's implementation with langchain's concurrency I made my own.
This makes it very easy to add persistent cache to anything.
Also it was easy to do thanks to my [BrownieCutter](https://pypi.org/project/BrownieCutter/).
I initially made an implementation that used sqlite (with support for encryption, compression and handled concurrency via a singleton) but then I stumbled upon [lmdb-dict](https://github.com/uchicago-dsi/lmdb-dict) which is very probably way better as it's done by pros. It's based on [LMDB](https://en.wikipedia.org/wiki/LMDB) which is a more suitable for what I was after when doing PersistDict than sqlite3. If you want to use the sqlite version take a look at version before `2.0.0`.

## Features:
- **threadsafe**: if several threads try to access the same db it won't be a
  problem. Even if multiple other threads use also another db. And if several
  python scripts run at the same time and try to access the same db, LMDB
  should make them wait appropriately.
- **atime and ctime**: each entry includes a creation time and a last access time.
- **expiration**: won't grow too large because old keys are automatically removed after a given amount of days.
- **cached**: Uses a `LRUCache128` from [cachetools](https://github.com/tkem/cachetools/).
- **customizable serializer for keys and values**: This can enable encryption, compression etc... By default, keys are compressed as [lmdb has a 511 default key length](https://stackoverflow.com/questions/66456228/increase-max-key-size-lmdb-key-value-database-in-python).
- **only one dependency needed** Only `lmdb-dict-full` is needed. If you have [beartype](https://github.com/beartype/beartype/) installed it will be used, same with [loguru](https://loguru.readthedocs.io/).


## Usage:
* Download from pypi with `pip install PersistDict`
* Or from git:
    * `git clone https://github.com/thiswillbeyourgithub/PersistDict`
    * `cd PersistDict`
    * `pip install -e .`
    * To run tests: `cd PersistDict ; python -m pytest test_persistdict.py -v`

``` python
from PersistDict import PersistDict

# create the object
d = PersistDict(
    database_path=a_path,
    # verbose=True,
    # expiration_days=30,
)
# then treat it like a dict:
d["a"] = 1

# You can even create it via __call__, like a dict:
# d = d(a=1, b="b", c=str)  # this actually calls __call__ but is only
# allowed once per PersistDict, just like a regular dict

# it's a child from dict
assert isinstance(d, dict)

# prints like a dict
print(d)
# {'a': 1, 'b': 'b', 'c': str}

# Supports the same methods
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

# If you create another object pointing at the same db, they will share the
# same cache and won't corrupt the db:
d2 = PersistDict(
database_path=dbp,
verbose=True,
)
list(d.keys()) == list(d2.keys()), d2
```
