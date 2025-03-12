# PersistDict

A persistent dictionary implementation backed by an [LMDB database](https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database). PersistDict looks and acts like a Python dictionary but persists data to disk. It makes heavy use of [lmdb-dict](https://github.com/uchicago-dsi/lmdb-dict) behind the scenes.

## Why?

I ran into issues with langchain's caches when developing [wdoc](https://github.com/thiswillbeyourgithub/WDoc) (my RAG library) and after months of waiting I decided to fix it myself. Instead of trusting sqldict's implementation with langchain's concurrency, I made my own.

This makes it very easy to add persistent caching to anything. I initially made an implementation that used SQLite (with support for encryption, compression and handled concurrency via a singleton), but then I discovered [lmdb-dict](https://github.com/uchicago-dsi/lmdb-dict) which is likely much better as it's developed by professionals. It's based on [LMDB](https://en.wikipedia.org/wiki/LMDB) which is more suitable for what I was after than SQLite3. If you want to use the SQLite version, check out versions before `2.0.0`.

## Features:
- **Thread-safe**: All operations are protected by a reentrant lock. Multiple threads can safely access the same database without corruption.
- **Background processing**: Integrity checks and expiration run in a background thread by default, avoiding blocking the main thread during initialization.
- **Automatic expiration**: Old entries are automatically removed after a configurable number of days to prevent unbounded growth.
- **Metadata tracking**: Each entry includes creation time (ctime) and last access time (atime).
- **Caching**: Uses a `LRUCache128` from [cachetools](https://github.com/tkem/cachetools/) for better performance.
- **Customizable serialization**: Supports custom serializers for both keys and values, enabling encryption, compression, etc.
- **Key hashing**: Keys are hashed and cropped to handle the LMDB key size limitation (default 511 bytes).
- **Robust error handling**: Gracefully handles serialization errors and database corruption.
- **Minimal dependencies**: Only requires `lmdb-dict-full`. Optionally uses [beartype](https://github.com/beartype/beartype/) for type checking and [loguru](https://loguru.readthedocs.io/) for logging if available.


## Installation:
* From PyPI:
  ```bash
  pip install PersistDict
  ```
* From GitHub:
  ```bash
  git clone https://github.com/thiswillbeyourgithub/PersistDict
  cd PersistDict
  pip install -e .
  ```
* Run tests:
  ```bash
  cd PersistDict
  python -m pytest tests/test_persistdict.py -v
  ```

## Basic Usage:

```python
from PersistDict import PersistDict

# Create a persistent dictionary
d = PersistDict(
    database_path="/path/to/db",  # Path to the database directory
    expiration_days=30,           # Optional: entries older than this will be removed
    verbose=False,                # Optional: enable debug logging
    background_thread=True,       # Optional: run initialization tasks in background
)

# Use it like a regular dictionary
d["key"] = "value"
print(d["key"])  # "value"
print("key" in d)  # True
print(len(d))  # 1

# Dictionary-style initialization (only available once)
d = d(a=1, b="string", c=[1, 2, 3])

# Supports standard dictionary methods
for key in d.keys():
    print(key)
    
for value in d.values():
    print(value)
    
for key, value in d.items():
    print(f"{key}: {value}")

# Delete items
del d["a"]

# Clear the entire dictionary
d.clear()
```

## Advanced Usage:

```python
import json
import pickle
import dill

# Custom serializers for encryption, compression, etc.
d = PersistDict(
    database_path="/path/to/db",
    key_serializer=json.dumps,       # Custom key serializer
    key_unserializer=json.loads,     # Custom key deserializer
    value_serializer=dill.dumps,     # Custom value serializer
    value_unserializer=dill.loads,   # Custom value deserializer
    key_size_limit=511,              # Maximum key size before hashing
    caching=True,                    # Enable/disable LRU caching
    background_timeout=30,           # Maximum time for background operations
)

# Multiple instances can safely access the same database
d2 = PersistDict(database_path="/path/to/db")
assert list(d.keys()) == list(d2.keys())
```
