# PersistDict

A persistent dictionary implementation backed by an [LMDB database](https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database). PersistDict looks and acts like a Python dictionary but persists data to disk, making it ideal for caching and persistent storage needs.

## Overview

PersistDict provides a dictionary-like interface that stores data on disk using the high-performance LMDB (Lightning Memory-Mapped Database). It builds upon [lmdb-dict](https://github.com/uchicago-dsi/lmdb-dict) to provide a robust, thread-safe persistent dictionary with additional features like automatic expiration, metadata tracking, and customizable serialization.

## Why PersistDict?

I created PersistDict while developing [wdoc](https://github.com/thiswillbeyourgithub/WDoc), my RAG library, after encountering issues with langchain's caching mechanisms. Instead of relying on existing implementations that didn't handle concurrency well, I built PersistDict to be thread-safe and robust from the ground up.

PersistDict makes it simple to add persistent caching to any Python application. While earlier versions (before 2.0.0) used SQLite, the current version leverages LMDB for better performance and reliability in concurrent environments.

## Key Features

- **Thread-safe**: All operations are protected by a reentrant lock, allowing multiple threads to safely access the same database without corruption.
- **Background Processing**: Integrity checks and expiration run in a background thread by default, avoiding blocking the main thread during initialization.
- **Automatic Expiration**: Old entries are automatically removed after a configurable number of days to prevent unbounded growth.
- **Metadata Tracking**: Each entry includes creation time (ctime) and last access time (atime) for advanced data management.
- **Performance Optimized**: Uses `LRUCache128` from [cachetools](https://github.com/tkem/cachetools/) for better performance with frequently accessed items.
- **Customizable Serialization**: Supports custom serializers for both keys and values, enabling encryption, compression, or any custom data transformation.
- **Key Hashing**: Keys are hashed and cropped to handle the LMDB key size limitation (default 511 bytes).
- **Robust Error Handling**: Gracefully handles serialization errors and database corruption with detailed logging.
- **Collision Management**: Properly handles key hash collisions to ensure data integrity.
- **Minimal Dependencies**: Only requires `lmdb-dict-full`. Optionally uses [beartype](https://github.com/beartype/beartype/) for type checking and [loguru](https://loguru.readthedocs.io/) for logging if available.

## Installation

### From PyPI
```bash
pip install PersistDict
```

### From GitHub
```bash
git clone https://github.com/thiswillbeyourgithub/PersistDict
cd PersistDict
pip install -e .
```

### Running Tests
```bash
cd PersistDict
python -m pytest tests/test_persistdict.py -v
```

## Basic Usage

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
print(d["key"])         # "value"
print("key" in d)       # True
print(len(d))           # 1

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

## Advanced Usage

### Custom Serialization

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
```

### Shared Database Access

Multiple instances can safely access the same database:

```python
# Create two instances pointing to the same database
d1 = PersistDict(database_path="/path/to/db")
d2 = PersistDict(database_path="/path/to/db")

# Changes in one instance are visible in the other
d1["shared_key"] = "shared_value"
assert d2["shared_key"] == "shared_value"
assert list(d1.keys()) == list(d2.keys())
```

### Background Thread Control

Control how initialization tasks run:

```python
# Run in background (default)
d1 = PersistDict(database_path="/path/to/db", background_thread=True)

# Run in foreground (blocking)
d2 = PersistDict(database_path="/path/to/db", background_thread=False)

# Skip initialization tasks entirely
d3 = PersistDict(database_path="/path/to/db", background_thread="disabled")
```

### Named Instances

Create named instances for better logging:

```python
d = PersistDict(
    database_path="/path/to/db",
    name="cache_db",    # Name for identifying this instance in logs
    verbose=True        # Enable logging
)
```
