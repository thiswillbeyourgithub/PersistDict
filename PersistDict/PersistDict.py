import sqlite3
import pickle
import datetime
from pathlib import Path, PosixPath
from typing import Union, Any, Optional, Tuple, Generator, Callable, Sequence
from threading import Lock
from dataclasses import MISSING
import hashlib

# only use those libs if present:
try:
    from beartype import beartype as typechecker
except ImportError:
    def typechecker(func: Callable) -> Callable:
        return func

try:
    from loguru import logger
    debug = logger.debug
except ImportError:
    def debug(message: str) -> None:
        print(message)


@typechecker
class PersistDict(dict):
    __VERSION__: str = "0.1.3"
    __already_called__: bool = False
    __missing_value__: Any = MISSING

    def __init__(
        self,
        database_path: Union[str, PosixPath],
        expiration_days: Optional[int] = 0,
        check_same_thread: bool = False,
        connection_timeout: int = 30,
        compression: bool = True,
        value_serializer: Callable = pickle.dumps,
        value_unserializer: Callable = pickle.loads,
        password: Optional[str] = None,
        password_inc: int = 100_000,
        verbose: bool = False,
        ) -> None:
        """
        Initialize a PersistDict instance.

        The PersistDict class provides a persistent dictionary-like interface, storing data in an SQLite database.
        It supports optional encryption, compression, and automatic expiration of entries.

        Args:
            database_path (Union[str, PosixPath]): Path to the SQLite database file.
            expiration_days (Optional[int], default=0): Number of days after which entries expire. 0 means no expiration.
            check_same_thread (bool, default=False): If False, allows SQLite connections from multiple threads.
            connection_timeout (int, default=30): Timeout in seconds for database connections.
            compression (bool, default=True): Whether to enable database compression.
            value_serializer (Callable, default=pickle.dumps): Function to serialize values before storing.
            value_unserializer (Callable, default=pickle.loads): Function to deserialize values after retrieval.
            password (Optional[str], default=None): Password for database encryption. If None, the database is not encrypted.
            password_inc (int, default=100_000): Number of iterations for password hashing.
            verbose (bool, default=False): If True, enables verbose logging.
        """
        if password:
            assert len(password.strip()) > 7, "password has to be at least 7 characters long excluding whitespaces"
            salt = "Z05gFsdff9m3pQhOfSB2sE0Y0waMpYw0RTaxNKH3He965ct/7xHBCQmBr+HgKu7bC8uhNkN4kk9NuHh7FU7sHQ"
            for i in range(password_inc):
                password = hashlib.sha3_512((salt + password).encode("utf-8")).hexdigest()
            self.__pw__ = password
        else:
            self.__pw__ = None
        del password

        self.verbose = verbose
        self._log(".__init__")
        self.connection_timeout = connection_timeout
        self.check_same_thread = check_same_thread
        self.expiration_days = expiration_days
        self.compression = compression
        self.value_serializer = value_serializer
        self.value_unserializer = value_unserializer
        self.database_path = Path(database_path)

        if self.database_path.is_dir():
            self.database_path = self.database_path / "sqlite_dict.db"

        self.shared = SingletonHolder()
        self.lockkey = str(self.database_path.absolute().resolve())
        if self.lockkey not in self.shared.db_locks:
            with self.shared.meta_db_lock:
                self.shared.db_locks[self.lockkey] = Lock()
                self.shared.db_caches[self.lockkey] = {}
                self.shared.cache_timestamps[self.lockkey] = 0

        self.lock = self.shared.db_locks[self.lockkey]
        with self.lock:
            with self.shared.meta_db_lock:
                self.__cache__ = self.shared.db_caches[self.lockkey]


        # create db if not exist
        self.__init_table__()

        # checks
        self.__integrity_check__()
        self.__expire__()
        self.__integrity_check__()

        self.__tick_cache__()

    def __connect__(self) -> Union[sqlite3.Connection, Any]:
        "open connection to the db"
        if self.__pw__ is None:
            self._log("opening connection")
            return sqlite3.connect(
                self.database_path,
                check_same_thread=self.check_same_thread,
                timeout=self.connection_timeout,
            )
        else:
            self._log("opening encrypted connection")
            try:
                from pysqlcipher3 import dbapi2 as sqlite3_encrypted
            except ImportError as e:
                raise Exception(f"Error when importing pysqlcipher3: '{e}'") from e
            conn = sqlite3_encrypted.connect(
                str(self.database_path),
                check_same_thread=self.check_same_thread,
                timeout=self.connection_timeout,
            )
            conn.execute(f"PRAGMA key='{self.__pw__}'")
            return conn

    def __init_table__(self) -> None:
        """
        Initialize the database tables.

        This method creates the necessary tables in the SQLite database if they don't exist.
        It also sets up compression if enabled and performs a vacuum operation.

        The method creates two tables:
        1. 'storage': Stores the key-value pairs along with creation and access timestamps.
        2. 'metadata': Stores metadata information, including the version of PersistDict.

        Raises:
            sqlite3.DatabaseError: If there's an issue with the database file or encryption.
        """
        self._log(".__init_table__")
        conn = self.__connect__()
        cursor = conn.cursor()
        try:
            with self.lock:
                cursor.execute("BEGIN")
                cursor.execute('''CREATE TABLE IF NOT EXISTS storage (
                                key TEXT PRIMARY KEY,
                                value BLOB,
                                ctime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                atime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                                )''')
                cursor.execute('''CREATE TABLE IF NOT EXISTS metadata (
                                key TEXT PRIMARY KEY,
                                value TEXT
                                )''')
                cursor.execute("INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)", ("version", str(self.__VERSION__)))
                conn.commit()

                if self.compression:
                    # Enable compression
                    cursor.execute("PRAGMA page_size = 4096")
                    cursor.execute("PRAGMA auto_vacuum = FULL")
                    conn.commit()

                cursor.execute("VACUUM")
                conn.commit()
        except sqlite3.DatabaseError as e:
            if "file is not a database" and not self.__pw__:
                raise sqlite3.DatabaseError("File is not a database. Maybe you are trying to open an encrypted db without supplying the password?") from e
            if "file is encrypted or is not a database":
                if self.__pw__:
                    raise sqlite3.DatabaseError("File is not a database or is not encrypted or you're using the wrong password.") from e
                else:
                    raise sqlite3.DatabaseError("File is not a database or is encrypted.") from e
            else:
                raise
        finally:
            conn.close()

    def __expire__(self) -> None:
        """
        Remove elements from the database that have not been accessed within the expiration period.

        This method checks the access time (atime) of each entry in the database and removes
        those that are older than the specified expiration period. The expiration period is
        determined by the `expiration_days` attribute set during initialization.

        If `expiration_days` is 0 or None, this method does nothing and returns immediately.

        The method performs the following steps:
        1. Calculates the expiration date based on the current date and `expiration_days`.
        2. Removes entries from the database with an access time older than the expiration date.
        3. Performs a VACUUM operation to optimize the database after deletion.
        4. Updates the in-memory cache to reflect the changes made to the database.

        Raises:
            AssertionError: If `expiration_days` is not a positive integer or 0.

        Note:
            This method is called internally and should not be called directly by users.
        """
        if not self.expiration_days:
            return
        self._log("expirating cache")
        assert self.expiration_days > 0, "expiration_days has to be a positive int or 0 to disable"
        expiration_date = datetime.datetime.now() - datetime.timedelta(days=self.expiration_days)

        keysbefore = list(self.keys())

        conn = self.__connect__()
        cursor = conn.cursor()
        try:
            with self.lock:
                cursor.execute("BEGIN")
                cursor.execute("DELETE FROM storage WHERE atime < ?", (expiration_date,))
                conn.commit()

                cursor.execute("VACUUM")
                conn.commit()
        finally:
            conn.close()

        keysafter = list(self.keys())
        diff = len(keysbefore) - len(keysafter)
        assert diff >= 0, diff
        self._log(f"expirating cache removed {diff} keys, remaining: {keysafter}")

        self.__check_cache__()

        with self.lock:
            for kb in keysbefore:
                if kb not in keysafter and kb in self.__cache__:
                    del self.__cache__[kb]
                    self.__tick_cache__()

    def __integrity_check__(self) -> None:
        """
        Perform an integrity check on the database.

        This method checks the integrity of the SQLite database and verifies the consistency
        of creation times (ctime) and access times (atime) for all entries.

        The method performs the following steps:
        1. Runs SQLite's built-in PRAGMA integrity_check.
        2. Verifies that all creation times are less than or equal to their corresponding access times.
        3. Calls the __version_check__ method to ensure version compatibility.

        Raises:
            Exception: If the PRAGMA integrity_check fails or if any inconsistencies are found
                       in the creation and access times.
            sqlite3.Error: If there's an SQLite-specific error during the check.

        Note:
            This method is called internally and should not be called directly by users.
        """
        self._log("checking integrity of db")
        self.__check_cache__()
        conn = self.__connect__()
        cursor = conn.cursor()
        try:
            with self.lock:
                cursor.execute("BEGIN")
                cursor.execute("PRAGMA integrity_check")
                result = cursor.fetchall()

                if not (len(result) == 1 and result[0][0] == 'ok'):
                    raise Exception("PRAGMA integrity_check failed:\n" + "\n".join([row[0] for row in result]))

                cursor.execute('SELECT ctime, atime FROM storage ORDER BY ctime')
                results = cursor.fetchall()
                if result:
                    cnt = 0
                    for ctime, atime in results:
                        cnt += 1
                        assert ctime <= atime, f"Found a creation time < to an access time in the {cnt} position"

        except sqlite3.Error as e:
            raise Exception(f"integrity_check failed: SQLite error: '{e}'")
        finally:
            conn.close()

        self.__version_check__()

    def __version_check__(self) -> None:
        """
        Check the version of the database against the current PersistDict version.

        This method retrieves the version information stored in the database's metadata
        and compares it with the current version of the PersistDict class. If there's
        a version mismatch, it raises a NotImplementedError, as migrations between
        different versions are not yet supported.

        Raises:
            AssertionError: If the 'version' key is missing from the metadata.
            NotImplementedError: If the database version doesn't match the current PersistDict version.
        """
        self._log("checking version")
        self.__check_cache__()
        conn = self.__connect__()
        cursor = conn.cursor()
        try:
            with self.lock:
                cursor.execute('SELECT key, value FROM metadata')
                metadatas = cursor.fetchall()
                metadatas: dict = {m[0]: m[1] for m in metadatas}
        finally:
            conn.close()
        assert "version" in metadatas, f"Missing version key in metadata:\n{metadatas}"
        if metadatas["version"] != self.__VERSION__:
            raise NotImplementedError(
                f"You are using PersistDict version {self.__VERSION__} "
                "but the db you're trying to load is in version "
                f"{metadatas['version']}. Crashing as migrations are not yet supported.")

    def _log(self, message: str) -> None:
        if self.verbose:
            debug("PersistDict:" + message)

    def __call__(self, *args, **kwargs):
        """ only available at instantiation time, to make it more dict-like.
        For example:
            d = dict(a=1)  # works
            d = d(a=2)  # fails, the instance of dict has no __call__ method
        Hence, we forbid calling __call__ after the first use of __getitem__
        """
        self._log(".__call__")
        assert not self.__already_called__, "The __call__ method of PersistDict can only be called once. Just like a regular dict."

        if len(args) == 1 and isinstance(args[0], (dict, list)):
            items = args[0].items() if isinstance(args[0], dict) else args[0]
        else:
            items = list(args) + list(kwargs.items())

        for key, value in items:
            self[key] = value

        return self

    def __getitem__(self, key: str) -> Any:
        return self.__getitems__([key])[0]

    def __getitems__(self, keys: Sequence[str]) -> Sequence[Any]:
        """
        Retrieve multiple items from the database or cache.

        This method performs a batch lookup of multiple keys, either from the cache
        or from the database if not found in the cache. It updates the access time
        for each key in the database and refreshes the cache with any newly retrieved values.

        Args:
            keys (Sequence[str]): A sequence of keys to retrieve.

        Returns:
            Sequence[Any]: A sequence of values corresponding to the input keys.
                           If a key is not found, the corresponding value will be self.__missing_value__.

        Note:
            This method is used internally by __getitem__ for efficient batch retrieval.
        """
        self._log(f"getting items for keys {keys}")
        # check the cache is still as expected
        self.__check_cache__()

        with self.lock:
            conn = self.__connect__()

            # already cached
            states = []
            known_vals = []
            todo_keys = []
            for key in keys:
                if key in self.__cache__:
                    known_vals.append(self.__cache__[key])
                    states.append(0)
                else:
                    todo_keys.append(key)
                    states.append(1)
            if not todo_keys:  # all in cache
                return known_vals

            # load the value from the db
            cursor = conn.cursor()
            try:
                cursor.execute("BEGIN")
                cursor.execute("UPDATE storage SET atime = CURRENT_TIMESTAMP WHERE key IN (" + ",".join(['?'] * len(todo_keys)) + ")", todo_keys)
                conn.commit()
                cursor.execute("SELECT key, value FROM storage WHERE key IN (" + ",".join(['?'] * len(todo_keys)) + ")", todo_keys)
                results = cursor.fetchall()
            finally:
                conn.close()

            # preserve the ordering
            results = {r[0]: self.value_unserializer(r[1]) for r in results}
            output = []
            for s in states:
                if not s:
                    output.append(known_vals.pop(0))
                else:
                    t = todo_keys.pop(0)
                    if t in results:
                        output.append(results[t])
                        self.__cache__[t] = results[t]
                    else:
                        output.append(self.__missing_value__)
            self.__tick_cache__()
        return output

    def __setitem__(self, key: str, value: Any) -> None:
        return self.__setitems__(((key, value),))

    def __setitems__(self, key_value_pairs: Sequence[Sequence]) -> None:
        "actual code to set the data in the db then the cache"
        if not self.__already_called__:
            self.__already_called__ = True
        assert all(len(pair) == 2 for pair in key_value_pairs)
        keys = [kv[0] for kv in key_value_pairs]
        vals = [kv[1] for kv in key_value_pairs]
        self._log(f"setting item at keys {keys}")
        if any(v is self.__missing_value__ for v in vals):
            raise Exception(f"PersistDict can't store self.__missing_value__ '{self.__missing_value__}' objects as it's used to denote missing objects")

        kvp = {k: self.value_serializer(v) for k, v in key_value_pairs}
        conn = self.__connect__()
        cursor = conn.cursor()
        try:
            with self.lock:
                cursor.execute("BEGIN")
                # Check if the record exists
                cursor.execute("SELECT key FROM storage WHERE key IN (" + ",".join(['?'] * len(keys)) + ")", keys)

                nonmissingk = [r[0] for r in cursor.fetchall()]
                to_add = [ (k, v) for k, v in kvp.items() if k not in nonmissingk]
                to_update = [ (v, k) for k, v in kvp.items() if k in nonmissingk]

                if to_add:
                    cursor.executemany("INSERT INTO storage (key, value) VALUES (?, ?)", to_add)
                if to_update:
                    cursor.executemany("UPDATE storage SET value = ?, atime = CURRENT_TIMESTAMP WHERE key = ?", to_update)

                conn.commit()
                for k, v in key_value_pairs:
                    self.__cache__[k] = v
                self.__tick_cache__()
        finally:
            conn.close()

    def __delitem__(self, key: str) -> None:
        return self.__delitems__([key])

    def __delitems__(self, keys: Sequence[str]) -> None:
        "delete item from cache and db"
        self._log(f"deleting items at key {keys}")
        self.__check_cache__()
        conn = self.__connect__()
        cursor = conn.cursor()
        try:
            with self.lock:
                cursor.execute("BEGIN")
                cursor.executemany("DELETE FROM storage WHERE key = ?", keys)
                conn.commit()
                for key in keys:
                    if key in self.__cache__:
                        del self.__cache__[key]
                self.__tick_cache__()

        finally:
            conn.close()

    def clear_cache(self) -> None:
        "clears the cache"
        self._log("clearing cache")
        with self.lock:
            self.__cache__.clear()
        self.__tick_cache__()

    def __check_cache__(self) -> None:
        """check if the db has been modified recently and not by us, then we
        need to drop the cache. It can happen if multiple python scripts are
        running at the same time."""
        if self.shared.cache_timestamps[self.lockkey] < self.database_path.stat().st_mtime:
            self._log("Cache was not up to date so clearing it.")
            self.clear_cache()

        # also check that the is is still as expected
        with self.shared.meta_db_lock:
            assert id(self.__cache__) == id(self.shared.db_caches[self.lockkey])

    def __tick_cache__(self) -> None:
        """
        Update the cache timestamp for the current database.

        This method updates the cache timestamp in the shared cache_timestamps
        dictionary, using the current modification time of the database file.
        This helps in tracking when the database was last modified.
        """
        with self.shared.meta_db_lock:
            self.shared.cache_timestamps[self.lockkey] = self.database_path.stat().st_mtime

    def clear(self) -> None:
        raise NotImplementedError("PersistDict cannot be cleared like a dict. Use self.cache_clear() to reset the cache, or reset the database with self.__delitems__(list(self.keys()))")

    def __len__(self) -> int:
        if self.verbose:
            self._log("getting length")
        return len(list(self.keys()))

    def __contains__(self, key: str) -> bool:
        self._log(f"checking if contains key {key}")
        if key in self.__cache__:
            return True
        for k in self.keys():
            if k == key:
                return True
        return False

    def __repr__(self) -> str:
        return {k: v for k, v in self.items()}.__repr__()

    def __str__(self) -> str:
        return {k: v for k, v in self.items()}.__str__()

    def keys(self) -> Generator[str, None, None]:
        "get the list of keys present in the db"
        self._log("getting list of keys")
        conn = self.__connect__()
        cursor = conn.cursor()
        try:
            with self.lock:
                cursor.execute('SELECT key FROM storage ORDER BY ctime')
                results = [row[0] if row else self.__missing_value__ for row in cursor.fetchall()]
        finally:
            conn.close()
        for r in results:
            assert r is not self.__missing_value__
            yield r

    def values(self) -> Generator[Any, None, None]:
        "get the list of values present in the db"
        self._log("getting list of values")
        conn = self.__connect__()
        cursor = conn.cursor()
        try:
            with self.lock:
                cursor.execute('SELECT key, value FROM storage ORDER BY ctime')
                results = cursor.fetchall()
                for k, v in results:
                    self.__cache__[k] = v
                self.__tick_cache__()
        finally:
            conn.close()
        for r in results:
            assert r is not self.__missing_value__
            return self.value_unserializer(r)

    def items(self) -> Generator[Tuple[str, Any], None, None]:
        self._log("getting list of items")
        conn = self.__connect__()
        cursor = conn.cursor()
        try:
            with self.lock:
                cursor.execute('SELECT key, value FROM storage ORDER BY ctime')
                results = [row if row else self.__missing_value__ for row in cursor.fetchall()]
        finally:
            conn.close()
        for r in results:
            assert r is not self.__missing_value__
            assert len(r) == 2
            yield r[0], self.value_unserializer(r[1])


class SingletonHolder:
    """singleton that holds dict used to keep track of caches and locks

    We use the same lock for each instance accessing the same db, as well as a
    meta db lock to add new locks. This way if you have multiple threads
    all accessing the same db they will lock the others when using the db. But
    if you also have threads accessing other dbs they will not be stopped.

    """
    _instance = None
    initialized = False
    meta_db_lock = None
    db_locks = None
    cache_timestamps = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if self.initialized:
            return
        self.meta_db_lock: Lock = Lock()
        self.db_locks: dict = {}

        # same idea for cache, if another instance points to the same path it could
        # modify a value and make the cache of another db wrong
        self.db_caches: dict = {}

        self.cache_timestamps: dict = {}

        self.initialized: bool = True

if __name__ ==  "__main__":
    dbp = Path("test_db.sqlite")

    # basic tests
    if dbp.exists():
        ans = input(f"'Yes' to delete '{dbp}'\n>")
        if ans != "yes":
            raise SystemExit()
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
                assert d["c"] == str, d
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

    import code
    code.interact(local=locals())

