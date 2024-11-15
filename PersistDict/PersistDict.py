from lmdb_dict import SafeLmdbDict
from lmdb_dict.cache import LRUCache128, DummyCache
import pickle
import datetime
from pathlib import Path, PosixPath
from typing import Union, Any, Optional, Tuple, Generator, Callable

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
    __VERSION__: str = "0.2.0"

    def __init__(
        self,
        database_path: Union[str, PosixPath],
        expiration_days: Optional[int] = 0,
        key_serializer: Optional[Callable] = json.dumps,
        key_unserializer: Optional[Callable] = json.loads,
        value_serializer: Optional[Callable] = pickle.dumps,
        value_unserializer: Optional[Callable] = pickle.loads,
        caching: bool = True,
        verbose: bool = False,
        ) -> None:
        """
        Initialize a PersistDict instance.

        The PersistDict class provides a persistent dictionary-like interface, storing data in a LMDB database.
        It supports optional automatic expiration of entries.
        Note that no checks are done to make sure the serializer is always the same. So if you change it and call the same db as before the serialization will fail.

        Args:
            database_path (Union[str, PosixPath]): Path to the LMDB database folder. Note that this is a folder, not a file.
            expiration_days (Optional[int], default=0): Number of days after which entries expire. 0 means no expiration.
            key_serializer (Callable, default=json.dumps): Function to serialize keys before storing. If None, no serializer will be used, but this can lead to issue.
            key_unserializer (Callable, default=json.loads): Function to deserialize keys after retrieval. If None, no unserializer will be used, but this can lead to issues.
            value_serializer (Callable, default=pickle.dumps): Function to serialize values before storing. If None, no serializer will be used, but this can lead to issue.
            value_unserializer (Callable, default=pickle.loads): Function to deserialize values after retrieval. If None, no unserializer will be used, but this can lead to issues.
            caching (bool, default=True): If False, don't use LMDB's built in caching. Beware that you can't change the caching method if an instance is already declared to use the db.
            verbose (bool, default=False): If True, enables verbose logging.
        """
        self.verbose = verbose
        self._log(".__init__")
        self.expiration_days = expiration_days
        self.database_path = Path(database_path)
        self.caching = caching

        if value_serializer is None or value_unserializer is None:
            assert value_serializer is None and value_unserializer is None, "If value_unserializer or value_serializer is None, the other one must be None too"
            def value_serializer(inp):
                return inp
            def value_unserializer(inp):
                return inp
        self.value_serializer = value_serializer
        self.value_unserializer = value_unserializer

        if key_serializer is None or key_unserializer is None:
            assert key_serializer is None and key_unserializer is None, "If key_unserializer or key_serializer is None, the other one must be None too"
            def key_serializer(inp):
                return inp
            def key_unserializer(inp):
                return inp
        self.key_serializer = key_serializer
        self.key_unserializer = key_unserializer

        extra_args = {}
        extra_args["cache"] = LRUCache128 if caching else DummyCache

        self.val_db = SafeLmdbDict(
            path=self.database_path,
            name="PersistDict_values",
            max_dbs=3,
            **extra_args,
        )
        self.metadata_db = SafeLmdbDict(
            path=self.database_path,
            name="PersistDict_metadata",
            max_dbs=3,
            **extra_args,
        )
        self.info_db = SafeLmdbDict(
            path=self.database_path,
            name="PersistDict_info",
            max_dbs=3,
            **extra_args,
        )

        # the db must be reset if the db file stops existing!
        if not self.database_path.exists():
            self.val_db.clear()
            self.metadata_db.clear()
            self.info_db.clear()

        if "version" not in self.info_db:
            self.info_db["version"] = self.__VERSION__
        if "ctime" not in self.info_db:
            self.info_db["ctime"] = datetime.datetime.now()
        if "already_called" not in self.info_db:
            self.info_db["already_called"] = False
        elif len(self.val_db) == 0:
            self.info_db["already_called"] = False

        # checks
        self.__integrity_check__()
        self.__expire__()
        self.__integrity_check__()

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

        Raises:
            AssertionError: If `expiration_days` is not a positive integer or 0.

        Note:
            This method is called internally and should not be called directly by users.
        """
        self._log("expirating")
        if not self.expiration_days:
            return
        assert self.expiration_days > 0, "expiration_days has to be a positive int or 0 to disable"
        expiration_date = datetime.datetime.now() - datetime.timedelta(days=self.expiration_days)

        keysbefore = list(self.val_db.keys())

        for k in keysbefore:
            if self.metadata_db[k]["atime"] <= expiration_date:
                del self.val_db[k], self.metadata_db[k]

        keysafter = list(self.val_db.keys())
        diff = len(keysbefore) - len(keysafter)
        assert diff >= 0, diff
        self._log(f"expirating removed {diff} keys, remaining: {keysafter}")

    def __integrity_check__(self) -> None:
        """
        Perform an integrity check on the database.

        This method checks the integrity of the LMDB database and verifies the consistency
        of creation times (ctime) and access times (atime) for all entries, as
        well as check that they all have the same keys.

        Note:
            This method is called internally and should not be called directly by users.
        """
        self._log("checking integrity of db")

        for k in self.val_db.keys():
            print(k)
            try:
                k2 = self.key_unserializer(k)
            except Exception as e:
                self._log(f"Couldn't unserialize key '{k}'. This might be due to changing the serialization in between runs.")
                del self.val_db[k], self.metadata_db[k]
                continue
            assert k in self.metadata_db, f"Item of key '{k2}' is missing from metadata_db"
            assert "atime" in self.metadata_db[k], f"Item of key '{k2}' is missing atime metadata"
            assert "ctime" in self.metadata_db[k], f"Item of key '{k2}' is missing ctime metadata"
            assert self.metadata_db[k]["ctime"] <= self.metadata_db[k]["atime"], f"Item of key '{k2}' has ctime after atime"

        l1 = len(self.val_db)
        l2 = len(self.metadata_db)
        assert l1 == l2, f"val_db and metadata_db sizes differ: {l1} vs {l2}"

        assert "version" in self.info_db, "info_db is missing the key 'version'"
        assert "ctime" in self.info_db, "info_db is missing the key 'ctime'"
        assert "already_called" in self.info_db, "info_db is missing the key 'already_called'"

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
        print(dict(self.info_db))
        assert not self.info_db["already_called"], (
            "The __call__ method of PersistDict can only be called once. "
            "Just like a regular dict.")

        if len(args) == 1 and isinstance(args[0], (dict, list)):
            items = args[0].items() if isinstance(args[0], dict) else args[0]
        else:
            items = list(args) + list(kwargs.items())

        for key, value in items:
            self[key] = value

        self.info_db["already_called"] = True

        return self

    def __getitem__(self, key: str) -> Any:
        self._log(f"getting item at key {key}")
        # self.__integrity_check__()
        ks = self.key_serializer(key)
        val = self.value_unserializer(self.val_db[ks])
        self.metadata_db[ks]["atime"] = datetime.datetime.now()
        return val

    def __setitem__(self, key: str, value: Any) -> None:
        self._log(f"setting item at key {key}")
        # self.__integrity_check__()
        ks = self.key_serializer(key)
        vs = self.value_serializer(value)
        self.val_db[ks] = vs
        t = datetime.datetime.now()
        self.metadata_db[ks] = {"ctime": t, "atime": t}

    def __delitem__(self, key: str) -> None:
        self._log(f"deleting item at key {key}")
        # self.__integrity_check__()
        ks = self.key_serializer(key)
        del self.val_db[ks], self.metadata_db[ks]

    def clear(self) -> None:
        self._log("Clearing database")
        keys = list(self.val_db.keys())
        for k in keys:
            del self.val_db[k], self.metadata_db[k]
        self.info_db["already_called"] = False

    def __len__(self) -> int:
        self._log("getting length")
        return len(self.val_db)

    def __contains__(self, key: str) -> bool:
        self._log(f"checking if val_db contains key {key}")
        ks = self.key_serializer(key)
        return ks in self.val_db.keys()

    def __repr__(self) -> str:
        return {k: v for k, v in self.items()}.__repr__()

    def __str__(self) -> str:
        return {k: v for k, v in self.items()}.__str__()

    def keys(self) -> Generator[str, None, None]:
        "get the list of keys present in the db"
        self._log("getting keys")
        for k in self.val_db.keys():
            yield self.key_unserializer(k)

    def values(self) -> Generator[Any, None, None]:
        "get the list of values present in the db"
        self._log("getting values")
        for k in self.val_db.keys():
            self.metadata_db[k]["atime"] = datetime.datetime.now()
            yield self.value_unserializer(self.val_db[k])

    def items(self) -> Generator[Tuple[str, Any], None, None]:
        self._log("getting items")
        for k in self.val_db.keys():
            yield self.key_unserializer(k), self.value_unserializer(self.val_db[k])
