import zlib
import base64
import hashlib
import os
import threading
import concurrent.futures
import time
import queue
from lmdb_dict import SafeLmdbDict
from lmdb_dict.cache import LRUCache128, DummyCache
from lmdb_dict.mapping.abc import LmdbDict
import pickle
import datetime
from pathlib import Path, PosixPath
try:
    from beartype.typing import Union, Any, Optional, Tuple, Generator, Callable
except Exception:
    from typing import Union, Any, Optional, Tuple, Generator, Callable

# only use those libs if present:
try:
    from beartype.beartype import beartype as typechecker
except ImportError:
    def typechecker(func: Callable) -> Callable:
        return func

try:
    from loguru import logger
    debug = logger.debug
except ImportError:
    def debug(message: str) -> None:
        print(message)

def key_to_string(key):
    return base64.b64encode(zlib.compress(pickle.dumps(key), level=1)).decode('utf-8')

def string_to_key(pickled_str):
    return pickle.loads(zlib.decompress(base64.b64decode(pickled_str.encode('utf-8'))))

def dummy_key_serializer(inp):
    if isinstance(inp, str):
        return inp.encode()
    return inp

def dummy_key_unserializer(inp):
    if hasattr(inp, "decode"):
        return inp.decode()
    return inp

def dummy_value_serializer(inp):
    if isinstance(inp, str):
        return inp.encode()
    return inp

def dummy_value_unserializer(inp):
    if hasattr(inp, "decode"):
        return inp.decode()
    return inp

@typechecker
class PersistDict(dict):
    __VERSION__: str = "0.2.5"

    def __init__(
        self,
        database_path: Union[str, PosixPath],
        expiration_days: Optional[int] = 0,
        key_serializer: Optional[Callable] = key_to_string,
        key_unserializer: Optional[Callable] = string_to_key,
        key_size_limit: int = 511,
        value_serializer: Optional[Callable] = pickle.dumps,
        value_unserializer: Optional[Callable] = pickle.loads,
        caching: bool = True,
        verbose: bool = False,
        async_expire: bool = True,
        expire_check_interval: int = 3600,  # Check expiration every hour by default
        ) -> None:
        """
        Initialize a PersistDict instance.

        The PersistDict class provides a persistent dictionary-like interface, storing data in a LMDB database.
        It supports optional automatic expiration of entries.
        Note that no checks are done to make sure the serializer is always the same. So if you change it and call the same db as before the serialization will fail.

        Args:
            database_path (Union[str, PosixPath]): Path to the LMDB database folder. Note that this is a folder, not a file.
            expiration_days (Optional[int], default=0): Number of days after which entries expire. 0 means no expiration.
            key_serializer (Callable, default=key_to_string): Function to serialize keys before storing. If None, no serializer will be used, but this can lead to issue.
            key_unserializer (Callable, default=string_to_key): Function to deserialize keys after retrieval. If None, no unserializer will be used, but this can lead to issues.
            key_size_limit (int, default=511): Maximum size for the key. If the key is larger than this it will be hashed then cropped.
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
        self.key_size_limit = key_size_limit
        self.async_expire = async_expire
        self.expire_check_interval = expire_check_interval
        self._expire_lock = threading.Lock()
        self._expire_thread = None
        self._stop_expire_thread = threading.Event()
        
        # For background initialization
        self._init_lock = threading.Lock()
        self._init_thread = None
        self._init_complete = threading.Event()
        self._init_queue = queue.Queue()

        if key_serializer is None or key_unserializer is None:
            assert key_serializer is None and key_unserializer is None, "If key_unserializer or key_serializer is None, the other one must be None too"
            key_serializer = dummy_key_serializer
            key_unserializer = dummy_key_unserializer
        self.key_serializer = key_serializer
        self.key_unserializer = key_unserializer
        if value_serializer is None or value_unserializer is None:
            assert value_serializer is None and value_unserializer is None, "If value_unserializer or value_serializer is None, the other one must be None too"
            value_serializer = dummy_value_serializer
            value_unserializer = dummy_value_unserializer
        self.value_serializer = value_serializer
        self.value_unserializer = value_unserializer

        class CustomLmdbDict(LmdbDict):
            """Like SafeLmdbDict but with our own serializer
            """
            __slots__ = ()

            @staticmethod
            def _deserialize_(raw):
                return value_unserializer(raw)

            @classmethod
            def _serialize_(cls, value):
                return value_serializer(value)

        self.val_db = CustomLmdbDict(
            path=self.database_path,
            name="PersistDict_values",
            max_dbs=3,
            cache=LRUCache128 if self.caching else DummyCache,
            map_size=10485760 * 1000,
        )
        self.metadata_db = SafeLmdbDict(
            path=self.database_path,
            name="PersistDict_metadata",
            max_dbs=3,
            cache=LRUCache128 if self.caching else DummyCache,
            map_size=10485760 * 1000,
        )
        self.info_db = SafeLmdbDict(
            path=self.database_path,
            name="PersistDict_info",
            max_dbs=3,
            cache=LRUCache128 if self.caching else DummyCache,
            map_size=10485760 * 1000,
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

        # Start background initialization thread
        self._start_init_thread()
        
        # Start expiration thread if needed
        if self.expiration_days and self.async_expire:
            self._start_expire_thread()

    def _start_init_thread(self) -> None:
        """
        Start the background thread for initialization tasks.
        
        This method creates and starts a daemon thread that performs integrity checks
        and initial expiration in the background, allowing the main thread to continue.
        """
        if self._init_thread is not None and self._init_thread.is_alive():
            self._log("Init thread already running")
            return
            
        self._init_complete.clear()
        self._init_thread = threading.Thread(
            target=self._init_thread_worker,
            daemon=True,
            name="PersistDict-Init"
        )
        self._init_thread.start()
        self._log("Started background initialization thread")
        
    def _init_thread_worker(self) -> None:
        """
        Worker function for the initialization thread.
        
        This function runs integrity checks and initial expiration in the background.
        """
        try:
            # Run integrity check
            self.__integrity_check__()
            
            # Run initial expiration if needed and not using async expiration
            if self.expiration_days and not self.async_expire:
                self.__expire__()
                
            # Process any queued operations that came in during initialization
            while True:
                try:
                    func, args, kwargs, result_queue = self._init_queue.get(block=False)
                    try:
                        result = func(*args, **kwargs)
                        if result_queue:
                            result_queue.put((True, result))
                    except Exception as e:
                        if result_queue:
                            result_queue.put((False, e))
                    finally:
                        self._init_queue.task_done()
                except queue.Empty:
                    break
        except Exception as e:
            self._log(f"Error in initialization thread: {str(e)}")
        finally:
            # Set the completion flag with the lock to ensure visibility
            with self._init_lock:
                self._init_complete.set()
            self._log("Background initialization completed")
    
    def _start_expire_thread(self) -> None:
        """
        Start the background thread for periodic expiration checks.
        
        This method creates and starts a daemon thread that periodically checks
        for expired entries and removes them from the database.
        """
        if self._expire_thread is not None and self._expire_thread.is_alive():
            self._log("Expire thread already running")
            return
            
        self._stop_expire_thread.clear()
        self._expire_thread = threading.Thread(
            target=self._expire_thread_worker,
            daemon=True,
            name="PersistDict-Expire"
        )
        self._expire_thread.start()
        self._log(f"Started expiration thread with interval {self.expire_check_interval}s")
        
    def _expire_thread_worker(self) -> None:
        """
        Worker function for the expiration thread.
        
        This function runs in a separate thread and periodically calls the
        expiration method to remove expired entries.
        """
        while not self._stop_expire_thread.is_set():
            try:
                self.__expire__()
            except Exception as e:
                self._log(f"Error in expiration thread: {str(e)}")
            
            # Sleep for the specified interval, but check periodically if we should stop
            for _ in range(min(36, self.expire_check_interval // 10)):
                if self._stop_expire_thread.is_set():
                    break
                time.sleep(10)  # Check every 10 seconds if we should stop
            
            # Sleep any remaining time
            remaining = self.expire_check_interval % 10
            if remaining > 0 and not self._stop_expire_thread.is_set():
                time.sleep(remaining)
                
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
            
        # Use a lock to prevent multiple expiration processes running simultaneously
        if not self._expire_lock.acquire(blocking=False):
            self._log("Another expiration process is already running")
            return
            
        try:
            assert self.expiration_days > 0, "expiration_days has to be a positive int or 0 to disable"
            expiration_date = datetime.datetime.now() - datetime.timedelta(days=self.expiration_days)

            # Get keys to expire
            keys_to_delete = []
            for k in self.val_db.keys():
                try:
                    if self.metadata_db[k]["atime"] <= expiration_date:
                        keys_to_delete.append(k)
                except KeyError:
                    # Handle case where metadata might be missing
                    self._log(f"Missing metadata for key {k}, marking for deletion")
                    keys_to_delete.append(k)
                    
            if not keys_to_delete:
                self._log("No keys to expire")
                return
                
            # Delete expired keys
            total_keys = len(self.val_db)
            for k in keys_to_delete:
                try:
                    del self.val_db[k]
                    if k in self.metadata_db:
                        del self.metadata_db[k]
                except Exception as e:
                    self._log(f"Error deleting expired key {k}: {str(e)}")
                    
            self._log(f"Expiration removed {len(keys_to_delete)} keys, remaining: {len(self.val_db)}")
        except Exception as e:
            self._log(f"Error during expiration: {str(e)}")
        finally:
            self._expire_lock.release()

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

    def wait_for_init(self, timeout=None) -> bool:
        """
        Wait for the initialization thread to complete.
        
        Args:
            timeout (float, optional): Maximum time to wait in seconds.
                                      If None, wait indefinitely.
        
        Returns:
            bool: True if initialization completed, False if timed out.
        """
        if self._init_thread is None:
            return True
            
        return self._init_complete.wait(timeout)
        
    def _ensure_init_complete(self, func, *args, **kwargs):
        """
        Ensure initialization is complete before executing a function.
        
        If initialization is still in progress, either wait for it or
        queue the operation to be executed by the init thread.
        
        Args:
            func: The function to execute
            *args, **kwargs: Arguments to pass to the function
            
        Returns:
            The result of the function call
        """
        # If initialization is complete, just run the function
        if self._init_complete.is_set():
            return func(*args, **kwargs)
            
        # If we're in the init thread, just run the function
        if threading.current_thread().name == "PersistDict-Init":
            return func(*args, **kwargs)
            
        # Check again with a lock to avoid race conditions
        with self._init_lock:
            if self._init_complete.is_set():
                return func(*args, **kwargs)
                
            # Queue the operation
            result_queue = queue.Queue()
            self._init_queue.put((func, args, kwargs, result_queue))
        
        # Wait for result with a short timeout
        try:
            success, result = result_queue.get(timeout=0.1)
            if not success:
                raise result  # Re-raise the exception
            return result
        except queue.Empty:
            # If we timed out waiting for the result, wait for init to complete
            # and then execute the function directly
            self.wait_for_init()
            return func(*args, **kwargs)
    
    def __getitem__(self, key: str) -> Any:
        self._log(f"getting item at key {key}")
        
        def _get_item(key):
            ks = self.key_serializer(self.hash_and_crop(key))
            val = self.val_db[ks]
            assert self.metadata_db[ks]["fullkey"].startswith(key)
            self.metadata_db[ks]["atime"] = datetime.datetime.now()
            return val
            
        return self._ensure_init_complete(_get_item, key)

    def __setitem__(self, key: str, value: Any) -> None:
        self._log(f"setting item at key {key}")
        
        def _set_item(key, value):
            ks = self.key_serializer(self.hash_and_crop(key))
            if ks in self.val_db:
                assert self.metadata_db[ks]["fullkey"].startswith(key), f"Collision for key '{key}'"
            self.val_db[ks] = value
            t = datetime.datetime.now()
            self.metadata_db[ks] = {"ctime": t, "atime": t, "fullkey": key}
            
        self._ensure_init_complete(_set_item, key, value)

    def __delitem__(self, key: str) -> None:
        self._log(f"deleting item at key {key}")
        
        def _del_item(key):
            ks = self.key_serializer(self.hash_and_crop(key))
            assert self.metadata_db[ks]["fullkey"].startswith(key)
            del self.val_db[ks], self.metadata_db[ks]
            
        self._ensure_init_complete(_del_item, key)

    def clear(self) -> None:
        self._log("Clearing database")
        
        def _clear():
            keys = list(self.val_db.keys())
            for k in keys:
                del self.val_db[k], self.metadata_db[k]
            self.info_db["already_called"] = False
            
        self._ensure_init_complete(_clear)
        
    def stop_expire_thread(self) -> None:
        """
        Stop the background expiration thread if it's running.
        
        This method should be called when the PersistDict instance is no longer needed
        to ensure clean shutdown of background threads.
        """
        if self._expire_thread and self._expire_thread.is_alive():
            self._log("Stopping expiration thread")
            self._stop_expire_thread.set()
            # Don't join here as it might block indefinitely

    def __len__(self) -> int:
        self._log("getting length")
        
        def _len():
            return len(self.val_db)
            
        return self._ensure_init_complete(_len)

    def __contains__(self, key: str) -> bool:
        self._log(f"checking if val_db contains key {key}")
        
        def _contains(key):
            ks = self.key_serializer(self.hash_and_crop(key))
            return ks in self.val_db.keys()
            
        return self._ensure_init_complete(_contains, key)

    def __del__(self) -> None:
        """
        Clean up resources when the object is garbage collected.
        """
        self.stop_expire_thread()
        self.wait_for_init(timeout=0.5)  # Give init thread a chance to finish
        
    def __repr__(self) -> str:
        return {k: v for k, v in self.items()}.__repr__()

    def __str__(self) -> str:
        return {k: v for k, v in self.items()}.__str__()

    def keys(self) -> Generator[str, None, None]:
        "get the list of keys present in the db, sorted by ctime"
        self._log("getting keys")
        
        # Wait for initialization to complete
        self.wait_for_init()
        
        all_keys = list(self.val_db.keys())
        all_fullkeys = {k: self.metadata_db[k]["fullkey"] for k in all_keys}
        all_ctime = {k: self.metadata_db[k]["ctime"] for k in all_keys}

        all_keys = sorted(all_keys, key=lambda k: all_ctime[k])
        output = [all_fullkeys[k] for k in all_keys]
        for k in output:
            yield k

    def values(self) -> Generator[Any, None, None]:
        "get the list of values present in the db"
        self._log("getting values")
        for k in self.keys():
            yield self[k]

    def items(self) -> Generator[Tuple[str, Any], None, None]:
        self._log("getting items")
        for k in self.keys():
            yield k, self[k]

    def hash_and_crop(self, string):
        """Hash a string with SHA256 and crop to desired length (default 16 chars)"""
        return hashlib.sha256(string.encode('utf-8')).hexdigest()[:self.key_size_limit]

