import zlib
import base64
import hashlib
import os
import threading
import concurrent.futures
import time
import functools
import traceback
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

def thread_safe(method):
    """
    Decorator to ensure thread safety for PersistDict methods.
    Acquires the lock before executing the method and releases it afterward.
    If minimal_locking is enabled, this will only use locks for methods that
    modify Python objects, not for operations that only interact with LMDB.
    """
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        if self.minimal_locking and getattr(method, '_no_lock_needed', False):
            return method(self, *args, **kwargs)
        with self._lock:
            return method(self, *args, **kwargs)
    return wrapper

def no_lock_needed(method):
    """
    Marker decorator to indicate methods that don't need locking
    when minimal_locking is enabled because they only interact with
    thread-safe LMDB operations.
    """
    method._no_lock_needed = True
    return method

@typechecker
class PersistDict(dict):
    __VERSION__: str = "0.2.7"

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
        background_thread: bool = True,
        background_timeout: int = 30,  # Maximum time in seconds for background operations
        name: str = "",  # Name identifier for logging purposes
        minimal_locking: bool = False,  # Reduce locking for better performance
        ) -> None:
        """
        Initialize a PersistDict instance.

        The PersistDict class provides a persistent dictionary-like interface, storing data in a LMDB database.
        It supports optional automatic expiration of entries.
        Note that no checks are done to make sure the serializer is always the same. So if you change it and call the same db as before the serialization will fail.

        Args:
            database_path (Union[str, PosixPath]): Path to the LMDB database folder. Note that this is a folder, not a file.
            expiration_days (Optional[int], default=0): Number of days after which entries expire. 0 means no expiration.
                Note that expiration checks are only performed at initialization time, not during normal operations.
            key_serializer (Callable, default=key_to_string): Function to serialize keys before storing. If None, no serializer will be used, but this can lead to issue.
            key_unserializer (Callable, default=string_to_key): Function to deserialize keys after retrieval. If None, no unserializer will be used, but this can lead to issues.
            key_size_limit (int, default=511): Maximum size for the key. If the key is larger than this it will be hashed then cropped.
            value_serializer (Callable, default=pickle.dumps): Function to serialize values before storing. If None, no serializer will be used, but this can lead to issue.
            value_unserializer (Callable, default=pickle.loads): Function to deserialize values after retrieval. If None, no unserializer will be used, but this can lead to issues.
            caching (bool, default=True): If False, don't use LMDB's built in caching. Beware that you can't change the caching method if an instance is already declared to use the db.
            verbose (bool, default=False): If True, enables verbose logging.
            background_thread (bool, default=True): If True, runs integrity check and expiration in a background thread.
                If False, these operations run in the current thread during initialization. Set to False for better
                determinism or in environments where threading is problematic.
            name (str, default=""): Optional name identifier for the PersistDict instance. Used in logging messages
                to identify which PersistDict instance is generating the logs when multiple instances exist.
            minimal_locking (bool, default=False): If True, reduces the use of locks for better performance.
                Since LMDB is already thread-safe, this only uses locks for operations that modify Python objects.
                Enable this for better performance in multi-threaded environments, but be aware that some
                race conditions might still occur with Python-level operations.
        """
        self.verbose = verbose
        self.name = name
        self._log(".__init__")
        self.expiration_days = expiration_days
        self.database_path = Path(database_path)
        self.caching = caching
        self.key_size_limit = key_size_limit
        self.background_thread = background_thread
        self.background_timeout = max(5, background_timeout)  # Ensure minimum timeout
        self.minimal_locking = minimal_locking
        
        # Thread safety
        self._lock = threading.RLock()
        self._bg_thread = None
        self._stop_event = threading.Event()
        self._bg_task_complete = threading.Event()
        self._initialization_complete = False  # Flag to track initialization state

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
        if "oldest_atime" not in self.info_db:
            self.info_db["oldest_atime"] = datetime.datetime.now()

        # Run integrity check and expiration
        if self.background_thread:
            # Start background thread for integrity check and expiration
            self._start_background_thread()
        else:
            # Run in current thread
            self._log("Running integrity check and expiration in current thread")
            try:
                start_time = time.time()
                self.__integrity_check__()
                self.__expire__()
                self._log(f"Initialization tasks completed in {time.time() - start_time:.2f} seconds")
            except Exception as e:
                self._log(f"Error during initialization tasks: {str(e)}")
                self._log(f"Traceback: {traceback.format_exc()}")
                # Continue initialization despite errors
        
        # Mark initialization as complete
        self._initialization_complete = True

    @thread_safe
    def _start_background_thread(self) -> None:
        """
        Start a background thread for integrity check and expiration.
        Only used when background_thread=True.
        
        This method is thread-safe and will not start a new thread if one is already running.
        """
        self._log("Starting background thread for integrity check and expiration")
        
        # Don't start a new thread if one is already running
        if self._bg_thread and self._bg_thread.is_alive():
            self._log("Background thread already running, not starting a new one")
            return
            
        # Reset events before starting thread
        self._stop_event.clear()
        self._bg_task_complete.clear()
            
        self._bg_thread = threading.Thread(
            target=self._background_task,
            daemon=True,
            name="PersistDict-BgThread"
        )
        self._bg_thread.start()
    
    def _background_task(self) -> None:
        """
        Background task that performs integrity check and expiration once.
        The thread terminates after completing these tasks.
        
        This method handles its own exceptions and always signals completion
        via the _bg_task_complete event, even if errors occur.
        """
        start_time = time.time()
        try:
            thread_id = threading.get_ident()
            self._log(f"Background thread started (thread id: {thread_id})")
            
            # Check if we should stop before starting work
            if self._stop_event.is_set():
                self._log("Background thread stopping before work begins")
                return
                
            # Run integrity check with timeout monitoring
            self._log("Background thread running integrity check")
            integrity_start = time.time()
            self.__integrity_check__()
            self._log(f"Integrity check completed in {time.time() - integrity_start:.2f} seconds")
            
            # Check if we should stop before expiration
            if self._stop_event.is_set():
                self._log("Background thread stopping after integrity check")
                return
                
            # Run expiration with timeout monitoring
            self._log("Background thread running expiration")
            expiration_start = time.time()
            self.__expire__()
            self._log(f"Expiration completed in {time.time() - expiration_start:.2f} seconds")
            
            total_time = time.time() - start_time
            self._log(f"Background thread completed tasks in {total_time:.2f} seconds and is terminating")
        except Exception as e:
            self._log(f"Error in background thread: {str(e)}")
            self._log(f"Traceback: {traceback.format_exc()}")
        finally:
            # Signal that the background task is complete
            self._bg_task_complete.set()
    
    def __del__(self) -> None:
        """
        Clean up resources when the object is garbage collected.
        
        This method safely stops any background threads and suppresses exceptions
        that might occur during garbage collection.
        """
        try:
            # Only attempt cleanup if the object was fully initialized
            if hasattr(self, '_stop_event') and hasattr(self, '_bg_thread'):
                self._stop_background_thread()
        except Exception as e:
            # Avoid exceptions during garbage collection
            try:
                self._log(f"Error during cleanup in __del__: {str(e)}")
            except:
                # If even logging fails, just silently continue
                pass
    
    @thread_safe
    def _stop_background_thread(self) -> None:
        """
        Stop the background thread if it's running.
        Waits for the thread to complete or times out.
        
        This method is thread-safe and handles the case where the current thread
        is the background thread to avoid deadlocks.
        """
        # Quick check if there's no thread to stop
        if not self._bg_thread:
            return
            
        # Check if thread is alive with lock protection
        if self._bg_thread.is_alive():
            thread_id = self._bg_thread.ident
            self._log(f"Stopping background thread (thread id: {thread_id})")
            self._stop_event.set()
            
            # Don't try to join the current thread (would deadlock)
            current_thread = threading.current_thread()
            if current_thread != self._bg_thread:
                # First wait for task completion with timeout
                task_completed = self._bg_task_complete.wait(timeout=5)
                if not task_completed:
                    self._log("Background task didn't complete in time")
                
                # Then wait for thread to terminate
                self._bg_thread.join(timeout=3)
                
                if self._bg_thread.is_alive():
                    self._log("Warning: Background thread did not terminate properly")
                else:
                    self._log(f"Background thread (id: {thread_id}) successfully terminated")
            else:
                self._log("Cannot wait for background thread from within itself")
            
        # Clear the thread reference
        self._bg_thread = None

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
            This method is called internally during initialization only and should not be called 
            directly by users. Expiration checks are NOT performed automatically during normal 
            dictionary operations.
        """
        self._log("expirating")
        if not self.expiration_days:
            return
            
        try:
            if self.expiration_days <= 0:
                self._log("Invalid expiration_days value, must be positive or 0")
                return
                
            expiration_date = datetime.datetime.now() - datetime.timedelta(days=self.expiration_days)
            
            # Skip expiration check if oldest atime is newer than expiration date
            if "oldest_atime" in self.info_db and self.info_db["oldest_atime"] > expiration_date:
                self._log("Skipping expiration check - oldest atime is newer than expiration date")
                return

            # Get keys to expire
            keys_to_delete = []
            oldest_atime = None
            all_keys = list(self.val_db.keys())
            
            for k in all_keys:
                # Check if we should stop
                if self._stop_event.is_set():
                    self._log("Expiration interrupted by stop event")
                    return
                    
                try:
                    if k not in self.metadata_db:
                        self._log(f"Missing metadata for key {k}, marking for deletion")
                        keys_to_delete.append(k)
                        continue
                        
                    if "atime" not in self.metadata_db[k]:
                        self._log(f"Missing atime for key {k}, marking for deletion")
                        keys_to_delete.append(k)
                        continue
                        
                    current_atime = self.metadata_db[k]["atime"]
                    if oldest_atime is None or current_atime < oldest_atime:
                        oldest_atime = current_atime
                        
                    if current_atime <= expiration_date:
                        keys_to_delete.append(k)
                except Exception as e:
                    # Handle any unexpected errors when checking keys
                    self._log(f"Error checking expiration for key {k}: {str(e)}")
                    keys_to_delete.append(k)
            
            # Update oldest_atime in info_db
            if oldest_atime is not None:
                self.info_db["oldest_atime"] = oldest_atime
                    
            if not keys_to_delete:
                self._log("No keys to expire")
                return
                
            # Delete expired keys
            total_keys = len(self.val_db)
            deleted_count = 0
            
            for k in keys_to_delete:
                # Check if we should stop
                if self._stop_event.is_set():
                    self._log(f"Expiration deletion interrupted after {deleted_count}/{len(keys_to_delete)} keys")
                    break
                    
                try:
                    del self.val_db[k]
                    if k in self.metadata_db:
                        del self.metadata_db[k]
                    deleted_count += 1
                except Exception as e:
                    self._log(f"Error deleting expired key {k}: {str(e)}")
                    
            self._log(f"Expiration removed {deleted_count} keys, remaining: {len(self.val_db)}")
        except Exception as e:
            self._log(f"Error during expiration: {str(e)}")
            self._log(f"Traceback: {traceback.format_exc()}")

    @thread_safe
    def __integrity_check__(self) -> None:
        """
        Perform an integrity check on the database.

        This method checks the integrity of the LMDB database and verifies the consistency
        of creation times (ctime) and access times (atime) for all entries, as
        well as check that they all have the same keys.

        Note:
            This method is called internally during initialization only and should not be called 
            directly by users. Integrity checks are NOT performed automatically during normal 
            dictionary operations.
        """
        self._log("checking integrity of db")
        
        oldest_atime = None
        keys_to_check = list(self.val_db.keys())
        
        for k in keys_to_check:
            # Check if we should stop
            if self._stop_event.is_set():
                self._log("Integrity check interrupted by stop event")
                return
                
            try:
                k2 = self.key_unserializer(k)
            except Exception as e:
                self._log(f"Couldn't unserialize key '{k}'. This might be due to changing the serialization in between runs. Error: {str(e)}")
                try:
                    del self.val_db[k]
                    if k in self.metadata_db:
                        del self.metadata_db[k]
                except Exception as del_err:
                    self._log(f"Error deleting corrupted key: {str(del_err)}")
                continue
                
            try:
                if k not in self.metadata_db:
                    self._log(f"Item of key '{k2}' is missing from metadata_db, removing from val_db")
                    del self.val_db[k]
                    continue
                    
                if "atime" not in self.metadata_db[k]:
                    self._log(f"Item of key '{k2}' is missing atime metadata, fixing")
                    self.metadata_db[k]["atime"] = datetime.datetime.now()
                    
                if "ctime" not in self.metadata_db[k]:
                    self._log(f"Item of key '{k2}' is missing ctime metadata, fixing")
                    self.metadata_db[k]["ctime"] = datetime.datetime.now()
                    
                if self.metadata_db[k]["ctime"] > self.metadata_db[k]["atime"]:
                    self._log(f"Item of key '{k2}' has ctime after atime, fixing")
                    self.metadata_db[k]["atime"] = self.metadata_db[k]["ctime"]
                
                # Track oldest atime
                if oldest_atime is None or self.metadata_db[k]["atime"] < oldest_atime:
                    oldest_atime = self.metadata_db[k]["atime"]
            except Exception as e:
                self._log(f"Error processing key '{k2}': {str(e)}")

        l1 = len(self.val_db)
        l2 = len(self.metadata_db)
        assert l1 == l2, f"val_db and metadata_db sizes differ: {l1} vs {l2}"

        assert "version" in self.info_db, "info_db is missing the key 'version'"
        assert "ctime" in self.info_db, "info_db is missing the key 'ctime'"
        assert "already_called" in self.info_db, "info_db is missing the key 'already_called'"
        
        # Update oldest_atime in info_db
        if oldest_atime is not None:
            self.info_db["oldest_atime"] = oldest_atime
        elif len(self.val_db) == 0:
            # If database is empty, set oldest_atime to current time
            self.info_db["oldest_atime"] = datetime.datetime.now()

    def _log(self, message: str) -> None:
        if self.verbose:
            name_prefix = f"[{self.name}]" if self.name else ""
            debug(f"PersistDict{name_prefix}:" + message)

    @thread_safe
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

    @thread_safe
    @no_lock_needed
    def __getitem__(self, key: str) -> Any:
        self._log(f"getting item at key {key}")
        ks = self.key_serializer(self.hash_and_crop(key))
        val = self.val_db[ks]
        assert self.metadata_db[ks]["fullkey"].startswith(key)
        self.metadata_db[ks]["atime"] = datetime.datetime.now()
        return val

    @thread_safe
    def __setitem__(self, key: str, value: Any) -> None:
        self._log(f"setting item at key {key}")
        ks = self.key_serializer(self.hash_and_crop(key))
        if ks in self.val_db:
            assert self.metadata_db[ks]["fullkey"].startswith(key), f"Collision for key '{key}'"
        self.val_db[ks] = value
        t = datetime.datetime.now()
        self.metadata_db[ks] = {"ctime": t, "atime": t, "fullkey": key}
        
        # Update oldest_atime if this is the first item or if current oldest_atime is None
        if len(self.val_db) == 1 or "oldest_atime" not in self.info_db:
            self.info_db["oldest_atime"] = t

    @thread_safe
    def __delitem__(self, key: str) -> None:
        self._log(f"deleting item at key {key}")
        ks = self.key_serializer(self.hash_and_crop(key))
        assert self.metadata_db[ks]["fullkey"].startswith(key)
        del self.val_db[ks], self.metadata_db[ks]

    @thread_safe
    def clear(self) -> None:
        self._log("Clearing database")
        keys = list(self.val_db.keys())
        for k in keys:
            del self.val_db[k], self.metadata_db[k]
        self.info_db["already_called"] = False
        self.info_db["oldest_atime"] = datetime.datetime.now()
        
    @thread_safe
    @no_lock_needed
    def __len__(self) -> int:
        self._log("getting length")
        return len(self.val_db)

    @thread_safe
    @no_lock_needed
    def __contains__(self, key: str) -> bool:
        self._log(f"checking if val_db contains key {key}")
        ks = self.key_serializer(self.hash_and_crop(key))
        return ks in self.val_db.keys()
        
    def __repr__(self) -> str:
        return {k: v for k, v in self.items()}.__repr__()

    def __str__(self) -> str:
        return {k: v for k, v in self.items()}.__str__()

    @thread_safe
    @no_lock_needed
    def keys(self) -> Generator[str, None, None]:
        "get the list of keys present in the db, sorted by ctime"
        self._log("getting keys")
        all_keys = list(self.val_db.keys())
        all_fullkeys = {k: self.metadata_db[k]["fullkey"] for k in all_keys}
        all_ctime = {k: self.metadata_db[k]["ctime"] for k in all_keys}

        all_keys = sorted(all_keys, key=lambda k: all_ctime[k])
        output = [all_fullkeys[k] for k in all_keys]
        for k in output:
            yield k

    @thread_safe
    @no_lock_needed
    def values(self) -> Generator[Any, None, None]:
        "get the list of values present in the db"
        self._log("getting values")
        for k in self.keys():
            yield self[k]

    @thread_safe
    @no_lock_needed
    def items(self) -> Generator[Tuple[str, Any], None, None]:
        self._log("getting items")
        for k in self.keys():
            yield k, self[k]

    def hash_and_crop(self, string):
        """Hash a string with SHA256 and crop to desired length (default 16 chars)"""
        return hashlib.sha256(string.encode('utf-8')).hexdigest()[:self.key_size_limit]

