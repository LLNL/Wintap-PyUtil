"""
Utility functions involving multiprocessing

Contains functions for creating, getting, and terminating a main thread pool, as well as helpers to more easily map
data to multiprocessing function calls.
"""

import os
import traceback
import multiprocessing
import numpy as np
import signal
import pickle
import sys
import time
import datetime
from timeit import default_timer
from .misc_utils import progressbar, get_module, log
from .cfg_utils import update_atomic_tokens


# Possible thread pool for multiprocessing
THREAD_POOL = None
_THREAD_POOL_INITIALIZED = False
THREAD_POOL_NUM_WORKERS = 0
MAX_DEFAULT_NUM_WORKERS = 32
DEFAULT_POOL_SIGKILL_TIMEOUT_SECONDS = 20

# The number of seconds to wait before attempting to aquire a lock after failing
AQUIRE_LOCK_FAIL_WAIT_TIME_SECONDS = 0.1


def init_thread_pool(num_workers=None):
    """Initializes the thread pool.

    Args:
        num_workers (int, optional): number of processes. Will default to min(os.cpu_count(), MAX_DEFAULT_NUM_WORKERS) 
            if None. Defaults to None.
    """
    global THREAD_POOL, _THREAD_POOL_INITIALIZED, THREAD_POOL_NUM_WORKERS

    if not _THREAD_POOL_INITIALIZED:
        num_workers = min(os.cpu_count(), MAX_DEFAULT_NUM_WORKERS) if num_workers is None else num_workers
        ctx = multiprocessing.get_context('fork')

        THREAD_POOL_NUM_WORKERS = num_workers
        THREAD_POOL = ctx.Pool(num_workers)
        _THREAD_POOL_INITIALIZED = True


def terminate_thread_pool(send_sigint=False):
    """Terminates the thread pool by first sending SIGINT, waiting for a few seconds, then SIGTERM if any are still running
    
    Code from: https://stackoverflow.com/questions/47553120/kill-a-multiprocessing-pool-with-sigkill-instead-of-sigterm-i-think
    """
    global THREAD_POOL, _THREAD_POOL_INITIALIZED

    if _THREAD_POOL_INITIALIZED:
        THREAD_POOL.close()

        # Do all this in a try/catch just in case something happens, we can force-terminate
        try:
            if send_sigint:
                # stop repopulating new child
                THREAD_POOL._state = multiprocessing.pool.TERMINATE
                THREAD_POOL._worker_handler._state = multiprocessing.pool.TERMINATE

                # Redirect stdout only for this section because of those pesky KeyboardInterrupt logs
                with open(os.devnull, 'w') as devnull:
                    with RedirectStdStreams(stdout=devnull):
                        for p in THREAD_POOL._pool:
                            os.kill(p.pid, signal.SIGINT)

                        # .is_alive() will reap dead process
                        init_time = default_timer()
                        while any(p.is_alive() for p in THREAD_POOL._pool):
                            if default_timer() - init_time > DEFAULT_POOL_SIGKILL_TIMEOUT_SECONDS:
                                break
        except:
            THREAD_POOL.terminate()
            raise
        
        THREAD_POOL.terminate()
        _THREAD_POOL_INITIALIZED = False
        THREAD_POOL = None


class RedirectStdStreams:
    """Context manager to temporarily redirect stdout and stderr streams
    
    NOTE: the passed streams will NOT be closed on exit
    """
    def __init__(self, stdout=None, stderr=None):
        self.stdout = stdout if stdout is not None else sys.stdout
        self.stderr = stderr if stderr is not None else sys.stderr
    
    def __enter__(self):
        self._stdout_old, self._stderr_old = sys.stdout, sys.stderr
        self._stderr_old.flush()
        self._stderr_old.flush()
        sys.stdout, sys.stderr = self.stdout, self.stderr
    
    def __exit__(self, *args):
        sys.stdout.flush()
        sys.stderr.flush()
        sys.stdout, sys.stderr = self._stdout_old, self._stderr_old


def get_thread_pool(ret_n=False, num_workers=None):
    """Returns (starts if needed) the current thread pool.

    Args:
        ret_n (bool, optional): if True, also returns the number of workers. Defaults to False.
        num_workers (Union[int, None], optional): if not None, then the number of workers to use when initializing the 
            thread pool. Only used if the thread pool is not currently initialized. Defaults to None.

    Returns:
        Union[Pool, Tuple[Pool, int]]: the current global multiprocessing.Pool() object, or tuple of (pool, num_workers)
            if `ret_n=True`
    """
    if not _THREAD_POOL_INITIALIZED:
        init_thread_pool(num_workers=num_workers)
    
    return THREAD_POOL if not ret_n else (THREAD_POOL, THREAD_POOL_NUM_WORKERS)


class ThreadPoolManager:
    """A custom context manager to handle the global thread pool.

    By default, this will only terminate the thread pool when an error was raised, however you can pass
    `terminate=True` to always terminate the thread pool after exiting this context. You can get the thread pool 
    and number of workers with something like:

    .. highlight:: python
    .. code-block:: python

        with ThreadPoolManager(num_workers=10) as tpm:
            pool = tpm.pool
            num_workers = tpm.num_workers

    Parameters
    ----------
    terminate: `bool`
        if True, will terminate the thread pool after exiting, even if no error was raised
    num_workers: `Optional[int]`
        the number of workers to pass to get_thread_pool()

        NOTE: will only determine the number of workers if the thread pool as not yet been initialized. Otherwise
        the number of workers will be whatever it was previously set to
    """

    pool = None
    """the multiprocessing process pool"""

    num_workers = None
    """the number of processes in the pool"""
    
    def __init__(self, terminate=False, num_workers=None):
        self.terminate = terminate
        self.pool, self.num_workers = get_thread_pool(ret_n=True, num_workers=num_workers)
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        if self.terminate or exc_type is not None:
            terminate_thread_pool()


_ATOMIC_READ_RAISE_ERR = object()


class AtomicData:
    """A class that allows for atomic reading/updating of the given data to a pickle file
    
    Parameters
    ----------
        init_data: `Any`
            Data to initialize the atomic file with. If the atomic file already exists, then that data will be loaded
        filepath: `Optional[str]`
            An optional filepath to store the dictionary, otherwise will be stored at './atomic_dict.pkl'
        lockpath: `Optional[str]`
            An optional filepath for the lock file to use to atomically update the dictionary, otherwise will be
                stored at './.[filepath].lock' where [filepath] is the given `filepath` parameter
        max_read_attempts: `Optional[int]`
            An optional integer specifying the maximum number of attempts to atomically read this dictionary before
                giving up and raising an error. Set to None to attempt indefinitely. Defaults to None
    """

    def __init__(self, init_data, filepath=None, lock_path=None, max_read_attempts=None):
        self._filepath = './atomic_data.pkl' if filepath is None else filepath
        self._lock_path = os.path.join(os.path.dirname(self._filepath), '.%s.lock' % os.path.basename(self._filepath)) if lock_path is None else lock_path
        self._lock = None

        if max_read_attempts is not None and max_read_attempts <= 0:
            raise ValueError("max_read_attempts must be > 0: %d" % max_read_attempts)
        self._max_read_attempts = 2**100 if max_read_attempts is None else max_read_attempts

        # Get the initial data
        self.atomic_read(default=init_data)
    
    def atomic_read(self, default=_ATOMIC_READ_RAISE_ERR):
        """Atomically reads the data from file, updating self.data
        
        Args:
            default (Optional[Any]): If this is passed and the file does not already exist, then this data will be saved
                to file and set to self.data
        """
        with _AquireLock(self._max_read_attempts, self._lock_path):

            # If the path doesn't exist, check if we need to raise an error, or update the file
            if not os.path.exists(self._filepath):
                if default is not _ATOMIC_READ_RAISE_ERR:
                    self.data = default
                    self._locked_write()
                else:
                    raise FileNotFoundError('Could not find inital atomic file to read from, and `default` data was not passed: %s' % self._filepath)
            
            # Otherwise it does exist, update self
            else:
                self.data = self._locked_read()
    
    def _locked_read(self):
        """Reads the data from file, assuming a lock has already been aquired"""
        with open(self._filepath, 'rb') as f:
            return pickle.load(f)
    
    def atomic_write(self):
        """Atomically writes the data at self.data to the pickle file"""
        with _AquireLock(self._max_read_attempts, self._lock_path):
            with open(self._filepath, 'wb') as f:
                pickle.dump(self.data, f)
    
    def _locked_write(self):
        """Writes the data at self.data to file, assuming a lock has already been aquired"""
        with open(self._filepath, 'wb') as f:
            pickle.dump(self.data, f)
    
    def atomic_update(self, update_func, *update_args, **update_kwargs):
        """Atomically updates the data
        
        Will first aquire a lock on the data, read it in, then call `update_func(file_data, update_data)` where `file_data`
        is the data from the current atomic file, then write the data back to file and finally release the lock.

        NOTE: this will prevent any and all updates to the atomic file until update_func has completed

        NOTE: any errors within the update_func will be handled properly and will likely not mess up the atomic file

        Args:
            update_func (Callable): function that takes in: the data currently saved in file, the current data, then the 
                passed args and kwargs, and returns the updated data to write back to file
            update_args (Any): args to pass to update_func, after the current data saved in file
            update_kwargs (Any): kwargs to pass to update_func
        
        Returns:
            Any: the updated data
        """
        with _AquireLock(self._max_read_attempts, self._lock_path):
            self.data = update_func(self._locked_read(), self.data, *update_args, **update_kwargs)
            self._locked_write()
            return self.data
    
    def aquire_lock(self):
        """Aquires the lock needed to update data
        
        NOTE: this will prevent any and all updates to the atomic file until self.release_lock() is called. Make sure
        you call it quickly or other processes may hang!

        NOTE: if the lock has already been aquired, nothing will happen

        NOTE: it can be dangerous to attempt to aquire locks yourself, as any errors raised must be handled nicely and
        self.release_lock() must be called otherwise other processes may hang
        """
        if self._lock is None:
            self._lock = _AquireLock(self._max_read_attempts, self._lock_path).__enter__()
    
    def release_lock(self):
        """Releases the lock. Assumes it has already been aquired, otherwise an error will be raised"""
        if self._lock is None:
            raise ValueError("release_lock() was called, but the lock has not been aquired!")
        self._lock.__exit__()
        self._lock = None
    
    def delete_file(self):
        """Atomically deletes the file being used"""
        with _AquireLock(self._max_read_attempts, self._lock_path):
            if os.path.exists(self._filepath):
                os.remove(self._filepath)
    
    def __len__(self):
        """Gives length of current self.data"""
        return len(self.data)
    
    def __getstate__(self):
        """Doesn't send the actual data itself, that will be loaded"""
        ret = self.__dict__.copy()
        del ret['data']
        return ret

    def __setstate__(self, state):
        """Set the state as normal, but read in the data when done"""
        for k, v in state.items():
            setattr(self, k, v)
        self.atomic_read()


class _AquireLock:
    """Context manager to aquire a file lock, and remove it when done"""
    def __init__(self, max_attempts, lock_path):
        self._max_attempts, self._lock_path = max_attempts, lock_path
        get_module('atomicwrites', err_message='Package is required for atomic dictionary file!')
        from atomicwrites import atomic_write
        self.atomic_write = atomic_write
    
    def __enter__(self):
        log(message="Attempting to aquire lock...")
        for i in range(self._max_attempts):
            try:
                with self.atomic_write(self._lock_path, overwrite=False) as f:
                    f.write(datetime.datetime.now().isoformat())
                log(message="Lock aquired after %d attempts!" % i)

                return self
            except FileExistsError:
                pass
            
            time.sleep(AQUIRE_LOCK_FAIL_WAIT_TIME_SECONDS)

        raise AquireLockError(self._max_attempts, self._lock_path)
            
    def __exit__(self, exc_type, exc_value, exc_tb):
        log(message="Releasing lock!")
        if os.path.exists(self._lock_path):
            os.remove(self._lock_path)


class AquireLockError(Exception):
    def __init__(self, attempts, lock_path):
        super().__init__("Could not aquire file lock from file after %d attempts using lock path: %s" % (attempts, lock_path))


class AtomicTokenDict:
    """Acts like a normal token dictionary, but allows for atomic operations"""

    def __init__(self, init_data=None, **atomic_data_kwargs):
        self._data = AtomicData(init_data=init_data if init_data is not None else {}, **atomic_data_kwargs)
    
    def __getitem__(self, key):
        return self.data[key]
    
    def __setitem__(self, key, value):
        if key in self.data:
            if self.data[key] != value:
                raise ValueError("Cannot set token key to a new value! key: %s, value: %s" % (repr(key), value))
            return
        
        self._atomic_update({key: value})
    
    def __contains__(self, key):
        return key in self.data
    
    def __len__(self):
        return len(self.data)
    
    def __str__(self):
        return repr(self)
    
    def __repr__(self):
        return repr(self.data)
    
    def __iter__(self):
        return iter(self.data)
    
    def update(self, tokens):
        """Updates this dictionary with the given tokens
        
        Args:
            tokens (Dict[str, int]): dictionary mapping token strings to their integer values. Any tokens in the dictionary
                that are not in this dictionary will be added, and any tokens that already exist and have the same value
                will be ignored. If there are any tokens that already exist, but have a different value, then an error
                will be raised
        """
        update_tokens = {}
        for k, v in tokens.items():
            if k in self.data:
                if self.data[k] != v:
                    raise ValueError("Cannot set token key to a new value! key: %s, value: %s" % (repr(k), v))
                continue
            update_tokens[k] = v
        
        if len(update_tokens) > 0:
            self._atomic_update(update_tokens)
    
    def items(self):
        return self.data.items()
    
    def values(self):
        return self.data.values()
    
    def keys(self):
        return self.data.keys()
    
    def get(self, key, default=None):
        return self.data.get(key, default=default)

    def setdefault(self, key, default=None):
        """If the key exists, return the value. Otherwise set the key to the given default (or len(self) if default=None)"""
        if key in self.data:
            return self.data[key]
        
        self._atomic_update({key: len(self) if default is None else default})
    
    def addtokens(self, *tokens):
        """Adds the given tokens to this dictionary, ignoring any that already exist
        
        Args:
            tokens (str): arbitrary number of string tokens to add to this token dict
        """
        update_tokens = {}
        for t in tokens:
            if t not in self:
                update_tokens[t] = len(self) + len(update_tokens)
        
        if len(update_tokens) > 0:
            self._atomic_update(update_tokens)
    
    def _atomic_update(self, token_dict):
        """Atomically update the tokens from the given token_dict. Does no checks beforehand to see if there are any
        conflicts, duplicates, etc.
        
        Args:
            token_dict (Dict[str, int]): token dictionary to update with
        """
        self._data.atomic_update(update_atomic_tokens, token_dict)
    
    def delete_file(self):
        "Deletes the atomic token dictinoary file"
        self._data.delete_file()

    @property
    def data(self):
        """Returns the token dictionary"""
        return self._data.data


def _get_on_error(on_error):
    """map_mp() helper function to sanitize on_error input"""
    if isinstance(on_error, str):
        on_error = on_error.lower().replace("-", '_')
        if on_error in ['raise', 'reraise', 're_raise', 'raise_err', 'raiseerr', 'raise_error', 'raiseerror']:
            return 'raise'
        elif on_error in ['error_info', 'err_info', 'errinfo', 'errorinfo', 'info', 'retinfo', 'ret_info', 'return_info']:
            return 'error_info'
        elif on_error in ['ret_none', 'none', 'return_none', 'retnone', 'returnnone']:
            return 'return_none'
        elif on_error in ['ignore']:
            return 'ignore'
        else:
            raise ValueError("Unknown on_error string: '%s'" % on_error)
    elif callable(on_error):
        return on_error
    else:
        raise TypeError("Unknown on_error type: '%s', must be string or callable" % type(on_error))


def _call_func(elem, func, starmap):
    """:func:`~bincfg.utils.mp_utils.map_mp` helper function to actually call a function"""
    try:
        return None, func(*elem) if starmap else func(elem)
    except Exception as e:
        return (e, traceback.format_exc()), None


def _unpack_chunk(chunk, func, starmap):
    """:func:`~bincfg.utils.mp_utils.map_mp` helper function to unpack chunks"""
    return [_call_func(elem, func, starmap) for elem in chunk]


def _unpack_chunksize(elems, func, starmap, using_chunks):
    """map_mp() helper function to unpack a chunksize"""
    return [(_unpack_chunk(elem, func, starmap) if using_chunks else _call_func(elem, func, starmap)) for elem in elems]


def map_mp(func, items=None, chunks=None, starmap=False, chunksize=1, num_workers=None, terminate=False, on_error='raise', progress=False):
    """Splits the given inputs over multiple processes in the global process pool

    Maps the given items or chunks of items to the given function using multiprocessing, allowing for the use of a
    progressbar. Also handles terminating the pool.

    Args:
        func (Callable): the function to call
        items (Iterable[Any], optional): an iterable of items to map. Mutually exclusive with chunks. Defaults to None.
        chunks (Iterable[Iterable], optional): an iterable of iterables to map. Will send the entire chunk to another 
            process and call the given function on each element, then return the results unpacked into a single total 
            list. Mutually exclusive with items. Defaults to None.
        starmap (bool, optional): if True, will call `starmap()` instead of `map()` on the pool. This assumes each 
            element should be star-unpacked when sending to `func()`. Defaults to False.
        chunksize (int, optional): if > 1, then multiple elements will be passed in one 'chunk' to each process. This 
            would have the same effect (if using items) as passing chunks with this chunksize, but if using chunks, then
            this many chunks will be passed to each process instead. Defaults to 1.
        num_workers (int, optional): the num_workers to pass to :func:`bincfg.utils.get_thread_pool`. Defaults to None.
        
            NOTE: this only has an effect if the thread pool has not yet been initialized. Otherwise this will use the
            num_workers the thread pool has already been initialized with.
        terminate (bool, optional): if True, then the global thread pool will be terminated after use. If False, then 
            the thread pool will only be terminated if an error is raised. Defaults to False.
        on_error (_MpOnErrorType, optional): what to do when there is an error can be either a string describing what to
            do, or a callable in which case the callable will be called and its return value will be used in place of the 
            error. Callables should take as input the error_object and traceback in that order as args, and return whatever
            value should be used in place of the errored value. Acceptable strings:
            
                - 'raise': raise any errors immediately
                - 'error_info': return the error information in place of the expected output whenever there is an error. Error
                    information will be a 2-tuple of (error_object, traceback)
                - 'return_none': will return None in place of values whenever an error occurs, and the error will be ignored
                - 'ignore': ignore any errors and doesn't return any values for those which fail. 
            
            Defaults to 'raise'.
        progress (bool, optional): if True, will show a progressbar. Defaults to False.

    Raises:
        TypeError: If none or both of `items` and `chunks` are passed
        MultiprocessingError: If there was an error running the multiprocessing calls, and `on_error='raise'`

    Returns:
        List[Any]: a list of results
    """
    if (items is not None and chunks is not None) or (items is None and chunks is None):
        raise TypeError("Exactly one of items and chunks must be passed, not both")

    on_error = _get_on_error(on_error)
    
    # Determine if we are using chunks or not
    using_chunks, using_chunksize = items is None, chunksize > 1
    elems = items if chunks is None else chunks
    using_func, extra_args = (_unpack_chunk if using_chunks else _call_func), [func, starmap]

    # Figure out if we need to do chunksize or not
    if using_chunksize:
        # Getting splits instead of directly splitting object just to avoid an annoying error message...
        splits = np.array_split(range(len(elems)), max(1, len(elems) // chunksize))
        elems = [elems[a[0]:a[-1] + 1] for a in splits]
        using_func, extra_args = _unpack_chunksize, [func, starmap, using_chunks]
    
    # Turn elems to a list
    elems = list(elems)
    
    # Keep track of the order of elements too
    results = [None] * len(elems)
    
    # Start sending chunks to the threadpool
    elem_idx = 0
    with ThreadPoolManager(terminate=terminate, num_workers=num_workers) as tpm:
        futures = [None] * tpm.num_workers
        progress_bar = progressbar(range(len(elems)), progress=progress)

        # While we have more elements to map, or some futures are not done
        while len(elems) > 0 or any(f is not None for f in futures):
            
            # Check if there are any empty futures we can add to
            for i in range(len(futures)):
                if futures[i] is None and len(elems) > 0:
                    next_elem = elems.pop(0)
                    futures[i] = (elem_idx, tpm.pool.apply_async(using_func, [next_elem] + extra_args))
                    elem_idx += 1
            
            # Check for any completed tasks
            for i in range(len(futures)):
                if futures[i] is not None and futures[i][1].ready():
                    # Need to unpack it now, or turn into a list if we weren't using any chunks
                    res_elem_idx, result = futures[i][0], futures[i][1].get()
                    unpacked = [r for l in result for r in l] if using_chunks and using_chunksize else result if using_chunks or using_chunksize else [result]

                    # Check for errors
                    elem_results = []
                    for error_info, r in unpacked:

                        # If there is an error, take the appropriate action, checking for string vs. callable on_error
                        if error_info is not None:
                            if isinstance(on_error, str):
                                if on_error == 'raise':
                                    raise MultiprocessingError(*error_info)
                                elif on_error == 'error_info':
                                    elem_results.append(error_info)
                                elif on_error == 'return_none':
                                    elem_results.append(None)
                                elif on_error == 'ignore':
                                    pass
                                else:
                                    raise ValueError("Unknown on_error string, this shouldn't happend! '%s'" % on_error)
                            else:
                                elem_results.append(on_error(*error_info))

                        # Otherwise all good, add the result to our list
                        else:
                            elem_results.append(r)
                    
                    # Finally, insert the elem_results in its correct spot, increment the progressbar only after 
                    #   completing a task, and set the future back to None
                    results[res_elem_idx] = elem_results
                    next(progress_bar)
                    futures[i] = None
    
    # Unpack all of the elem_results and return
    return [r for l in results for r in l]


class MultiprocessingError(Exception):
    """Custom Error raised after an error in a multiprocessing call"""
    def __init__(self, err, tb):
        super().__init__("Multiprocessing call returned error:\n\n%s" % tb)
