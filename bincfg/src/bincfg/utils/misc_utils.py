"""
Miscellaneous utility functions
"""

import re
import time
import numpy as np
from enum import Enum
from threading import Thread
from hashlib import sha256


# The actual progressbar object, once it has been determined
_IMPORT_PROGRESSBAR = None

# A logger to log with if needed
LOGGER = None


def log(severity='info', message=''):
    """Attempts to log a message if LOGGER has already been set"""
    severity = severity.lower()
    if LOGGER is None:
        return
    elif severity in ['warn', 'warning', 'warns', 'warnings']:
        LOGGER.warn(message)
    elif severity in ['debug']:
        LOGGER.debug(message)
    elif severity in ['info']:
        LOGGER.info(message)
    elif severity in ['critical', 'crit']:
        LOGGER.critical(message)
    elif severity in ['error', 'err']:
        LOGGER.error(message)
    else:
        raise ValueError("Unknown severity: %s" % repr(severity))


def set_logger(logger):
    """Sets the logger for this module"""
    global LOGGER
    LOGGER = logger


def get_smallest_np_dtype(val, signed=False):
    """Returns the smallest numpy integer dtype needed to store the given max value.

    Args:
        val (int): the largest magnitude (furthest from 0) integer value that we need to be able to store
        signed (bool, optional): if True, then use signed ints. Defaults to False.

    Raises:
        ValueError: if a bad value was passed, or if the value was too large to store in a known integer size

    Returns:
        np.dtype: the smallest integer dtype needed to store the given max value
    """
    val = abs(val) if signed else val
    if val < 0:
        raise ValueError("Val must be >0 if using unsigned values: %d" % val)
    
    for dtype in ([np.int8, np.int16, np.int32, np.int64] if signed else [np.uint8, np.uint16, np.uint32, np.uint64]):
        if val < np.iinfo(dtype).max and (not signed or -val > np.iinfo(dtype).min):
            return dtype

    raise ValueError("Could not find an appropriate size for given integer: %d" % val)


def scatter_nd_numpy(target, indices, values):
    """Sets the values at `indices` to `values` in numpy array `target`
    
    Shamelessly stolen from: https://stackoverflow.com/questions/46065873/how-to-do-scatter-and-gather-operations-in-numpy

    Args:
        target (np.ndarray): the target ndarray to modify
        indices (np.ndarray): n-d array (same ndim as target) of the indices to set values to
        values (np.ndarray): 1-d array of the values to set

    Returns:
        np.ndarray: the resultant array, modified inplace
    """
    indices = tuple(indices.reshape(-1, indices.shape[-1]).T)
    np.add.at(target, indices, values.ravel())  # type: ignore
    return target


def arg_array_split(length, sections, return_index=None, dtype=np.uint32):
    """Like np.array_split(), but returns the indices that one would split at

    This will always return `sections` sections, even if `sections` > length (in which case, any empty sections will
    come at the end). If `sections` does not perfectly divide `length`, then any extras will be front-loaded, one per
    split array as needed.

    NOTE: this code was modified from the numpy array_split() source
    
    Args:
        length (int): the length of the sequence to split
        sections (int): the number of sections to split into
        return_index (Optional[int]): if not None, then an int to determine which tuple of (start, end) indices to
            return (IE: if you were splitting an array into 10 sections, and passed return_index=3, this would return
            the tuple of (start, end) indicies for the 4th split array (since we start indexing at 0))
        dtype (np.dtype): the numpy dtype to use for the returned array
    
    Returns:
        Union[np.ndarray, Tuple[int, int]]: a numpy array of length `sections + 1` where the split array at index `i`
            would use the start/end endices `[returned_array[i]:returned_array[i+1]]`, unless return_index is not None,
            in which case a 2-tuple of the (start_idx, end_idx) will be returned
    """
    if sections <= 0:
        raise ValueError('Number of sections must be > 0. Got %d' % sections)
    if length < 0:
        raise ValueError("Length must be >= 0. Got %d" % length)
    if return_index is not None and (return_index < 0 or return_index >= sections):
        raise ValueError("return_index, if not None, must be a positive integer in the range [0, sections). Got "
            "sections=%d, return_index=%d" % (sections, return_index))

    # If sections > length, then we would return the array [0, 1, 2, 3, ..., length - 1, length, length, length, ...]
    # NOTE: this also handles the case where length == 0 since sections > 0 always
    if sections >= length:
        ret_arr = np.arange(sections + 1, dtype=dtype)
        ret_arr[length:] = length

    # Otherwise we can do the normal divmod method
    # NOTE: this also handles the case where length == sections
    else:
        num_per_section, extras = divmod(length, sections)
        section_sizes = [0] + extras * [num_per_section + 1] + (sections - extras) * [num_per_section]
        ret_arr = np.array(section_sizes, dtype=dtype).cumsum()
    
    if return_index is not None:
        return ret_arr[return_index], ret_arr[return_index + 1]
    
    return ret_arr


# Some object types
_SingletonObjects = [None, Ellipsis, NotImplemented]
_DictKeysType = type({}.keys())
_GeneratorType = type((x for x in [1]))

_MAX_STR_LEN = 1000

# Types that are all able to be checked against one another using default '==' equality check
_DUNDER_EQ_TYPES = (int, float, np.number, complex, bytes, bytearray, memoryview, str, range, type, set, frozenset, _DictKeysType)

# Keep track of the current kwargs being used in equal()
_CURR_EQUAL_KWARGS = None
_EQ_DEFAULT_STRICT_TYPES = object()
_EQ_DEFAULT_UNORDERED = object()
_EQ_DEFAULT_RAISE_ERR = object()


# Context manager to return _CURR_EQUAL_KWARGS back to expected
class _ReturnCurrEqualKwargs:
    def __init__(self, kwargs):
        self.kwargs = kwargs
    def __enter__(self):
        return self
    def __exit__(self, *args):
        global _CURR_EQUAL_KWARGS
        if self.kwargs['control_kwargs']:
            _CURR_EQUAL_KWARGS = None
        else:
            for k in self.kwargs:
                if k.startswith('prev_'):
                    _CURR_EQUAL_KWARGS[k[len('prev_'):]] = self.kwargs[k]


def eq_obj(a, b, selector=None, strict_types=_EQ_DEFAULT_STRICT_TYPES, unordered=_EQ_DEFAULT_UNORDERED, raise_err=_EQ_DEFAULT_RAISE_ERR):
    """
    Determines whether a == b, generalizing for more objects and capabilities than default __eq__() method.
    Equal() is an equivalence relation, and thus:
    
        1. equal(a, a) is always True                       (reflexivity)
        2. equal(a, b) implies equal(b, a)                  (symmetry)
        3. equal(a, b) and equal(b, c) implies equal(a, c)  (transitivity)
    
    NOTE: This method is not meant to be very fast. I will apply as many optimizations as feasibly possible that I can
    think of, but there will be various inefficient conversions of types to check equality.
    
    NOTE: kwargs passed to the initial :func:`~gstats_utils.pythonutils.equality.equal` function call will be passed to 
    all subcalls, including those done in other objects using their built-in __eq__ function. Any objects can override
    those kwargs for any later subcalls (but not those above/adjacent). 
    NOTE: The `selector` kwarg is only used once, then consumed for any later subcalls
    
    Args:
        a (Any): object to check equality
        b (Any): object to check equality
        selector (Optional[str]): if not None, then a string that determines the 'selector' to use on both objects for
            determining equality. It should start with either a letter (case-sensitive), underscore '_', dot '.' or
            bracket '['. This string will essentially be appended to each object to get some attribute to determine
            equality of instead of the objects themselves. For example, if you have two lists, but only want to check
            if their element at index '2' are equal, you could pass `selector='[2]'`. This is useful for debugging purposes
            as the error messages on unequal objects will be far more informative. Defaults to None.
            NOTE: if you pass a `selector` string that starts with an alphabetical character, it will be assumed to be
            an attribute, and this will check equality on `a.SELECTOR` and `b.SELECTOR`
        strict_types (bool): if True, then the types of both objects must exactly match. Otherwise objects which are 
            equal but of different types will be considered equal. Defaults to False.
        unordered (bool): if True, then all known sequential objects (list, tuple, numpy array, etc.) will be considered
            equal even if elements are in a different order (eg: a multiset equality). Otherwise, sequential objects are
            expected to have their subelements appear in the same order. If the passed objects are not sequential, then
            this has no effect. Defaults to False.
        raise_err (bool): if True, then an ``EqualityError`` will be raised whenever `a` and `b` are unequal, along with
            an informative stack trace as to why they were determined to be unequal. Defaults to False.
    
    Raises:
        EqualityError: if the two objects are not equal, and `raise_err=True`
        EqualityCheckingError: if there was an error raised during equality checking
    
    Returns:
        bool: True if the two objects are equal, False otherwise
    """
    # Check if we are the first call and thus should controll the kwargs
    global _CURR_EQUAL_KWARGS
    _stack_kwargs = {'control_kwargs': False}
    if _CURR_EQUAL_KWARGS is None:
        _stack_kwargs['control_kwargs'] = True
        _CURR_EQUAL_KWARGS = {'strict_types': False, 'unordered': False, 'raise_err': False}
    
    # Update the kwargs if needed, otherwise grab them from the curr kwargs
    _stack_kwargs.update({'prev_strict_types': _CURR_EQUAL_KWARGS['strict_types'], 'prev_unordered': _CURR_EQUAL_KWARGS['unordered'],
        'prev_raise_err': _CURR_EQUAL_KWARGS['raise_err']})
    if strict_types is not _EQ_DEFAULT_STRICT_TYPES:
        _CURR_EQUAL_KWARGS['strict_types'] = strict_types
    else:
        strict_types = _CURR_EQUAL_KWARGS['strict_types']
    
    if unordered is not _EQ_DEFAULT_UNORDERED:
        _CURR_EQUAL_KWARGS['unordered'] = unordered
    else:
        unordered = _CURR_EQUAL_KWARGS['unordered']

    if raise_err is not _EQ_DEFAULT_RAISE_ERR:
        _CURR_EQUAL_KWARGS['raise_err'] = raise_err
    else:
        raise_err = _CURR_EQUAL_KWARGS['raise_err']

    # Cover with a context manager to reset _CURR_EQUAL_KWARGS back to expected values
    with _ReturnCurrEqualKwargs(_stack_kwargs):
        
        # Get the right selector, raising an error if it's bad
        if selector is not None:
            if not isinstance(selector, str):
                raise TypeError("`selector` arg must be str, not %s" % repr(type(selector).__name__))
            if selector == '':
                selector = None
            elif selector[0].isalpha():
                selector = '.' + selector
            elif selector[0] not in '._[':
                raise ValueError("`selector` string must start with a '.', '_', '[', or alphabetic character: %s" % repr(selector))
        
        # Use `selector` if needed
        if selector is not None:
            try:
                _failed_obj_name = 'a'
                _check_a = eval('a' + selector)
                _failed_obj_name = 'b'
                _check_b = eval('b' + selector)
                _failed_obj_name = None

                return eq_obj(_check_a, _check_b, selector=None, strict_types=strict_types, unordered=unordered, raise_err=raise_err)
            except EqualityError:
                raise EqualityError(a, b, "Objects had different sub-objects using `selector` %s" % repr(selector))
            except Exception:
                if _failed_obj_name is None:
                    raise EqualityCheckingError("Could not determine equality between objects a and b using `selector` %s\na: %s\nb: %s" %
                        (repr(selector), _limit_str(a), _limit_str(b)))
                raise EqualityCheckingError("Could not use `selector` with value %s on object `%s`" % (repr(selector), _failed_obj_name))

        # Wrap everything in a try/catch in case there is an error, so it will be easier to spot
        try:

            # Do a quick first check for 'is' as they should always be equal, no matter what
            if a is b:
                return True
            
            # Check if there are strict types
            if strict_types and type(a) != type(b):
                return _eq_check(False, a, b, raise_err, message='Objects are of different types and `strict_types=True`.')
            
            ##################
            # Checking types #
            ##################

            # We already checked 'is', so this must be an error
            if any(a is x for x in _SingletonObjects) or isinstance(a, Enum):
                return _eq_check(False, a, b, raise_err)
            
            # Check for bool first that way int's and bool's cannot be equal
            elif isinstance(a, bool) or isinstance(b, bool):
                # Enforce that this is a bool no matter what. Bool's are NOT int's. I will die on this hill...
                if not _eq_enforce_types(bool, a, b, raise_err):
                    return False
                return _eq_check(a == b, a, b, raise_err, message=None)
            
            # Check for objects using '=='
            elif isinstance(a, _DUNDER_EQ_TYPES):
                if not _eq_enforce_types(_DUNDER_EQ_TYPES, a, b, raise_err):
                    return False
                return _eq_check(a == b, a, b, raise_err, message=None)
            
            # Check for sequences list/tuple
            elif isinstance(a, (list, tuple)):
                
                # Check that b is something that could be converted into a list/tuple nicely

                # If check_b is a numpy array, convert check_a to one and do a numpy comparison
                if isinstance(b, np.ndarray):
                    # Check if check_b is an object array, and if so, use lists, otherwise use numpy
                    if b.dtype == object:
                        return _check_with_conversion(a, None, b, list, unordered, raise_err, strict_types)
                    return _check_with_conversion(a, np.ndarray, b, None, unordered, raise_err)

                # Check for things to convert to list
                elif isinstance(b, (_GeneratorType, _DictKeysType)):
                    return _check_with_conversion(a, None, b, list, unordered, raise_err)
                
                # Otherwise, make sure check_b is a list/tuple
                elif not isinstance(b, (list, tuple)):
                    return _eq_check(False, a, b, raise_err, message="checked b type could not be converted into list/tuple")
                
                # This is where we handle the actual checking.
                # Check that they are the same length
                if len(a) != len(b):
                    return _eq_check(False, a, b, raise_err, message="Objects had different lengths: %d != %d" % (len(a), len(b)))
                
                # If we are using ordered, then we can just naively check, otherwise, we have to do some other things...
                if not unordered:
                    # Check each element in the lists
                    for i, (_checking_a, _checking_b) in enumerate(zip(a, b)):
                        try:
                            # It will have returned an error if raise_err, so just return False
                            if not eq_obj(_checking_a, _checking_b, selector=None, strict_types=strict_types, unordered=unordered, raise_err=raise_err):
                                return False
                        except EqualityError:  # If we get an equality error, then raise_err must be true
                            raise EqualityError(a, b, "Values at index %d were not equal" % i)
                        except Exception:
                            raise EqualityCheckingError("Could not determine equality between elements at index %d" % i)
                    
                    # Now we can return True
                    return True

                # Unordered list checking
                else:
                    raise NotImplementedError
            
            # Check for numpy array
            elif isinstance(a, np.ndarray):

                # Ensure the other value can be converted into an array
                if not isinstance(b, np.ndarray):
                    # If check_a is an object array, then just convert it to a list now and have that check it
                    if a.dtype == object:
                        return _check_with_conversion(a, list, b, None, unordered, raise_err, strict_types)
                    
                    # Otherwise, if it is a known convertible, convert it
                    if isinstance(b, (list, tuple, _GeneratorType)):
                        return _check_with_conversion(a, None, b, np.array, unordered, raise_err, strict_types)
                    
                    # Otherwise, assume not equal
                    return _eq_check(False, a, b, raise_err, message="Could not convert b object of type %s to numpy array" % type(b).__name__)

                # Check if we are using objects or a different dtype
                if a.dtype == object:
                    # Attempt to check using lists at this point
                    return _check_with_conversion(a, list, b, list, unordered, raise_err, strict_types)

                # Otherwise, check if we are doing unordered or ordered.
                if not unordered:
                    # we can use the builtin numpy assert equal thing
                    try:
                        np.testing.assert_equal(a, b)
                        return True
                    except AssertionError as e:
                        return _eq_check(False, a, b, raise_err, message='Numpy assert_equal found discrepancies:\n%s' % e)
                
                # Otherwise we need to do an unordered equality check. Just convert to a list at this point and check it
                else:
                    return _check_with_conversion(a, list, b, list, unordered, raise_err, strict_types)
            
            # Check for dictionaries
            elif isinstance(a, dict):
                # b must be a dictionary
                if not _eq_enforce_types(dict, a, b, raise_err, message='Dictionaries must be same type to compare'):
                    return False
                
                # Check all the keys are the same
                try:
                    if not eq_obj(a.keys(), b.keys(), selector=None, strict_types=strict_types, unordered=unordered, raise_err=raise_err):
                        return False
                except EqualityError:  # If we get an equality error, then raise_err must be true
                    a_un = set(k for k in a if k not in b)
                    b_un = set(k for k in b if k not in a)
                    raise EqualityError(a, b, message="Dictionaries had different .keys()\n`a`-unique keys: %s\n`b`-unique keys: %s" 
                                        % (_limit_str(a_un), _limit_str(b_un)))
                except Exception:
                    raise EqualityCheckingError("Could not determine equality between dictionary keys\na: %s\nb: %s" %
                        (_limit_str(a.keys()), _limit_str(b.keys())))
                
                # Check all the values are the same
                for k in a:
                    try:
                        if not eq_obj(a[k], b[k], selector=None, strict_types=strict_types, unordered=unordered, raise_err=raise_err):
                            return False
                    except EqualityError:  # If we get an equality error, then raise_err must be true
                        raise EqualityError(a, b, message="Values at key %s differ" % repr(k))
                    except Exception:
                        raise EqualityCheckingError("Could not determine equality between dictionary values at key %s" % repr(k))
                
                # Now we can return True
                return True
            
            # Otherwise, use the default equality measure
            else:
                try:
                    return _eq_check(a == b, a, b, raise_err, message='Using built-in __eq__ equality measure')
                except EqualityError:  # If we get an equality error, then raise_err must be true
                    raise EqualityError(a, b, message="Values were not equal using built-in __eq__ method")
                except Exception:
                    raise EqualityCheckingError("Could not determine equality between dictionary values using built-in __eq__ method")
        
        except EqualityError:
            raise
        except Exception:
            raise EqualityCheckingError("Could not determine equality between objects\na: %s\nb: %s" % (_limit_str(a), _limit_str(b)))


def _check_with_conversion(a, type_a, b, type_b, unordered, raise_err, strict_types=False):
    """Attempts to convert check_a into type_a and check_b into type_b (by calling the types), then check equality on those
    
    Gives better error messages when things go wrong. You can pass None to one of the types to not change type. Pass the
    type itself (instead of a function) for better nameing on error messages about what they were being converted into.
    The name is given by type_a.__name__ if type_a is a type, or 'a lambda function' if it is an annonymus function, or
    the module + function name if a function
    """
    ca_type, check_a_str = _get_check_type(type_a)
    cb_type, check_b_str = _get_check_type(type_b)

    conversion_str = ('(with a value being converted using %s and b value being converted using %s)' % (check_a_str, check_b_str))\
            if check_a_str and check_b_str else \
        ('(with a value being converted using %s)' % check_a_str) if check_a_str else \
        ('(with b value being converted using %s)' % check_b_str) if check_b_str else \
        ''

    try:
        return eq_obj(ca_type(a), cb_type(b), selector=None, strict_types=strict_types, unordered=unordered, raise_err=raise_err)
    except EqualityCheckingError:
        raise
    except Exception:
        _eq_check(False, a, b, raise_err, message="Values were not equal %s" % conversion_str)


def _get_check_type(t):
    """Returns a function to call and a string describing what is being used to convert type given the type to convert
    
    Returns a tuple of (conversion_callable, type_description_string). The string will be empty if the conversion is
    the identity, t.__name__ if t is a type, 'a lambda function' if it is an anonymous function, or the module + 
    function/class name if it is a callable.
    """
    if t is None:
        return lambda x: x, ''

    if not callable(t):
        raise EqualityCheckingError("Cannot convert object types as given `type` is not callable: %s" % repr(t))
    
    return t, ('type ' + repr(t.__name__)) if isinstance(t, type) else repr(t)


def _eq_enforce_types(types, a, b, raise_err, message=None):
    """enforces check_b is of the given types using isinstance"""
    if not isinstance(a, types) or not isinstance(b, types):
        return _eq_check(False, a, b, raise_err, 'Objects were of incompatible types. %s' % message)
    return True


def _eq_check(checked, a, b, raise_err, message=None):
    """bool equal check, determine whether or not we need to raise an error with info, or just return true/false"""
    if not checked:
        if raise_err:
            raise EqualityError(a, b, message)
        return False
    return True


class _TimeoutFuncThread(Thread):
    """
    A simple Thread class to call the passed function with passed args/kwargs
    """
    def __init__(self, func, *args, **kwargs):
        """
        :param func: the function to call
        :param args: *args to pass to function when calling
        :param kwargs: **kwargs to pass to function when calling
        """
        super().__init__()
        self._func, self._args, self._kwargs = func, args, kwargs
        self._return = None
    
    def run(self):
        """
        This should never be called. Instead, call TimeoutFuncThread.start() to start thread
        """
        self._return = self._func(*self._args, **self._kwargs)


def timeout_wrapper(timeout=3, timeout_ret_val=None):
    """
    Wraps a function to allow for timing-out after the specified time. If the function has not completed after timeout
        seconds, then the function will be terminated.
    """
    def decorator(func):
        def wraped_func(*args, **kwargs):
            thread = _TimeoutFuncThread(func, *args, **kwargs)
            thread.start()

            init_time = time.time()
            sleep_time = 1e-8
            while time.time() - init_time < timeout:
                if thread.is_alive():
                    time.sleep(sleep_time)
                    sleep_time = min(0.1, sleep_time * 1.05)
                else:
                    return thread._return
            
            # If we make it here, there is an error, return value
            return timeout_ret_val
    
        return wraped_func
    return decorator


# Fail if string conversion takes > 10 seconds
_STR_CONV_TIMEOUT_SECONDS = 10

@timeout_wrapper(timeout=_STR_CONV_TIMEOUT_SECONDS, timeout_ret_val="[ERROR: String conversion timed out. Max time: %d seconds]" % _STR_CONV_TIMEOUT_SECONDS)
def _limit_str(a, limit=_MAX_STR_LEN):
    a_str = repr(a)
    return a_str if len(a_str) < limit else (a_str[:limit] + '...')


class EqualityError(Exception):
    """Error raised whenever an :func:`~gstats_utils.pythonutils.equality.equal` check returns false and `raise_err=True`"""

    def __init__(self, a, b, message=None):
        message = "Values are not equal" if message is None else message
        super().__init__("Object a (%s) is not equal to object b (%s)\na: %s\nb: %s\nMessage: %s" % \
            (repr(type(a).__name__), repr(type(b).__name__), _limit_str(a), _limit_str(b), message))


class EqualityCheckingError(Exception):
    """Error raised whenever there is an unexpected problem attempting to check equality between two objects"""


def eq_obj_err(obj1, obj2):
    """Same as eq_obj, but always raises an error"""
    return eq_obj(obj1, obj2, raise_err=True)


def hash_obj(obj, return_int=False):
    """Hashes the given object

    Args:
        obj (Any): the object to hash
        return_int (bool, optional): by default this method returns a hex string, but setting return_int=True will 
            return an integer instead. Defaults to False.

    Returns:
        Union[str, int]: hash of the given object
    """
    string = ""
    if obj is None:
        string += '[None]'
    elif isinstance(obj, (str, bool)):
        string += '(' + type(obj).__name__ + ') ' + str(obj)
    elif isinstance(obj, (int, np.integer)):
        string += '(int) ' + str(obj)
    elif isinstance(obj, (float, np.floating)):
        string += '(float) ' + str(obj)
    elif isinstance(obj, (list, tuple)):
        string += '(' + type(obj).__name__ + ') '
        for o in obj:
            string += hash_obj(o)
    elif isinstance(obj, (set, frozenset)):
        string += '(' + type(obj).__name__ + ') ' + str(sum(hash_obj(o, return_int=True) for o in obj))
    elif isinstance(obj, dict):
        string += '(' + type(obj).__name__ + ') '
        string += str(sum(hash_obj(hash_obj(k) + ', ' + hash_obj(v), return_int=True) for k, v in obj.items()))
    elif isinstance(obj, np.ndarray):
        string += '(' + type(obj).__name__ + ') '
        if obj.dtype == object:
            for a in obj:
                string += hash_obj(a) + ' '
        else:
            string += str(obj.data.tobytes())
    elif isinstance(obj, re.Pattern):
        string += '(' + type(obj).__name__ + ') ' + hash_obj(obj.pattern)
        
    else:
        string += str(hash(obj))
    
    hasher = sha256()
    hasher.update(string.encode('utf-8'))
    return int(hasher.hexdigest(), 16) if return_int else hasher.hexdigest()


def get_module(package, raise_err=True, err_message=''):
    """Checks that the given package is installed, returning it, and raising an error if not

    Args:
        package (str): string name of the package
        raise_err (bool, optional): by default, this will raise an error if attempting to load the module and it doesn't 
            exist. If False, then None will be returned instead if it doesn't exist. Defaults to True.
        err_message (str): an error message to add on to any import errors raised

    Raises:
        ImportError: if the package cannot be found, and `raise_err=True`

    Returns:
        Union[ModuleType, None]: the package
    """
    try:
        import importlib
        return importlib.import_module(package)
    except ImportError:
        if raise_err:
            raise ImportError("Could not find `%s` package.%s" % (package, err_message))
        return None


def isinstance_with_iterables(obj, types, recursive=False, ret_list=False):
    """Checks that obj is one of the given types, allowing for iterables of these types

    Args:
        obj (Any): the obj to test type
        types (Union[type, Tuple[type, ...]]): either a type, or tuple of types that obj can be
        recursive (bool, optional): by default, this method will only allow iterables to contain objects of a type in 
            `types`. If `recursive=True`, then this will accept arbitrary-depth iterables of types in `types`. 
            Defaults to False.
        ret_list (bool, optional): if True, will return a single list of all elements (or None if the isinstance check 
            fails). Defaults to False.

    Returns:
        Union[List[Any], bool, None]: the return value
    """
    if isinstance(obj, types):
        return [obj] if ret_list else True
    
    try:
        if ret_list:
            ret = []
            for elem in obj:
                ret += [elem] if isinstance(elem, types) else isinstance_with_iterables(elem, types, recursive=True, ret_list=True) if recursive else None
            return ret
        else:
            for elem in obj:
                if not (isinstance(elem, types) or (recursive and isinstance_with_iterables(elem, types, recursive=True, ret_list=False))):
                    return False
    except:
        return None if ret_list else False


# This fixes a very dumb, stupid, idiotic, and dumb problem that caused me so much hassle for no reason: ipython overrides
#   the default exception handler in python so that it can print exceptions without them crashing the kernel every time.
#   That makes sense, however they also take any error strings and change them, converting anything inside <> into their
#   html (i think?) representation. Even this isn't so bad, but if you put anything that can't be parsed correctly inside
#   those <> tags, it will fail silently and remove the entire tag. Meaning, if you have an error occur while, oh I don't
#   know, testing/debugging your tokenizer that handles rose output. It will say the parsing failed on the string:
#       "0x00402ccc: mov    rax, qword ds:[rip + 0x000000000025230d]"
#   when it didn't. And so you spend many hours of your life trying to figure out why it's not working on that string
#   when the ~actual~ string it failed on was:
#       "0x00402ccc: mov    rax, qword ds:[rip + 0x000000000025230d<absolute=0x0000000000654fe0>]"
#   but the exception handler just ate up that extra rose information in the <> tags and didn't even have the decency
#   to mention it.
#   This overrides the ipython exception handler (if we are using ipython) and replaces all "<" with "<<aa>". "aa" is
#   not a known html tag, and so it fails silently eating only the "<aa>" section, while leaving the rest of the original
#   <> tag intact. This is the best/easiest way I could think of that would keep the normal notebook exception printing
#   (which I otherwise like), and stop it from destroying my exception messages that I worked so hard on
#   This seems to work on any weird mashing combination of '<'s and other characters I tested, so it's good enough for me
#
# GREAT, ANOTHER PROBLEM: doing this removes the full stack trace that will happen when one exception is caught but
#   another is raised (IE: the whole 'During handling of the above exception, another exception occurred' thing). I'm
#   going to have to figure out how to fix this, but for now, I'm gonna turn this off.
_OVERRIDE_EXC_HANDLER = False
if _OVERRIDE_EXC_HANDLER:
    try:
        def _custom_exc(shell, etype, evalue, tb, tb_offset=None):
            shell.showtraceback((etype, str(evalue).replace('<', "<<aa>"), tb), tb_offset=tb_offset)

        # Done this way to remove a yellow squiggly
        import __main__
        getattr(__main__, 'get_ipython')().set_custom_exc((Exception, ), _custom_exc)

    except NameError:
        pass


def _using_progressbar(iterable, progress=True):
    """Allows one to call progressbar(iterable, progress) to determine use of progressbar automatically.
    
    Checks to see if we are in a python notebook or not to determine which progressbar we should use.
    Copied from: https://stackoverflow.com/questions/15411967/how-can-i-check-if-code-is-executed-in-the-ipython-notebook
    """
    if not progress:
        return iter(iterable)

    global _IMPORT_PROGRESSBAR
    if _IMPORT_PROGRESSBAR is None:
        try:
            _tqdm_import = get_module('tqdm')
            _IMPORT_PROGRESSBAR = lambda *args, **kwargs: iter(_tqdm_import.tqdm(*args, **kwargs))
        except ImportError:
            print("Could not import tqdm!")
            _IMPORT_PROGRESSBAR = iter
    
    return _IMPORT_PROGRESSBAR(iterable)

progressbar = _using_progressbar
