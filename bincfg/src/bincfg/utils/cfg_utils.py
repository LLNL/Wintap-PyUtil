"""
Utilities for CFG/MemCFG objects and their datasets
"""

import numpy as np
import bincfg
from .misc_utils import get_smallest_np_dtype


def get_address(obj):
    """Gets the integer address from the given object

    Args:
        obj (Union[str, int, Addressable]): a string, int, or object with a string/int `.address` attribute (should 
            always be positive)

    Raises:
        TypeError: `obj` is an unknown type
        ValueError: given address is negative

    Returns:
        int: the integer address
    """
    if isinstance(obj, str):
        ret = int(obj, 0)
    elif isinstance(obj, (int, np.integer)):
        ret = obj
    elif hasattr(obj, 'address'):
        ret = get_address(obj.address)
    else:
        raise TypeError("Cannot get address value from object of type: '%s'" % type(obj).__name__)

    if ret < 0:
        raise ValueError("Cannot have an address that is negative: %d" % obj)
    return ret


def get_special_function_names():
    """Returns the current global special function names"""
    return SPECIAL_FUNCTION_NAMES


def check_for_normalizer(dataset, cfg_data):
    """Checks the incoming data for a normalizer to set to be `dataset`'s normalizer
    
    Assumes this dataset does not yet have a normalizer. Searches the incoming `cfg_data` for a cfg/dataset that
    has a normalizer, and sets it to be this dataset's normalizer. If this method finds no normalizer, or multiple
    unique normalizers, then an error will be raised.

    :param dataset: 
    :param cfg_data: 

    Args:
        dataset (Union[CFGDataset, MemCFGDataset]): a ``CFGDataset`` or ``MemCFGDataset`` without a normalizer
        cfg_data (Iterable[Union[CFG, MemCFG, CFGDataset, MemCFGDataset]]): an iterable of 
            ``CFG``/``MemCFG``/``CFGDataset``/``MemCFGDataset``'s

    Raises:
        ValueError: when there are multiple conflicting normalizers, or if no normalizer could be found
    """
    for cfgd in cfg_data:
        if cfgd.normalizer is not None:
            if dataset.normalizer is not None:
                dataset.normalizer = cfgd.normalizer
            elif dataset.normalizer != cfgd.normalizer:
                raise ValueError("Multiple normalizers detected.")
    
    if dataset.normalizer is None:
        raise ValueError("Could not find a normalizer in cfg_data.")


def update_atomic_tokens(file_tokens, curr_data, update_tokens):
    """Updates atomic tokens. Only meant to be passed to AtomicData.atomic_update as the function to use"""
    curr_data.update(file_tokens)
    for t in update_tokens:
        curr_data.setdefault(t, len(curr_data))
    return curr_data


def update_memcfg_tokens(cfg_data, tokens):
    """Adds all new tokens to `tokens`, and updates all tokens in `cfg_data` to their respective values in `tokens`

    Tokens in `cfg_data` will be modified, as will the `.asm_lines` attribute of each memcfg. Assumes the `cfg_data`
    has conflicting tokens to `tokens` and thus needs modification. Both `cfg_data` and `tokens` will be 
    modified in-place.
    
    Args:
        cfg_data (Union[MemCFG, MemCFGDataset]): the memcfg/memcfgdataset to have its tokens changed
        tokens (Union[Dict[str, int], AtomicData]): the dictionary of tokens to update with the new tokens in `cfg_data`.
            Can be an AtomicData object for atomic updating of tokens
    """
    # If the tokens are the same, we can just return
    if tokens is cfg_data.tokens:
        return
        
    # Add all the new tokens in cfg_data to tokens
    if isinstance(tokens, bincfg.AtomicTokenDict):
        tokens.update(cfg_data.tokens)

    else:
        for new_token in cfg_data.tokens:
            tokens.setdefault(new_token, len(tokens))
    
    # Create the mapping from old token value to new token value
    old_to_new = {v: tokens[t] for t, v in cfg_data.tokens.items()}

    # Update all of the `.asm_lines` to their new token values
    update_cfgs = [cfg_data] if isinstance(cfg_data, bincfg.MemCFG) else cfg_data.cfgs
    for cfg in update_cfgs:
        new_asm_lines = [old_to_new[l] for l in cfg.asm_lines]
        cfg.asm_lines = np.array(new_asm_lines, dtype=get_smallest_np_dtype(max(new_asm_lines)))


# The default global set of special function names
SPECIAL_FUNCTION_NAMES = {'free', 'printf', 'fputc', 'fprintf', '__gmon_start__', 'memcpy', 'exit', 'fwrite', 'abort', 
'strcpy', 'memmove', '__errno_location', 'ferror', 'fread', 'fclose', 'sprintf', '__libc_start_main', 'ftell', 'fseek', 
'fopen', 'libintl_gettext', 'getenv', 'libintl_textdomain', 'strrchr', 'fputs', 'libintl_bindtextdomain', 'strerror', 
'_IO_putc', 'setlocale', 'vfprintf', 'xmalloc', '__xstat', 'xexit', 'unlink', 'getopt_long', 'close', 'fflush', 'strcmp',
'malloc', 'bfd_scan_vma', 'xrealloc', 'bfd_get_error', 'bfd_errmsg', 'strlen', 'bfd_set_format', 'strtol', 'access', 
'xstrdup', 'mkstemps', 'bfd_malloc', 'bfd_target_list', 'xmalloc_set_program_name', 'bfd_openw', '__assert_fail', 
'bfd_close_all_done', 'bfd_set_default_target', 'mkstemp', 'memset', 'bfd_arch_list', 'bfd_openr', 'ctime', 'strchr', 
'bfd_printable_arch_mach', 'realloc', 'mkdtemp', 'bfd_close', 'bfd_init', 'bfd_check_format_matches', 'bfd_check_format', 
'strncmp', '_IO_getc', 'puts', 'strncpy', 'putchar', 'bfd_set_error_program_name', 'bfd_get_section_by_name', 
'bfd_map_over_sections', 'strtoul', 'qsort', 'bfd_get_section_contents', '__strdup', 'filename_cmp', 'open', 'write', 
'strstr', 'bfd_openr_next_archived_file', 'bfd_set_section_contents', 'bfd_get_arch', 'bfd_set_section_size', 'strcat', 
'sbrk', 'memcmp', 'bfd_get_mach', 'bfd_canonicalize_reloc', 'bfd_get_reloc_upper_bound', 'read', 'concat', 'xcalloc', 
'feof', 'stpcpy', 'bfd_set_error', 'lbasename', 'bfd_set_symtab', 'bfd_make_section_with_flags', 'strcasecmp', 
'unlink_if_ordinary', 'bfd_bread', 'bfd_seek', 'snprintf', 'calloc', 'cplus_demangle_name_to_style', 
'cplus_demangle_set_style', 'fileno', 'perror', 'bfd_reloc_type_lookup', 'xstrerror', '_exit', 'fcntl', 'fdopen', 
'__lxstat', 'chmod', 'bsearch', 'fnmatch', 'time', 'bfd_demangle', 'bfd_set_section_flags', 'cplus_demangle', 
'remove', 'dup2', 'pipe', 'kill', 'wait4', 'waitpid', 'vfork', 'execvp', 'sleep', 'execv', 'bfd_hash_traverse', 
'bfd_hash_newfunc', 'bfd_hash_table_free', 'bfd_hash_allocate', 'bfd_hash_lookup', 'bfd_scan_arch', 'bfd_hash_table_init', 
'bfd_set_start_address', 'atoi', 'rewind', 'htab_find_slot', 'htab_find', 'abort', 'abs', 'acos', 'asctime', 'asctime_r', 
'asin', 'assert', 'atan', 'atan2', 'atexit', 'atof', 'atoi', 'atol', 'bsearch', 'btowc', 'calloc', 'catclose', 'catgets', 
'catopen', 'ceil', 'clearerr', 'clock', 'cos', 'cosh', 'ctime', 'ctime64', 'ctime_r', 'ctime64_r', 'difftime', 
'difftime64', 'div', 'erf', 'erfc', 'exit', 'exp', 'fabs', 'fclose', 'fdopen', 'feof', 'ferror', 'fflush', 'fgetc', 
'fgetpos', 'fgets', 'fgetwc', 'fgetws', 'fileno', 'floor', 'fmod', 'fopen', 'fprintf', 'fputc', 'fputs', 'fputwc', 
'fputws', 'fread', 'free', 'freopen', 'frexp', 'fscanf', 'fseek', 'fsetpos', 'ftell', 'fwide', 'fwprintf', 'fwrite', 
'fwscanf', 'gamma', 'getc', 'getchar', 'getenv', 'gets', 'getwc', 'getwchar', 'gmtime', 'gmtime64', 'gmtime_r', 
'gmtime64_r', 'hypot', 'isalnum', 'isalpha', 'isascii', 'isblank', 'iscntrl', 'isdigit', 'isgraph', 'islower', 
'isprint', 'ispunct', 'isspace', 'isupper', 'iswalnum', 'iswalpha', 'iswblank', 'iswcntrl', 'iswctype', 'iswdigit', 
'iswgraph', 'iswlower', 'iswprint', 'iswpunct', 'iswspace', 'iswupper', 'iswxdigit', 'isxdigit', 'j0', 'j1', 'jn', 
'labs', 'ldexp', 'ldiv', 'localeconv', 'localtime', 'localtime64', 'localtime_r', 'localtime64_r', 'log', 'log10', 
'longjmp', 'malloc', 'mblen', 'mbrlen', 'mbrtowc', 'mbsinit', 'mbsrtowcs', 'mbstowcs', 'mbtowc', 'memchr', 'memcmp', 
'memcpy', 'memmove', 'memset', 'mktime', 'mktime64', 'modf', 'nextafter', 'nextafterl', 'nexttoward', 'nexttowardl', 
'nl_langinfo4', 'perror', 'pow', 'printf', 'putc', 'putchar', 'putenv', 'puts', 'putwc', 'putwchar', 'qsort', 
'quantexpd32', 'quantexpd64', 'quantexpd128', 'quantized32', 'quantized64', 'quantized128', 'samequantumd32', 
'samequantumd64', 'samequantumd128', 'raise', 'rand', 'rand_r', 'realloc', 'regcomp', 'regerror', 'regexec', 
'regfree', 'remove', 'rename', 'rewind', 'scanf', 'setbuf', 'setjmp', 'setlocale', 'setvbuf', 'signal', 'sin', 
'sinh', 'snprintf', 'sprintf', 'sqrt', 'srand', 'sscanf', 'strcasecmp', 'strcat', 'strchr', 'strcmp', 'strcoll', 
'strcpy', 'strcspn', 'strerror', 'strfmon', 'strftime', 'strlen', 'strncasecmp', 'strncat', 'strncmp', 'strncpy', 
'strpbrk', 'strptime', 'strrchr', 'strspn', 'strstr', 'strtod', 'strtod32', 'strtod64', 'strtod128', 'strtof', 
'strtok', 'strtok_r', 'strtol', 'strtold', 'strtoul', 'strxfrm', 'swprintf', 'swscanf', 'system', 'tan', 'tanh', 
'time', 'time64', 'tmpfile', 'tmpnam', 'toascii', 'tolower', 'toupper', 'towctrans', 'towlower', 'towupper', 
'ungetc', 'ungetwc', 'va_arg', 'va_copy', 'va_end', 'va_start', 'vfprintf', 'vfscanf', 'vfwprintf', 'vfwscanf', 
'vprintf', 'vscanf', 'vsprintf', 'vsnprintf', 'vsscanf', 'vswprintf', 'vswscanf', 'vwprintf', 'vwscanf', 'wcrtomb', 
'wcscat', 'wcschr', 'wcscmp', 'wcscoll', 'wcscpy', 'wcscspn', 'wcsftime', 'wcslen', 'wcslocaleconv', 'wcsncat', 
'wcsncmp', 'wcsncpy', 'wcspbrk', 'wcsptime', 'wcsrchr', 'wcsrtombs', 'wcsspn', 'wcsstr', 'wcstod', 'wcstod32', 
'wcstod64', 'wcstod128', 'wcstof', 'wcstok', 'wcstol', 'wcstold', 'wcstombs', 'wcstoul', 'wcsxfrm', 'wctob', 'wctomb', 
'wctrans', 'wctype', 'wcwidth', 'wmemchr', 'wmemcmp', 'wmemcpy', 'wmemmove', 'wmemset', 'wprintf', 'wscanf', 'y0', 'y1', 
'yn'}
