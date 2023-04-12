"""
An assortment of helper/utility functions for tokenization/normalization.
"""

import re
import bincfg
from .tokenization_constants import Tokens, ROSE_REPLACE_STR, X86_REG_SIZES, MEMORY_SIZES
from ..utils import get_special_function_names


# Used to check to make sure an immediate value is all digits before inserting a '-' at the front
RE_ALL_DIGITS = re.compile(r'[0-9]+')

# Constant string names for tokens
IMMEDIATE_VALUE_STR = 'immval'
FUNCTION_CALL_STR = 'func'
RECURSIVE_FUNCTION_CALL_STR = 'self'
INTERNAL_FUNCTION_CALL_STR = 'innerfunc'
EXTERNAL_FUNCTION_CALL_STR = 'externfunc'
MULTI_FUNCTION_CALL_STR = 'multifunc'
JUMP_DESTINATION_STR = 'jmpdst'
MEMORY_EXPRESSION_STR = 'memexpr'
GENERAL_REGISTER_STR = 'reg'
FAR_JUMP_SEGMENT_STR = 'seg'
MEM_SIZE_TOKEN_STR = 'memptr'

# Token mapping and regex string to make sure memory expressions are correct
MEM_EXPR_TOKEN_MAPPING = {Tokens.OPEN_BRACKET: '[', Tokens.CLOSE_BRACKET: ']', Tokens.SPACING: '', Tokens.ROSE_INFO: '', 
    Tokens.REGISTER: 'r', Tokens.IMMEDIATE: 'i', Tokens.PLUS_SIGN: 'p', Tokens.TIMES_SIGN: 't'}
RE_MEM_EXPR_OBJ = '(?:r(?:ti)?|i)'
RE_MEM_EXPR_MATCH = re.compile(r'\[{memobj}(?:p{memobj})*\]'.format(memobj=RE_MEM_EXPR_OBJ))

# Regex's to check if registers are general or not, and to remove number information.
RE_GENERAL_REGISTER_MATCH = re.compile(r'r[0-9]+[dwb]?|[re]?[abcd]x|[abcd][lh]|[re]?[sd]il?')
RE_REMOVE_REGISTER_NUMBER = re.compile(r'\(?[0-9]+\)?')

# Handling memory size information
MEM_SIZE_RE = re.compile(r'(?:v([0-9]+))?([a-z]+)')
REPLACED_MEMORY_EXPRESSION_TOKEN = 'memory_expression'

# Default threshold for immediate values for normalization methods such as 'safe'
DEFAULT_IMMEDIATE_THRESHOLD = 5000


_CLEAN_INSTRUCITON_REPL = re.compile(r'"([^"\\]*(?:\\.[^"\\]*)*)"')
def clean_incoming_instruction(s):
    """Performs a first pass cleaning input strings. 
    
    Currently:
        1. converts to all lowercase
        2. strip()'s extra whitespace at the ends
        3. Replaces all strings (like those in rose info) with __STR__

    Args:
        s (str): the string to clean

    Returns:
        str: the clean string
    """
    return _CLEAN_INSTRUCITON_REPL.sub(ROSE_REPLACE_STR, s.lower().strip())


def imm_to_int(token):
    """Convert the given value to integer
    
    If token is an integer, returns token. Otherwise, converts a string token to an integer, then back to a string, 
        accounting for hexadecimal, decimal, octal, and binary values

    Args:
        token (Union[str, int]): the immediate token to convert to integer

    Returns:
        int: integer value of given token
    """
    return int(token, 0) if isinstance(token, str) else token


def ignore(self, *args, **kwargs):
    """Ignores information (if using for rose info, then it will also ignore negatives)"""
    pass


def clean_nop(idx, line, *args, **kwargs):
    """Cleans any line with the opcode 'nop' to only contain the opcode

    Args:
        idx (int): the index in ``line`` of the 'nop' opcode
        line (List[TokenTuple]): a list of (token_name, token) tuples. the current assembly line
        args: unused
        kwargs: unused

    Returns:
        int: integer index in line of last handled token
    """
    old_line = ' '.join([l[2] for l in line])
    line.clear()
    line.append((Tokens.OPCODE, 'nop', old_line))
    return 1


def return_immstr(*args, include_negative=False):
    """Builds a function that replaces immediate values with the IMMEDIATE_VALUE_STR. 
    
    This will return a function to be called as a part of a normalizer. This function takes no arguments and only 1 keyword 
    argument: whether or not to include a negative sign '-' in front of the immediate string when the input is negative.

    NOTE: This is meant to be a higher-order function. But, just in case the user forgets that (or is too lazy to add in
    two extra characters to call this function), if you pass multiple args then it will be assumed this is being called 
    as if it is the _repl_func() function below and will simply return the default result

    Args:
        args: args for this function. Ideally empty
        include_negative (bool, optional): if True, will include a negative sign in front of the returned immediate 
            string when the input is negative. Defaults to False.

    Returns:
        Union[Callable[..., str], str]: either a function that will handle immediate strings (if this function was 
            called correctly), or a handled immediate string
    """
    def _ret_imm(self, token, line, sentence, *args, **kwargs) -> str:
        return ('-' + IMMEDIATE_VALUE_STR) if include_negative and imm_to_int(token) < 0 else IMMEDIATE_VALUE_STR
    return _ret_imm if len(args) == 0 else _ret_imm(*args)


def threshold_immediate(*args):
    """Builds a function that replaces immediate values with `immval` iff abs(immediate) > some threshold
    
    This will return a function to be called as a part of a normalizer. This only takes one argument: the immediate 
    value threshold. If no arguments are passed, then the threshold will default to `DEFAULT_IMMEDIATE_THRESHOLD`.
    
    NOTE: This is meant to be a higher-order function. But, just in case the user forgets that (or is too lazy to add in
    two extra characters to call this function), if you pass multiple args then it will be assumed this is being called 
    as if it is the _repl_func() function below and will simply return the default result

    Args:
        args: args for this function. Ideally either empty to use the default thresholding value, or a single positive 
            integer for the immediate threshold

    Returns:
        Union[Callable[..., str], str]: either a function that will handle thresholded immediate strings (if this 
            function was called correctly), or a handled thresholded immediate string
    """
    threshold = args[0] if len(args) == 1 else DEFAULT_IMMEDIATE_THRESHOLD

    if not isinstance(threshold, int):
        raise TypeError('Threshold must be int, instead got `%s`: %s' % (type(threshold).__name__, threshold))

    def _threshold(self, token, line, sentence, *args, **kwargs):
        val = imm_to_int(token)
        return str(val) if abs(val) <= threshold else IMMEDIATE_VALUE_STR

    return _threshold if len(args) <= 1 else _threshold(*args)


def replace_general_register(self, token, *args):
    """Replaces general registers with a default string and their size, keeping special registers the same (while removing their numbers)

    Args:
        token (str): the current string token

    Returns:
        str: normalized name of register
    """
    return (GENERAL_REGISTER_STR + str(X86_REG_SIZES[token])) if RE_GENERAL_REGISTER_MATCH.fullmatch(token) is not None else RE_REMOVE_REGISTER_NUMBER.sub('', token)


def replace_jmpdst(self, idx, line, *args, **kwargs):
    """Replaces the jump destination immediate with 'jmpdst' iff the jump destination is an immediate value, not a segment address

    Args:
        idx (int): the index in ``line`` of the 'jump' opcode
        line (List[TokenTuple]): a list of (token_name, token) tuples. the current assembly line

    Returns:
        int: integer index in line of last handled token
    """
    line[idx + 1] = (line[idx + 1][0], JUMP_DESTINATION_STR if line[idx + 1][0] == Tokens.IMMEDIATE else line[idx + 1][1], line[idx + 1][2])
    return idx + 2


def memsize_value(self, token, *args):
    """Replaces memory size pointers with 'memsize' followed by the value of that memsize in bytes

    Args:
        token (str): the current string token

    Returns:
        str: normalized memory size string
    """
    vsize, mem_str = MEM_SIZE_RE.fullmatch(token).groups()
    mem_size = MEMORY_SIZES[mem_str] * (1 if vsize is None else int(vsize))
    return MEM_SIZE_TOKEN_STR + str(mem_size)


def replace_memory_expression(*args):
    """Builds a function that replaces memory expressions with the given replacement string
    
    This will return a function to be called as a part of a normalizer. This only takes one argument: the replacement string.
    If no arguments are passed, then the replacement string will default to 'memexpr'

    NOTE: This is meant to be a higher-order function. But, just in case the user forgets that (or is too lazy to add in
    two extra characters to call this function), if you pass multiple args then it will be assumed this is being called 
    as if it is the _repl_func() function below and will simply return the default result

    Args:
        args: args for this function. Ideally either empty to use default memory expression string, or a string to replace
            all memory expressions with.

    Returns:
        Union[Callable[..., None], None]: either a function that will handle memory expressions (if this function was 
            called correctly), or a handled memory expression
    """
    replacement = args[0] if len(args) == 1 else MEMORY_EXPRESSION_STR

    if not isinstance(replacement, str):
        raise TypeError('Replacement must be str, instead got `%s`: %s' % (type(replacement).__name__, replacement))

    def repl_mem(self, memory_start, token, line, *args):
        """Replace memory expressions with 'memexpr'"""
        self.valid_memory_expression(line[memory_start:], line)  # To check if it's valid

        # Using a brand new token so it's not confused with anything else. Keep track of the old value as well
        line[memory_start] = (REPLACED_MEMORY_EXPRESSION_TOKEN, replacement, ' '.join([l[2] for l in line[memory_start + 1:]]))
        del line[memory_start + 1:]  # Delete the rest of the line after memory_start index

    return repl_mem if len(args) <= 1 else repl_mem(*args)


def replace_function_call_immediate(*args):
    """Builds a function that replaces function call immediate values with the given replacement string
    
    This will return a function to be called as a part of a normalizer. This only takes one argument: the replacement string.
    If no arguments are passed, then the replacement string will default to 'func'

    NOTE: This is meant to be a higher-order function. But, just in case the user forgets that (or is too lazy to add in
    two extra characters to call this function), if you pass multiple args then it will be assumed this is being called 
    as if it is the _repl_func() function below and will simply return the default result

    Args:
        args: args for this function. Ideally either empty to use default function call string, or a string to replace
            all function callsa with.

    Returns:
        Union[Callable[..., None], None]: either a function that will handle function calls (if this function was 
            called correctly), or a handled function call
    """
    replacement = args[0] if len(args) == 1 else FUNCTION_CALL_STR

    if not isinstance(replacement, str):
        raise TypeError('Replacement must be str, instead got `%s`: %s' % (type(replacement).__name__, replacement))

    def _repl_func(self, idx, line, *args, **kwargs):
        line[idx + 1] = (Tokens.IMMEDIATE, replacement, line[idx + 1][2])
        return idx + 2

    return _repl_func if len(args) <= 1 else _repl_func(*args)


def special_function_call(self, idx, line, special_functions=None, cfg=None, block=None):
    """Handles special function calls
    
    Special external functions have their name kept. Recursive calls are replaced with 'self', other internal function 
    calls are replaced with 'internfunc', other external function calls are replaced with 'externfunc'. If a block has
    multiple function calls out, then it will be replaced with 'multifunc'.

    NOTE: This can all only happen if cfg and block information is passed. If it is not passed, then all function
    calls will be replaced with 'func'

    Args:
        idx (int): the index in ``line`` of the 'call' opcode
        line (List[TokenTuple]): a list of (token_name, token) tuples. the current assembly line
        special_functions (Set[str], optional): If passed, should be a set of string special function names. Otherwise 
            the default special functions from :func:`bincfg.utils.cfg_utils.get_special_function_names` will be used. 
            Defaults to None.
        cfg (Union[CFG, MemCFG], optional): either a ``CFG`` or ``MemCFG`` object that these lines occur in. Used for 
            determining function calls to self, internal functions, and external functions. If not passed, then these 
            will not be used. Defaults to None.
        block (Union[CFGBasicBlock, int], optional): either a ``CFGBasicBlock`` or integer block_idx in a ``MemCFG`` 
            object. Used for determining function calls to self, internal functions, and external functions. If not 
            passed, then these will not be used. Defaults to None.

    Returns:
        int: integer index in line of last handled token
    """
    # If cfg is None, then we cannot determine intern/extern/self function calls. Just return 'func' for function call
    if cfg is None:
        line[idx + 1] = (Tokens.IMMEDIATE, FUNCTION_CALL_STR, line[idx + 1][2])
        return idx + 2

    # Get the special functions if they were not passed
    if special_functions is None:
        special_functions = self.special_functions if hasattr(self, 'special_functions') else get_special_function_names()

    # Check that we even need to replace the value. We should only replace immediate values, so check to make sure
    #   the old_token starts with a digit
    if line[idx + 1][2][0] not in "0123456789":
        return idx + 2

    # Find the function call info in cfg
    if isinstance(cfg, bincfg.MemCFG):
        # Find the outgoing edges of this block. Normally the first edge is the function call edge, however we still need
        #   to check because there can be times where it only contains the normal return edge as the function call
        #   address couldn't be resolved to a known basic block
        func_call_block_inds, edge_types = cfg.get_block_edges_out(block, ret_edge_types=True)

        # Make sure the first value is in fact a function call
        if len(edge_types) > 0 and edge_types[0] == bincfg.FUNCTION_CALL_EDGE_CONN_VALUE:

            # Check for a self call by comparing the two block's function indices
            func_call_block_idx = func_call_block_inds[0]
            self_call = cfg.block_func_idx[block] == cfg.block_func_idx[func_call_block_idx]
            extern_func_name = cfg.get_block_function_name(func_call_block_idx) if cfg.is_block_extern_function(func_call_block_idx) else None

            # Check for a multi-function call
            multi_call = len(edge_types[edge_types == bincfg.FUNCTION_CALL_EDGE_CONN_VALUE]) > 1
        
        # Otherwise, we have no clue where the function call goes to, treat it as just some innerfunc
        else:
            self_call = False
            extern_func_name = None
            multi_call = False

    # Otherwise this is a plain CFG. Get the call address from the next immediate value, and look up its info in the CFG
    elif isinstance(cfg, bincfg.CFG):
        fc_out = block.get_sorted_edges(edge_types='function', direction='out')[0]

        if len(fc_out) > 1:
            multi_call = True
        else:
            multi_call = False

            # Need to check that the disassembler was able to figure out the call location
            if len(fc_out) == 0:
                self_call = False
                extern_func_name = None
            else:
                func = cfg.get_block(fc_out[0].to_block).parent_function
                self_call = func.address == block.parent_function.address
                extern_func_name = func.name if func.is_extern_function else None
    
    else:
        raise TypeError("Unknown cfg type for special_function_call normalization: %s" % repr(type(cfg)))

    # Determine what function call strings to return
    # A multi-function call
    if multi_call:
        line[idx + 1] = (line[idx + 1][0], MULTI_FUNCTION_CALL_STR, line[idx + 1][2])

    # A recursive function call
    elif self_call:
        line[idx + 1] = (line[idx + 1][0], RECURSIVE_FUNCTION_CALL_STR, line[idx + 1][2])

    # An external function call
    elif extern_func_name is not None:
        extern_func_name = extern_func_name.split('@')[0]
        if extern_func_name in special_functions:
            line[idx + 1] = (line[idx + 1][0], '{' + extern_func_name + '}', line[idx + 1][2])
        else:
            line[idx + 1] = (line[idx + 1][0], EXTERNAL_FUNCTION_CALL_STR, line[idx + 1][2])

    # An internal function call
    else:
        line[idx + 1] = (line[idx + 1][0], INTERNAL_FUNCTION_CALL_STR, line[idx + 1][2])

    return idx + 2


def eq_special_funcs(s1, s2):
    """Returns True if the given two sets of special function names are equal, false otherwise"""
    return len(s1.intersection(s2)) == len(s1) == len(s2)
