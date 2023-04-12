"""
Classes for normalizing assembly instructions.
"""

import re
import hashlib
from types import MethodType
from .norm_utils import imm_to_int, clean_nop, FAR_JUMP_SEGMENT_STR, MEM_EXPR_TOKEN_MAPPING, RE_MEM_EXPR_MATCH, \
    IMMEDIATE_VALUE_STR, RE_ALL_DIGITS
from ..utils import get_module, eq_obj
from .tokenization_constants import TokenMismatchError, Tokens, INSTRUCTION_START_TOKEN, TokenizationLevel
from .base_tokenizer import DEFAULT_TOKENIZER


class MetaNorm(type):
    """A metaclass for BaseNormalizer. 

    The Problem:
        If you change instance functions within the __init__ method (EG: see the SAFE _handle_immediate() function
        being changed in __init__), then 'self' will not automatically be passed to those functions.

        NOTE: this is specifically useful when the effect of a normalization method depends on parameters sent to
        the instance, not inherent to the class

        NOTE: this is not the case for any functions that are set during class initialization (EG: outside of the
        __init__() block)

        So, any functions changed within __init__ methods must be altered to also pass 'self'. I ~could~ force the
        users to have to call a '__post_init__()' function or something, but can we count on them (IE: myself) to
        always do that?...

    The Solution:
        This metaclass inserts extra code before and after any normalizer's __init__ method is called. That code keeps
        track of all instance functions before intitialization, and checks to see if any of them change after
        initialization. This means someone re-set a function within __init__ (IE: self._handle_immediate = ...).
        When this happens, 'self' will not automatically be passed when that function is called. These functions
        are then wrapped to also automatically pass 'self'.

        NOTE: to determine if a function changes, we just check equality between previous and new functions using
        getattr(self, func_name). I don't know why basic '==' works but 'is' and checking id's do not, but I'm not 
        going to question it...

        NOTE: We also have to keep track of the instance functions as an instance variable in case a parent class needs
        their function updated, or if a child class also changes a parent class's function in init

    NOTE: this will mean you cannot call all of that class's methods and expect them to always be the same as calling
    instance methods if you change functions in __init__
    """
    def __new__(cls, name, bases, dct):
        ret_cls = super().__new__(cls, name, bases, dct)  # Create a new class object (not instance)

        old_init = ret_cls.__init__  # Save this class's __init__ function to call later
        def insert_post(self, *args, **kwargs):
            """Create the new __init__ function, inserting code before and after the old __init__"""
            # Keep track of all of this instance's functions. Need to do this as an instance variable in case a parent
            #   class changed things in init so it's not wrapped twice in the child class. Also keep track of which
            #   stack frame needs to remove the __instance_funcs__ attribute
            remove_instance_funcs = False
            if not hasattr(self, '__instance_funcs__') or self.__instance_funcs__ is None:
                self.__instance_funcs__ = {k: getattr(self, k) for k in dir(self) if not k.startswith("__") and callable(getattr(self, k))}
                remove_instance_funcs = True

            # Call the old __init__ function
            old_init(self, *args, **kwargs)

            # Check if any of the functions before are no longer equal. If so, assume we need to change these functions
            #   to pass self. I don't know why basic '==' works but 'is' and checking id's do not, but I'm not going to
            #   question it...
            new_instance_funcs = {k: getattr(self, k) for k in dir(self) if k in self.__instance_funcs__ and self.__instance_funcs__[k] != getattr(self, k)}
            for k, v in new_instance_funcs.items():
                # Check to make sure v is not already a bound method of self. This can happen if the user sets a method
                #   of self to another previously bound method of self while in __init__
                if isinstance(v, MethodType) and getattr(self, v.__name__) == v:
                    continue

                setattr(self, k, MethodType(v, self))
                self.__instance_funcs__[k] = getattr(self, k)  # Update the instance funcs with the new function
            
            if remove_instance_funcs:
                del self.__instance_funcs__
        
        ret_cls.__init__ = insert_post  # Set this class's __init__ function to be the new one
        return ret_cls


class BaseNormalizer(metaclass=MetaNorm):
    """A base class for a normalization method. 
    
    Performs an 'unnormalized' normalization, removing what is likely extraneous information, and providing a base class
    for other normalization methods to inherit from.
    
    Parameters
    ----------
    tokenizer: `Optional[Tokenizer]`
        the tokenizer to use, or None to use the default BaseTokenizer
    token_sep: `Optional[str]`
        the string to use to separate each token in returned instruction lines. Only used if tokenization_level is 
        'instruction'. If None, then a default value will be used (' ' for unnormalized using BaseNormalizer(), '_' 
        for everything else)
    tokenization_level: `Optional[Union[TokenizationLevel, str]]`
        the tokenization level to use for return values. Can be a string, or a ``TokenizationLevel`` type. Strings can be:

            - 'op': tokenized at the opcode/operand level. Will insert a 'INSTRUCTION_START' token at the beginning of
              each instruction line
            - 'inst'/'instruction': tokenized at the instruction level. All tokens in each instruction line are joined
              together using token_sep to construct the final token
            - 'auto': pick the default value for this normalization technique
    anonymize_tokens: `bool`
        if True, then tokens will be annonymized by taking their 4-byte shake_128 hash. Why does this exist? Bureaucracy.
    """

    DEFAULT_TOKENIZATION_LEVEL = TokenizationLevel.INSTRUCTION
    """The default tokenization level used for this normalizer"""

    __requires_dill__ = True  # These normalizers require dill to be pickled

    tokenizer = None
    """The tokenizer used for this normalizer"""

    token_sep = None
    """The separator string used for this normalizer
    
    Will default to ' ' for ``BaseNormalizer``, and '_' for all other normalizers.
    """

    tokenization_level: TokenizationLevel
    """The tokenization level to use for this normalizer"""

    def __init__(self, tokenizer=None, token_sep=None, tokenization_level=TokenizationLevel.AUTO, anonymize_tokens=False):
        self.tokenizer = DEFAULT_TOKENIZER if tokenizer is None else tokenizer
        self.token_sep = token_sep if token_sep is not None else ' ' if type(self) == BaseNormalizer else '_'
        self.tokenization_level = self._parse_tokenization_level(tokenization_level)
        self.anonymize_tokens = anonymize_tokens

        # Need to make sure function calls will not have future overides ignored, so we have to make a lambda function here
        self.opcode_handlers = []
        self.register_opcode_handler('nop', clean_nop)
        self.register_opcode_handler('call', self.handle_function_call)
        self.register_opcode_handler(r'j.*', self.handle_jump)
    
    def register_opcode_handler(self, op_regex, func_or_str_name):
        """Registers an opcode handler for this normalizer

        Adds the given `op_regex` as an opcode to handle during self._handle_instruction() along with the given function
        to call with token/cfg arguments. `op_regex` can be either a compiled regex expression, or a string which
        will be compiled into a regex expression. `func_or_str_name` can either be a callable, or a string. If it's
        a string, then that attribute will be looked up on this normalizer dynamically to find the function to use.

        Notes for registering opcode handlers:

            1. passing instance method functions converts them to strings automatically
            2. passing lambda's or inner functions (not at global scope) would not be able to be pickled
            3. opcodes will be matched in order starting with 'nop', 'call', and 'j.*', then all in order of those 
               passed to `register_opcode_handler()`

        Args:
            op_regex (Union[str, Pattern]): a string or compiled regex
            func_or_str_name (Union[Callable, str]): the function to call with token/cfg arguments when an opcode 
                matches op_regex, or a string name of a callable attribute of this normalizer to be looked up dynamically

        Raises:
            TypeError: Bad `func_or_str_name` type
        """
        op_regex = re.compile(op_regex) if isinstance(op_regex, str) else op_regex

        if not isinstance(func_or_str_name, str):
            # Check it is callable
            if not callable(func_or_str_name):
                raise TypeError("fun_or_str_name must be str or callable, not '%s'" % type(func_or_str_name))

            # Check if the passed function is an instance method of this normalization method class specifically
            # Have to check if func_or_str_name has a __name__ attribute first since they could sometimes be _LOF classes
            if hasattr(func_or_str_name, '__name__') and hasattr(self.__class__, func_or_str_name.__name__):
                func_or_str_name = func_or_str_name.__name__

        self.opcode_handlers.append((op_regex, func_or_str_name))
    
    def _parse_tokenization_level(self, tokenization_level):
        """Returns the TokenizationLevel enum based on the given tokenization_level.

        Args:
            tokenization_level (Union[TokenizationLevel, str]): either a string tokenization level, or a class from the 
                TokenizationLevels enum

        Raises:
            ValueError: Bad string argument
            TypeError: Bad `tokenization_level` type

        Returns:
            TokenizationLevel: a class from the ``TokenizationLevels`` enum
        """
        if isinstance(tokenization_level, str):
            tl = tokenization_level.lower().replace('-', '_')
            for l in TokenizationLevel:
                if tl in l.value:
                    ret = l
                    break
            else:
                raise ValueError("Unknown tokenization_level string: '%s'" % tokenization_level)
        elif isinstance(tokenization_level, TokenizationLevel):
            ret = tokenization_level
        else:
            raise TypeError("Unknown tokenization_level type: '%s'" % type(tokenization_level))
        
        # Check for auto
        if ret is TokenizationLevel.AUTO:
            return self.__class__.DEFAULT_TOKENIZATION_LEVEL
        
        return ret
    
    def tokenize(self, *strings, enforce_asm_rules=None, newline_tup=DEFAULT_TOKENIZER.DEFAULT_NEWLINE_TUPLE,
                 match_instruction_address=True):
        """Tokenizes the given strings using this normalizer's tokenizer

        Args:
            strings (str): arbitrary length list of strings to tokenize
            enforce_asm_rules (Optional[bool]): if True, then extra processing and checks will be done to make sure the 
                tokenized assembly language matches the rules of assembly. See self.check_assembly_rules() for more info. 
                If False, these checks aren't done and bad assembly could make its way through without error, but should 
                be noticeably faster. If None, will use the default value. The default value starts as False at the
                beginning of program execution but can be modified using 
                :func:`~bincfg.normalization.base_tokenizer._set_default_enforce_asm_rules`
            newline_tup (Tuple[str, str], optional): the tuple to insert inbetween each passed string, or None to not 
                insert anything. Defaults to DEFAULT_TOKENIZER.DEFAULT_NEWLINE_TUPLE.
            match_instruction_address (bool, optional): if True, will assume there will be an instruction address at the 
                start of the string. This only has an effect on ghidra-like instruction addresses where that address 
                could be interpreted as either an immediate, or an instruction address. If True, then any immediates 
                found at the start of a line will be assumed to be instruction addresses instead of immediates. If False,
                then instruction addresses can still be matched, but they must end with a colon ':', otherwise they will 
                be considered immediates. Defaults to True.

        Raises:
            TokenMismatchError: on a bad branch prediction string

        Yields:
            Tuple[str, str]: (token_name, token) tuples
        """
        return self.tokenizer(*strings, enforce_asm_rules=enforce_asm_rules, newline_tup=newline_tup, 
                              match_instruction_address=match_instruction_address)

    def normalize(self, *strings, cfg=None, block=None, enforce_asm_rules=None,
                  newline_tup=DEFAULT_TOKENIZER.DEFAULT_NEWLINE_TUPLE, match_instruction_address=True):
        """Normalizes the given iterable of strings.

        Args:
            strings (str): arbitrary number of strings to normalize
            cfg (Union[CFG, MemCFG], optional): either a ``CFG`` or ``MemCFG`` object that these lines occur 
                in. Used for determining function calls to self, internal functions, and external functions. If not 
                passed, then these will not be used. Defaults to None.
            block (Union[CFGBasicBlock, int], optional): either a ``CFGBasicBlock`` or integer block_idx in a ``MemCFG``
                object. Used for determining function calls to self, internal functions, and external functions. If not 
                passed, then these will not be used. Defaults to None.
            enforce_asm_rules (Optional[bool]): if True, then extra processing and checks will be done to make sure the 
                tokenized assembly language matches the rules of assembly. See self.check_assembly_rules() for more info. 
                If False, these checks aren't done and bad assembly could make its way through without error, but should 
                be noticeably faster. If None, will use the default value. The default value starts as False at the
                beginning of program execution but can be modified using 
                :func:`~bincfg.normalization.base_tokenizer._set_default_enforce_asm_rules`
            newline_tup (Tuple[str, str], optional): the tuple to insert inbetween each passed string, or None to not 
                insert anything. Defaults to DEFAULT_TOKENIZER.DEFAULT_NEWLINE_TUPLE.
            match_instruction_address (bool, optional): if True, will assume there will be an instruction address at the 
                start of the string. This only has an effect on ghidra-like instruction addresses where that address 
                could be interpreted as either an immediate, or an instruction address. If True, then any immediates 
                found at the start of a line will be assumed to be instruction addresses instead of immediates. If False,
                then instruction addresses can still be matched, but they must end with a colon ':', otherwise they will 
                be considered immediates. Defaults to True.

        Raises:
            MalformedMemoryExpressionError: when memory expressions are malformed
            MisplacedInstructionPrefixError: when instruction prefixes do not occur in the correct place
            TokenMismatchError: on a bad branch prediction string

        Returns:
            List[str]: a list of normalized string instruction lines
        """
        # Check if the first string is an instruction start. If so, we are normalizing an already-normalized string that
        #   was normalized with tokenization_level='op'. Combine all the strings together, joining on spaces, but splitting
        #   the string array on all INSTRUCTION_START_TOKEN's
        if len(strings) > 0 and strings[0] == INSTRUCTION_START_TOKEN:
            newline_tup = None

        sentence = []  # A list of either full instruction line tokens, or opcode/operand-level tokens
        line = []  # A list of tuples of names and tokens
        memory_start = -1  # index at which a memory information starts
        for name, old_token in self.tokenize(*strings, enforce_asm_rules=enforce_asm_rules, newline_tup=newline_tup, 
                                             match_instruction_address=match_instruction_address):
            new_token = old_token  # By default, assume we are just using the old token

            # Check for a token that is ignored, as well as mismatched tokens
            if name in [Tokens.INSTRUCTION_ADDRESS, Tokens.SPACING, Tokens.PTR, Tokens.SEGMENT, Tokens.BRANCH_PREDICTION]:
                new_token = self.handle_ignored(name, old_token, line, sentence)
            elif name in [Tokens.MISMATCH]:
                new_token = self.handle_mismatch(name, old_token, line, sentence)

            # Check for a newline, making sure memory expressions were closed previously
            # Instruction_start's should count as newlines
            elif name in [Tokens.NEWLINE, Tokens.INSTRUCTION_START]:
                if memory_start >= 0:
                    raise MalformedMemoryExpressionError(name, old_token, line)
                new_token = self.handle_newline(old_token, line, sentence, cfg=cfg, block=block)
                line.clear()
            
            # Check for rose information. Whether we are in memory expression or not doesn't matter
            elif name == Tokens.ROSE_INFO:
                new_token = self.handle_rose_info(old_token, line, sentence)
            
            # Check for a start of memory expression. Raise an error if one was previously started but not closed
            elif name == Tokens.OPEN_BRACKET:
                if memory_start >= 0:
                    raise MalformedMemoryExpressionError(name, old_token, line)
                memory_start = len(line)
            
            # Check for a close of memory expression. Raise an error if one was not previously started
            elif name == Tokens.CLOSE_BRACKET:
                if memory_start < 0:
                    raise MalformedMemoryExpressionError(name, old_token, line)

                # Insert the token first and clear token so it isn't inserted again
                line.append((name, old_token, old_token))
                new_token = self.handle_memory_expression(memory_start, old_token, line, sentence)
                memory_start = -1
            
            # Check for an instruction prefix, and make sure it comes before other things
            elif name == Tokens.INSTRUCTION_PREFIX:
                if len(line) != 0:
                    raise MisplacedInstructionPrefixError(old_token, line, sentence)
            
            # Check for a segment address. Convert the sub-tokens as needed
            # Only update the segment register to FAR_JUMP_SEGMENT_STR if we are not doing an 'unnormalized' normalization
            elif name == Tokens.SEGMENT_ADDRESS:
                left, right = old_token.split(':')
                left = self.handle_immediate(left, [], []) if left[0] in "0123456789" else left if type(self) == BaseNormalizer else FAR_JUMP_SEGMENT_STR
                right = self.handle_immediate(right, [], []) if right[0] in "0123456789" else self.handle_register(right, [], [])
                line.append((name, left + ':' + right, old_token))
                new_token = None
            
            # Check for an immediate value
            elif name == Tokens.IMMEDIATE:
                new_token = self.handle_immediate(old_token, line, sentence)

            # Check for a register
            elif name == Tokens.REGISTER:
                new_token = self.handle_register(old_token, line, sentence)
            
            # Check for a memory size
            elif name == Tokens.MEMORY_SIZE:
                new_token = self.handle_memory_size(old_token, line, sentence)
            
            # Check for opcode
            elif name == Tokens.OPCODE:
                new_token = self.handle_opcode(old_token, line, sentence)
            
            # Check for tokens that we just blindly add, otherwise this is an unknown token
            elif name not in [Tokens.PLUS_SIGN, Tokens.TIMES_SIGN]:
                new_token = self.handle_unknown_token(name, old_token, line, sentence)
            
            # Finally, add this (name, new_token, old_token) triplet to our list if new_token is not None
            if new_token is not None:
                line.append((name, new_token, old_token))
        
        # If there is anything else left in line, call _handle_newline() again
        self.handle_newline(None, line, sentence, cfg=cfg, block=block)

        # If we are anonymizing the tokens, do that now
        if self.anonymize_tokens:
            for i, t in enumerate(sentence):
                if isinstance(t, str):
                    sentence[i] = self.hash_token(t)
                else:
                    for j, st in enumerate(t):
                        t[j] = self.hash_token(st)
        
        return sentence
    
    def handle_opcode(self, token, line, sentence):
        """Handles an opcode. Defaults to returning the raw opcode

        NOTE: This should only be used to determine how all opcode strings are handled. For how to handle specific opcodes
        to give them different behaviors, see :func:`~bincfg.normalization.base_normalizer.BaseNormalizer.register_opcode_handler`

        Args:
            token (str): the current string token
            line (List[TokenTuple]): a list of (token_name, token) tuples. the current assembly line
            sentence (List[str]): the current sentence, a list of strings. These will be either full assembly instructions
                if `tokenization_level='instuction'`, or single tokens with a separator between each assembly line if
                `tokenization_level='op'`

        Returns:
            str: the original token
        """
        return token
    
    def handle_memory_size(self, token, line, sentence):
        """Handles a memory size. Defaults to returning the raw memory size

        Args:
            token (str): the current string token
            line (List[TokenTuple]): a list of (token_name, token) tuples. the current assembly line
            sentence (List[str]): the current sentence, a list of strings. These will be either full assembly instructions
                if `tokenization_level='instuction'`, or single tokens with a separator between each assembly line if
                `tokenization_level='op'`

        Returns:
            str: the original token
        """
        return token
    
    def handle_register(self, token, line, sentence):
        """Handles a register. Defaults to returning the raw register name

        Args:
            token (str): the current string token
            line (List[TokenTuple]): a list of (token_name, token) tuples. the current assembly line
            sentence (List[str]): the current sentence, a list of strings. These will be either full assembly instructions
                if `tokenization_level='instuction'`, or single tokens with a separator between each assembly line if
                `tokenization_level='op'`

        Returns:
            str: the original token
        """
        return token

    def handle_immediate(self, token, line, sentence):
        """Handles an immediate value. Defaults to converting into decimal

        Args:
            token (str): the current string token
            line (List[TokenTuple]): a list of (token_name, token) tuples. the current assembly line
            sentence (List[str]): the current sentence, a list of strings. These will be either full assembly instructions
                if `tokenization_level='instuction'`, or single tokens with a separator between each assembly line if
                `tokenization_level='op'`

        Returns:
            str: the token in decimal
        """
        return str(imm_to_int(token))

    def handle_memory_expression(self, memory_start, token, line, sentence):
        """Handles memory expressions. Defaults to simply checking if the memory expression is valid
        
        Since this is an 'unnormalized' normalization method, do nothing but make sure it is a valid memory expression 
            and clear out any extraneous information.

        Args:
            memory_start (int): integer index in line where the full memory expression starts. The full memory expression
            would then be the list of tokens ``line[memory_start:]``
            token (str): the current string token
            line (List[TokenTuple]): a list of (token_name, token) tuples. the current assembly line
            sentence (List[str]): the current sentence, a list of strings. These will be either full assembly instructions
                if `tokenization_level='instuction'`, or single tokens with a separator between each assembly line if
                `tokenization_level='op'`
        """
        self.valid_memory_expression(line[memory_start:], line)
    
    def valid_memory_expression(self, memory_expression, line):
        """Raises an error if the input is an invalid memory expression

        Args:
            memory_expression (List[TokenTuple]): a list of (name, new_token, old_token) triplets
            line (List[TokenTuple]): a list of (token_name, token) tuples. the current assembly line

        Raises:
            MalformedMemoryExpressionError: when the memory expression is malformed
        """
        string = ""
        for name, token, _ in memory_expression:
            if name not in MEM_EXPR_TOKEN_MAPPING:
                raise MalformedMemoryExpressionError("Memory expression contains unexpected token type '%s'" % name, token, line)
            string += MEM_EXPR_TOKEN_MAPPING[name]
        
        if RE_MEM_EXPR_MATCH.fullmatch(string) is None:
            raise MalformedMemoryExpressionError(None, string, line)
    
    def handle_rose_info(self, token, line, sentence):
        """Checks to see if the rose info is telling us an immediate value is negative, otherwise ignores it

        Args:
            token (str): the current string token
            line (List[TokenTuple]): a list of (token_name, token) tuples. the current assembly line
            sentence (List[str]): the current sentence, a list of strings. These will be either full assembly instructions
                if `tokenization_level='instuction'`, or single tokens with a separator between each assembly line if
                `tokenization_level='op'`
        """
        # Check the rose info suggests a minus sign for an immediate value. If so, set the previous immediate to be
        #   self._handle_immediate(rose_value) where rose_value is the value in the brackets <>
        # Also have to split the rose info on ',' since there is sometimes extra info with ints
        # Also have to check that the penultimate element in line is not a 'call' or 'jump' instruction
        if token[1] == '-' and len(line) > 0 and (RE_ALL_DIGITS.fullmatch(line[-1][1]) is not None or line[-1][1] in [IMMEDIATE_VALUE_STR, '-' + IMMEDIATE_VALUE_STR])\
            and not (len(line) >= 2 and line[-2][0] == Tokens.OPCODE and (line[-2][1] == 'call' or line[-2][1][0] == 'j')):
            line[-1] = (line[-1][0], self.handle_immediate(imm_to_int(token[1:-1].split(',')[0]), line, sentence), line[-1][2])
    
    def handle_newline(self, token, line, sentence, cfg=None, block=None):
        """Handles a newline token depending on what this normalizer's tokenization_level is

        Args:
            token (str): the current string token
            line (List[TokenTuple]): a list of (token_name, token) tuples. the current assembly line
            sentence (str): the current sentence, a list of strings. These will be either full assembly instructions
                if `tokenization_level='instuction'`, or single tokens with a separator between each assembly line if
                `tokenization_level='op'`
            cfg (Optional[Union[CFG, MemCFG]], optional): either a ``CFG`` or ``MemCFG`` object that these lines occur in. 
                Used for determining function calls to self, internal functions, and external functions. If not passed, 
                then these will not be used. Defaults to None.
            block (Optional[Union[CFGBasicBlock, int]], optional): either a ``CFGBasicBlock`` or integer block_idx in a 
                ``MemCFG`` object. Used for determining function calls to self, internal functions, and external functions. 
                If not passed, then these will not be used. Defaults to None.

        Raises:
            NotImplementedError: If a TokenizationLevel was added but not implemented here
        """
        if len(line) == 0:
            return

        handled = self.handle_instruction(line, cfg=cfg, block=block)
        strings = [t for name, t, _ in (line if handled is None else handled)]

        if self.tokenization_level is TokenizationLevel.OPCODE:
            sentence.append(INSTRUCTION_START_TOKEN)
            sentence += strings
        elif self.tokenization_level is TokenizationLevel.INSTRUCTION:
            sentence.append(self.token_sep.join(strings))
        else:
            raise NotImplementedError("Unknown tokenization: %s" % self.tokenization_level)
        
        line.clear()
    
    def handle_ignored(self, name, token, line, sentence):
        """Handles ignored tokens. Defaults to doing nothing

        EG: spacing, commas, instruction memory address, etc.

        Args:
            name (str): the name of this token
            token (str): the current string token
            line (List[TokenTuple]): a list of (token_name, token) tuples. the current assembly line
            sentence (List[str]): the current sentence, a list of strings. These will be either full assembly instructions
                if `tokenization_level='instuction'`, or single tokens with a separator between each assembly line if
                `tokenization_level='op'`
        """
        pass

    def handle_mismatch(self, name, token, line, sentence):
        """What to do when the normalizaion method finds a token mismatch (in case they were ignored in the tokenizer)

        Defaults to raising a TokenMismatchError()

        Args:
            name (str): the name of this token
            token (str): the current string token
            line (List[TokenTuple]): a list of (token_name, token) tuples. the current assembly line
            sentence (List[str]): the current sentence, a list of strings. These will be either full assembly instructions
                if `tokenization_level='instuction'`, or single tokens with a separator between each assembly line if
                `tokenization_level='op'`

        Raises:
            TokenMismatchError: by default
        """
        raise TokenMismatchError("Mismatched token %s found during normalization!" % repr(token))
    
    def handle_unknown_token(self, name, token, line, sentence):
        """Handles an unknown token. Currently just raises a TypeError
        
        Can be overridden in subclasses to add new token types
        
        Args:
            name (str): the name of this token
            token (str): the current string token
            line (List[TokenTuple]): a list of (token_name, token) tuples. the current assembly line
            sentence (List[str]): the current sentence, a list of strings. These will be either full assembly instructions
                if `tokenization_level='instuction'`, or single tokens with a separator between each assembly line if
                `tokenization_level='op'`

        Raises:
            TypeError: by default
        """
        raise TypeError("Unknown token name '%s'" % name)
    
    def handle_instruction(self, line, cfg=None, block=None):
        """Handles an entire instruction once reaching a new line. 
        
        Allows for extra manipulations like checking call/jump destinations, etc. If nothing is returned, then it is 
        assumed line itself has been edited.

        Args:
            line (List[TokenTuple]): a list of (token_name, token) tuples. the current assembly line
            cfg (Optional[Union[CFG, MemCFG]], optional): either a ``CFG`` or ``MemCFG`` object that these lines occur in. 
                Used for determining function calls to self, internal functions, and external functions. If not passed, 
                then these will not be used. Defaults to None.
            block (Optional[Union[CFGBasicBlock, int]], optional): either a ``CFGBasicBlock`` or integer block_idx in a 
                ``MemCFG`` object. Used for determining function calls to self, internal functions, and external functions. 
                If not passed, then these will not be used. Defaults to None.
        """
        idx = 0
        while idx < len(line):
            
            # Check for any handled opcodes
            if line[idx][0] == Tokens.OPCODE:
                opcode = line[idx][1]

                for regex, func in self.opcode_handlers:
                    if regex.fullmatch(opcode) is not None:
                        # Check for string name to lookup on self
                        if isinstance(func, str):
                            func = getattr(self, func)

                        new_idx = func(idx, line, cfg=cfg, block=block)
                        idx = (new_idx - 1) if new_idx is not None else idx
                        break
            
            idx += 1
    
    def handle_function_call(self, idx, line, cfg=None, block=None):
        """Handles function calls. Defaults to returning raw call values

        This is an opcode handler. It should modify the list of token tuples ``line`` in-place, then return the integer
        index in ``line`` of the last token that has been 'handled' by this function call.

        Args:
            idx (int): the index in ``line`` of the 'call' opcode
            line (List[TokenTuple]): a list of (token_name, token) tuples. the current assembly line
            cfg (Optional[Union[CFG, MemCFG]], optional): either a ``CFG`` or ``MemCFG`` object that these lines occur in. 
                Used for determining function calls to self, internal functions, and external functions. If not passed, 
                then these will not be used. Defaults to None.
            block (Optional[Union[CFGBasicBlock, int]], optional): either a ``CFGBasicBlock`` or integer block_idx in a 
                ``MemCFG`` object. Used for determining function calls to self, internal functions, and external functions. 
                If not passed, then these will not be used. Defaults to None.

        Returns:
            int: index in line of last handled token
        """
        return idx + 1
    
    def handle_jump(self, idx, line, cfg=None, block=None):
        """Handles jumps. Defaults to returning raw jump values

        This is an opcode handler. It should modify the list of token tuples ``line`` in-place, then return the integer
        index in ``line`` of the last token that has been 'handled' by this function call.

        Args:
            idx (int): the index in ``line`` of the 'jump' opcode
            line (List[TokenTuple]): a list of (token_name, token) tuples. the current assembly line
            cfg (Optional[Union[CFG, MemCFG]], optional): either a ``CFG`` or ``MemCFG`` object that these lines occur in. 
                Used for determining function calls to self, internal functions, and external functions. If not passed, 
                then these will not be used. Defaults to None.
            block (Optional[Union[CFGBasicBlock, int]], optional): either a ``CFGBasicBlock`` or integer block_idx in a 
                ``MemCFG`` object. Used for determining function calls to self, internal functions, and external functions. 
                If not passed, then these will not be used. Defaults to None.

        Returns:
            int: index in line of last handled token
        """
        return idx + 1
    
    def hash_token(self, token):
        """Hashes tokens during annonymization

        By default, converts each individual token into its 4-byte shake_128 hash

        Args:
            token (str): the string token to hash
        
        Returns:
            str: the 4-byte shake_128 hash of the given token
        """
        hasher = hashlib.shake_128()
        hasher.update(token.encode('utf-8'))
        return hasher.hexdigest(4)

    def __call__(self, *strings, cfg=None, block=None, enforce_asm_rules=None, 
                 newline_tup=DEFAULT_TOKENIZER.DEFAULT_NEWLINE_TUPLE, match_instruction_address=True):
        """Normalizes the given iterable of strings.

        Args:
            strings (str): arbitrary number of strings to normalize
            cfg (Union[CFG, MemCFG], optional): either a ``CFG`` or ``MemCFG`` object that these lines occur 
                in. Used for determining function calls to self, internal functions, and external functions. If not 
                passed, then these will not be used. Defaults to None.
            block (Union[CFGBasicBlock, int], optional): either a ``CFGBasicBlock`` or integer block_idx in a ``MemCFG``
                object. Used for determining function calls to self, internal functions, and external functions. If not 
                passed, then these will not be used. Defaults to None.
            enforce_asm_rules (Optional[bool]): if True, then extra processing and checks will be done to make sure the 
                tokenized assembly language matches the rules of assembly. See self.check_assembly_rules() for more info. 
                If False, these checks aren't done and bad assembly could make its way through without error, but should 
                be noticeably faster. If None, will use the default value. The default value starts as False at the
                beginning of program execution but can be modified using 
                :func:`~bincfg.normalization.base_tokenizer._set_default_enforce_asm_rules`
            newline_tup (Tuple[str, str], optional): the tuple to insert inbetween each passed string, or None to not 
                insert anything. Defaults to DEFAULT_TOKENIZER.DEFAULT_NEWLINE_TUPLE.
            match_instruction_address (bool, optional): if True, will assume there will be an instruction address at the 
                start of the string. This only has an effect on ghidra-like instruction addresses where that address 
                could be interpreted as either an immediate, or an instruction address. If True, then any immediates 
                found at the start of a line will be assumed to be instruction addresses instead of immediates. If False,
                then instruction addresses can still be matched, but they must end with a colon ':', otherwise they will 
                be considered immediates. Defaults to True.

        Raises:
            MalformedMemoryExpressionError: when memory expressions are malformed
            MisplacedInstructionPrefixError: when instruction prefixes do not occur in the correct place
            TokenMismatchError: on a bad branch prediction string

        Returns:
            List[str]: a list of normalized string instruction lines
        """
        return self.normalize(*strings, cfg=cfg, block=block, newline_tup=newline_tup, match_instruction_address=match_instruction_address)
    
    def __eq__(self, other):
        """Checks equality between this normalizer and another. 
        
        Defaults to checking if class types, tokenizers, and tokenization_level are the same. Future children should 
            also check any kwargs.
        """
        return type(self) == type(other) and self.tokenizer == other.tokenizer and self.tokenization_level is other.tokenization_level \
            and eq_obj([r for r, _ in self.opcode_handlers], [r for r, _ in other.opcode_handlers]) \
            and self.anonymize_tokens == other.anonymize_tokens
    
    def __repr__(self) -> str:
        return self.__class__.__name__ + "()"
    
    def __str__(self) -> str:
        return self.__class__.__name__


_DILL_IMPORT_ERROR_MESSAGE = 'Package is required to pickle Normalizer() objects!'
class _Pickled_Normalizer():
    """A pickled version of a normalizer, often requiring the `dill` package"""
    def __init__(self, normalizer):
        if hasattr(normalizer, 'normalize') and (not hasattr(normalizer, '__requires_dill__') or normalizer.__requires_dill__):
            self._normalizer = get_module('dill', err_message=_DILL_IMPORT_ERROR_MESSAGE).dumps(normalizer)
            self._using_dill = True
        else:
            self._normalizer = normalizer
            self._using_dill = False

    def unpickle(self):
        return get_module('dill', err_message=_DILL_IMPORT_ERROR_MESSAGE).loads(self._normalizer) if self._using_dill else self._normalizer


class MalformedMemoryExpressionError(Exception):
    """Error raised when there is a malformed memory expression"""

    def __init__(self, name, token, line):
        if name == Tokens.NEWLINE:
            start = "Reached newline before closing memory brackets"
        elif name == Tokens.OPEN_BRACKET:
            start = "Reached another open_bracket before closing memory brackets"
        elif isinstance(name, str):
            start = name
        else:
            start = "Malformed memory expression"
        token = token if isinstance(token, str) else 'None' if token is None else token[1]
        super().__init__("%s at token %s. Current line: \"%s\"" % (start, repr(token), line))


class MisplacedInstructionPrefixError(Exception):
    """Error raised when there is an instruction prefix that is not at the start of a line"""

    def __init__(self, token, line, sentence):
        super().__init__("Found an instruction prefix not at the start of line. "
            "At token '%s'. Current line:: %s\nCurrent sentence: %s" % (token, line, sentence))
