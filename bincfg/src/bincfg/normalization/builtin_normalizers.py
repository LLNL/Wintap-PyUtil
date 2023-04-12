"""
A bunch of builtin normalization methods based on literature.

NOTE: some of these are slightly modified from their original papers either for code purposes, or because we are using
decompiled binaries instead of compiled assembly and thus lose out on some information (EG: symbol information
for jump instructions)
"""

from .base_normalizer import BaseNormalizer, _Pickled_Normalizer
from .tokenization_constants import TokenizationLevel
from .norm_utils import return_immstr, ignore, replace_function_call_immediate, replace_general_register, \
    replace_memory_expression, threshold_immediate, special_function_call, memsize_value, replace_jmpdst, \
    eq_special_funcs, FUNCTION_CALL_STR, IMMEDIATE_VALUE_STR, MEMORY_EXPRESSION_STR, DEFAULT_IMMEDIATE_THRESHOLD
from ..utils import get_special_function_names


def get_normalizer(normalizer):
    """Returns the normalizer being used.

    Args:
        normalizer (Union[str, Normalizer]): either a ``Normalizer`` object (IE: has a callable 'normalize' function), 
            or a string name of a built-in normalizer to use
            Accepted strings include: 'innereye', 'deepbindiff', 'safe', 'deepsem'/'deepsemantic', 'none'/'unnormalized'

    Raises:
        ValueError: for unknown string name of normalizer
        TypeError: if `normalizer` was not a string or ``Normalizer`` object

    Returns:
        Normalizer: a ``Normalizer`` object
    """
    if isinstance(normalizer, str):
        norm_str = normalizer.lower()
        if norm_str.endswith("_normalizer"):
            norm_str, *_ = norm_str.rpartition("_normalizer")
        elif norm_str.endswith("_norm"):
            norm_str, *_ = norm_str.rpartition("_norm")
        
        if norm_str in ['innereye', 'inner', 'innereyenormalizer']:
            return InnerEyeNormalizer()
        elif norm_str in ['deepbindiff', 'bindiff', 'deepbin', 'deepbindiffnormalizer']:
            return DeepBinDiffNormalizer()
        elif norm_str in ['safe', 'safenormalizer']:
            return SafeNormalizer()
        elif norm_str in ['deepsem', 'deepsemantic', 'semantic', 'deepsemanticnormalizer']:
            return DeepSemanticNormalizer()
        elif norm_str in ['none', 'unnorm', 'unnormalized', 'base', 'basenormalizer']:
            return BaseNormalizer()
        elif norm_str in ['compressed', 'stats', 'comp_stats', 'compressed_stats', 'statistics']:
            return CompressedStatsNormalizer()
        else:
            raise ValueError("Unknown normalization string: '%s'" % normalizer)
    
    elif isinstance(normalizer, _Pickled_Normalizer):
        return get_normalizer(normalizer.unpickle())
    
    elif hasattr(normalizer, 'normalize') and callable(normalizer.normalize):
        return normalizer
    
    else:
        raise TypeError("Unknown normalizer type: '%s'" % normalizer)

        
class InnerEyeNormalizer(BaseNormalizer):
    """A normalizer based on the Innereye method

    :inherited-members: BaseNormalizer
    
    From the InnerEye paper: https://arxiv.org/pdf/1808.04706.pdf

    Rules:

        * Constant values are ignored and replaced with 'immval' or '-immval' for negative values
        * Function names are ignored and replaced with 'func'
        * Jump destinations are 'immval'
        * Registers are left as-is 
        * Doesn't say anything about memory sizes, so they are ignored
        * Tokens are at the instruction-level
    """

    DEFAULT_TOKENIZATION_LEVEL = TokenizationLevel.INSTRUCTION
    """"""
    handle_immediate = return_immstr(include_negative=True)
    """"""
    handle_memory_size = ignore
    """"""
    handle_function_call = replace_function_call_immediate(FUNCTION_CALL_STR)
    """"""
        

class DeepBinDiffNormalizer(BaseNormalizer):
    """A normalizer based on the Deep Bin Diff method
    
    From the DeepBinDiff paper: https://www.ndss-symposium.org/wp-content/uploads/2020/02/24311-paper.pdf

    Rules:

        * Constant values are ignored and replaced with 'immval'
        * General registers are renamed based on length, special ones are left as-is (with number information removed.
            EG: st5 -> st, rax -> reg8, r14d -> reg4, rip -> rip, zmm13 -> zmm)
        * Memory expressions are replaced with 'memexpr'
        * Can't really tell what's supposed to be done with function calls, will just assume they should be 'call immval'
        * Jump destinations are 'immval'
        * Doesn't say anything about memory sizes, so they are ignored
        * Tokens are at the op-level
    """

    DEFAULT_TOKENIZATION_LEVEL = TokenizationLevel.OPCODE
    """"""
    
    handle_immediate = return_immstr
    """"""
    handle_memory_size = ignore
    """"""
    handle_function_call = replace_function_call_immediate(IMMEDIATE_VALUE_STR)
    """"""
    handle_register = replace_general_register
    """"""
    handle_memory_expression = replace_memory_expression(MEMORY_EXPRESSION_STR)
    """"""


class SafeNormalizer(BaseNormalizer):
    """A normalizer based on the SAFE method
    
    From the SAFE paper: https://github.com/gadiluna/SAFE

    Rules:

        * All base memory addresses (IE: memory addresses that are constant values) are replaced with 'immval'
        * All immediate values greater than some threshold (safe_threshold parameter, they use 5000 in the
          paper) are replaced with 'immval'
        * Function calls are replaced with 'self' if a recursive call, 'innerfunc' if the function is within
          the binary, and 'externfunc' if the function is external.

          NOTE: this is different to how it is done in the SAFE paper (I believe they just keep the function names), 
          but I made the executive decision to change it for OOV problems, and changed it instead to how it is done 
          in the deepsemantic paper to try and give it the most information possible)
        * Jump destinations are 'immval'
        * Doesn't say anything about memory sizes, so they are ignored
        * Doesn't say anything about registers, so they are left as-is
        * Tokens are at the instruction-level

    
    Parameters
    ----------
    imm_threshold: `int`
        immediate values whose absolute value is <= imm_threshold will be left alone, those above it will be replaced 
        with the string 'immval'
    special_functions: `Optional[Set[str]]`
        a set of special function names. All external functions whose name (ignoring the '@plt' at the end) is in this 
        set will have their name kept, otherwise they will be replaced with 'externfunc'. If None, will attempt to load 
        the default special function names from :func:`bincfg.utils.cfg_utils.get_special_function_names`. If you do not
        wish to use any special function names, then pass an empty set.
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
    """

    DEFAULT_TOKENIZATION_LEVEL = TokenizationLevel.INSTRUCTION
    """"""

    def __init__(self, imm_threshold=DEFAULT_IMMEDIATE_THRESHOLD, special_functions=None, tokenizer=None, token_sep=None, 
        tokenization_level=TokenizationLevel.AUTO):

        self.imm_threshold = imm_threshold
        self.special_functions = get_special_function_names() if special_functions is None else special_functions
        super().__init__(tokenizer=tokenizer, token_sep=token_sep, tokenization_level=tokenization_level)

        self.handle_immediate = threshold_immediate(self.imm_threshold)

    handle_memory_size = ignore
    """"""
    handle_function_call = special_function_call
    """"""
        
    def __eq__(self, other):
        return super().__eq__(other) and other.imm_threshold == self.imm_threshold and eq_special_funcs(self.special_functions, other.special_functions)
    

class DeepSemanticNormalizer(BaseNormalizer):
    """A normalizer based on the Deepsemantic method
    
    from the DeepSemantic paper:
    https://arxiv.org/abs/2106.05478

    Rules:

        * Immediates can fall into multiple categories:
            a. Function calls:
                - libc function name(): "libc[name]" (not used)
                - recursive call: 'self'
                - function within the binary: 'innerfunc'
                - function outside the binary: 'externfunc'
            b. Jump (branching) family: "jmpdst"
            c. Reference: (NOTE: This is not done as I don't know how to do it with ROSE...)
                - String literal: 'str'
                - Statically allocated variable: "dispbss"
                - Data (data other than a string): "dispdata"
            d. Default (all other immediate values): "immval"

        * Registers can fall into multiple categories:
            a. Stack/Base/Instruction pointer: Keep track of type and size
                [e|r]*[b|s|i]p[l]*  ->  [s|b|i]p[1|2|4|8]
            b. Special purpose (IE: flags): Keep track of type
                cr[0-15], dr[0-15], st([0-7]), [c|d|e|f|g|s]s  ->  reg[cr|dr|st], reg[c|d|e|f|s]s
            c. AVX registers: Keep track of type
                [x|y|z]*mm[0-7|0-31]  ->  reg[x|y|z]*mm
            d. General purpose registers: Keep track of size
                [e|r]*[a|b|c|d|si|di][x|l|h]*, r[8-15][b|w|d]*  ->  reg[1|2|4|8]
        
        * Pointers can fall into multiple categories:
            a. Direct, small: keep track of size
                byte,word,dword,qword,ptr  ->  memptr[1|2|4|8]
            b. Direct, large: keep track of size
                tbyte,xword,[x|y|z]mmword  ->  memptr[10|16|32|64]
            c. Indirect, string: 
                [base+index*scale+displacement]  ->  [base+index*scale+dispstr]
            d. Indirect, not string:
                [base+index*scale+displacement]  ->  [base+index*scale+disp]
        
        * Tokenized at instruction-level
    
    Parameters
    ----------
    special_functions: `Optional[Set[str]]`
        a set of special function names. All external functions whose name (ignoring the '@plt' at the end) is in this 
        set will have their name kept, otherwise they will be replaced with 'externfunc'. If None, will attempt to load 
        the default special function names from :func:`bincfg.utils.cfg_utils.get_special_function_names`. If you do not
        wish to use any special function names, then pass an empty set.
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
    """

    DEFAULT_TOKENIZATION_LEVEL = TokenizationLevel.INSTRUCTION
    """"""

    def __init__(self, special_functions=None, tokenizer=None, token_sep=None, tokenization_level=TokenizationLevel.AUTO):
        self.special_functions = get_special_function_names() if special_functions is None else special_functions
        super().__init__(tokenizer=tokenizer, token_sep=token_sep, tokenization_level=tokenization_level)
    
    handle_register = replace_general_register
    """"""
    handle_immediate = return_immstr
    """"""
    handle_memory_size = memsize_value
    """"""
    handle_function_call = special_function_call
    """"""
    handle_jump = replace_jmpdst
    """"""
        
    def __eq__(self, other) -> bool:
        return super().__eq__(other) and eq_special_funcs(self.special_functions, other.special_functions)


class MyNormalizer(BaseNormalizer):
    """A normalizer I created. Combines safe, deepsem, and deepbindiff methods, also uses opcode tokenization

    Rules:

        * Immediates are handled the same as in safe
        * function calls are handled the same as deepsemantic
        * jump destinations are 'jmpdst'
        * registers are handled the same as deepsem/deepbindiff
        * memory pointers/memory expressions are handled the same as in deepsemantic
        * Tokenized at the opcode-level
    
    Parameters
    ----------
    imm_threshold: `int`
        immediate values whose absolute value is <= imm_threshold will be left alone, those above it will be replaced 
        with the string 'immval'
    special_functions: `Optional[Set[str]]`
        a set of special function names. All external functions whose name (ignoring the '@plt' at the end) is in this 
        set will have their name kept, otherwise they will be replaced with 'externfunc'. If None, will attempt to load 
        the default special function names from :func:`bincfg.utils.cfg_utils.get_special_function_names`. If you do not
        wish to use any special function names, then pass an empty set.
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
    """

    DEFAULT_TOKENIZATION_LEVEL = TokenizationLevel.OPCODE
    """"""

    def __init__(self, imm_threshold=DEFAULT_IMMEDIATE_THRESHOLD, special_functions=None, tokenizer=None, token_sep=None, 
        tokenization_level=TokenizationLevel.AUTO):

        self.imm_threshold = imm_threshold
        self.special_functions = get_special_function_names() if special_functions is None else special_functions
        super().__init__(tokenizer=tokenizer, token_sep=token_sep, tokenization_level=tokenization_level)

        self.handle_immediate = threshold_immediate(self.imm_threshold)
            
    handle_register = replace_general_register
    """"""
    handle_jump = replace_jmpdst
    """"""
    handle_memory_size = memsize_value
    """"""
    handle_function_call = special_function_call
    """"""
        
    def __eq__(self, other):
        return super().__eq__(other) and other.imm_threshold == self.imm_threshold and eq_special_funcs(self.special_functions, other.special_functions)


class CompressedStatsNormalizer(BaseNormalizer):
    """A normalizer I created for use in CFG.get_compressed_stats()

    Rules:

        * Immediates are treated like in safe, but with a much lower default threshold
        * function calls are either self vs. intern vs. extern func, no special functions
        * jump destinations are 'jmpdst'
        * registers are handled the same as deepsem/deepbindiff
        * memory pointers/memory expressions are handled the same as in deepsemantic
        * Tokenized at the instruction-level
    
    Parameters
    ----------
    imm_threshold: `int`
        immediate values whose absolute value is <= imm_threshold will be left alone, those above it will be replaced 
        with the string 'immval'. Defaults to a small value
    special_functions: `Optional[Set[str]]`
        a set of special function names. All external functions whose name (ignoring the '@plt' at the end) is in this 
        set will have their name kept, otherwise they will be replaced with 'externfunc'. If None, this will default to
        not using any special functions
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
    """

    DEFAULT_TOKENIZATION_LEVEL = TokenizationLevel.INSTRUCTION
    """"""

    def __init__(self, special_functions=None, tokenizer=None, token_sep=None, 
        tokenization_level=TokenizationLevel.AUTO):

        self.special_functions = set() if special_functions is None else special_functions
        super().__init__(tokenizer=tokenizer, token_sep=token_sep, tokenization_level=tokenization_level)
            
    handle_register = replace_general_register
    """"""
    handle_jump = replace_jmpdst
    """"""
    handle_memory_size = memsize_value
    """"""
    handle_function_call = special_function_call
    """"""
    handle_immediate = return_immstr(include_negative=True)
    """"""
