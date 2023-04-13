"""Class for tokenizing assembly lines"""

import re
from .tokenization_constants import Tokens, DEFAULT_TOKENS, TokenMismatchError, RE_INSTRUCTION_PREFIX_CODES, \
    RE_BRANCH_PREDICTION
from .norm_utils import clean_incoming_instruction
from .check_asm import check_assembly_rules


_DEFAULT_ENFORCE = False


def _set_default_enforce_asm_rules(val):
    """Sets the default enforce_asm_rules parameter used during tokenization"""
    global _DEFAULT_ENFORCE
    _DEFAULT_ENFORCE = val


class BaseTokenizer:
    """A default class to tokenize assembly line input

    Currently, this tokenization schema can handle outputs from:

        * rose disassembly
        * ghidra disassembly (in BB data)
    
    The tokenizer will tokenize essentially anything, so long as it fits known tokens.

    Known Tokens:

        * Instruction start token: used for op-level normalization techniques
        * Instruction address: the address sometimes present and only at the very start of the instruction that is used to
          specify the address of that instruction. Should be an immediate value
        * Newline token: used to specify new instructions in case multiple instructions are being tokenized in one string.
          Can be a newline character or a pipe character '|'
        * Rose Information: Any information contained within <> brackets is considered rose information (including the brackets)
        * Spacing: one or more spaces, tabs, or commas in a row. Can also be the characters '.' and '_' when used as delimiters
          for instruction prefixes built onto opcodes (see below)
        * PTR: the literal string 'ptr' that appears in some memory accesses (EG: dword ptr [...])
        * Single character tokens used for memory accesses: '[', ']', '+', '*'
        * Instruction prefixes: lock, rep, repne, etc. These can appear as plain string separated by spacing, or attatched
          to opcodes. If attatched to an opcode, its order does not matter, and it must be delimited by either a '.' or a
          '_'. If this occurs, there can only be one opcode, and all of the other substrings must be known instruction
          prefixes
        * Branch prediction tokens: the literal strings ',pt' and ',pn' for branch predictions. Must come immediately
          following an opcode (or opcode + attatched instruction prefix(es)) if present
        * Segment addresses: far-jump address information, eg: "fs:0x123456"
        * Immediate values: can be in binary, octal, decimal, and hex
        * Segment token: memory segment specifier used for memory accesses, eg: "qword ptr ds:[...]", the "ds:" bit
        * Memory size: tokens that specify memory access size, eg: "qword", "dword", "byte", etc.
        * Registers: known register names
        * Opcodes: any string of alphabet characters that does not fit one of the tokens above is considered an opcode

    Anything that does not fit one of the above tokens will be considered a 'token mismatch'

    This will do the following transformations to the incomming token stream:

        * Any instruction prefixes or branch predictions will be moved to immediately before their opcode in the same order
          that they appear in the string (separated by the same spacings that were used before them in the original string). 
          This means any branch prediction will be immediately before the opcode and after any other instruction prefixes 
          since they can only appear at the end of opcode + prefix strings


    Parameters
    ----------
    tokens: `List[Tuple[str, str]]`
        the tokens to use. Should be a list of 2-tuples. Each tuple is a pair of (name, regex) where
        name is the string name of the token, and regex is a regular expression to find that token. These
        tuples should be ordered in the preferred order to search for tokens with a 'mismatch' token matching
        all characters at the very end to find mismatch lines. Defaults to `bincfg.normalization.tokenization_constants.DEFAULT_TOKENS`
    """

    DEFAULT_NEWLINE_TUPLE = (Tokens.NEWLINE, '\n')
    """The default (token_name, token) tuple to use for newlines"""

    def __init__(self, tokens=DEFAULT_TOKENS):
        self.tokens = tokens
        self.tokenizer = re.compile('|'.join([("(?P<%s>%s)" % pair) for pair in self.tokens]), flags=re.M)

    def tokenize(self, *strings, enforce_asm_rules=None, newline_tup=DEFAULT_NEWLINE_TUPLE, match_instruction_address=True):
        """Tokenizes some number of strings in the order they were recieved returning a list of 2-tuples. 
        
        Each tuple is (name, token) where name is the string name of the token, and token is the substring in the given 
        string corresponding to that token. Extra 'newline' tuples will be added inbetween each string.

        Initially cleans the string. See :func:`~bincfg.normalization.norm_utils.clean_incoming_instruction` for more details.

        Also pulls prefixes out of opcodes. See top of file for possible placements of instruction prefixes. These prefixes
        are returned in order before the opcode, with no extra newlines or anything.

        Args:
            strings (str): arbitrary number of strings to tokenize.
            enforce_asm_rules (Optional[bool]): if True, then extra processing and checks will be done to make sure the 
                tokenized assembly language matches the rules of assembly. See self.check_assembly_rules() for more info. 
                If False, these checks aren't done and bad assembly could make its way through without error, but should 
                be noticeably faster. If None, will use the default value. The default value starts as False at the
                beginning of program execution but can be modified using 
                :func:`~bincfg.normalization.base_tokenizer._set_default_enforce_asm_rules`
            newline_tup (Tuple[str, str], optional): the tuple to insert inbetween each passed string, or None to not 
                insert anything. Defaults to `self.__class__.DEFAULT_NEWLINE_TUPLE`.
            match_instruction_address (bool, optional): if True, will assume there will be an instruction address at the 
                start of the string. This only has an effect on ghidra-like instruction addresses where that address 
                could be interpreted as either an immediate, or an instruction address. If True, then any immediates 
                found at the start of a line will be assumed to be instruction addresses instead of immediates. If False,
                then instruction addresses can still be matched, but they must end with a colon ':', otherwise they will 
                be considered immediates. Defaults to True.

        Raises:
            TokenMismatchError: on a bad branch prediction string

        Returns:
            List[Tuple[str, str]]: list of (token_name, token) tuples
        """
        if enforce_asm_rules is None:
            enforce_asm_rules = _DEFAULT_ENFORCE
        
        ret = []

        # This is a weird mess of things, but it works. Maybe I'll clean it later...
        for string in strings:
            previous_newline = True
            clean_string = clean_incoming_instruction(string)
            app = []

            # So we don't have to do the reorganizing every time
            reorganize_tokens = False

            for mo in self.tokenizer.finditer(clean_string):
                name, token = mo.lastgroup, mo.group()
                add_after = []

                if name == Tokens.MISMATCH:
                    self.on_token_mismatch(token, string, mo)
                elif name == Tokens.OPCODE:

                    # For now, we will leave all instruction prefixes and opcodes in their original position (for error checking later)
                    # Leave the original token as-is for error messages
                    total_token = token

                    # If the token ends with commas, then consider those spacing
                    if total_token[-1] == ',':
                        num_commas = len(total_token) - len(re.split(r',+$', total_token)[0])
                        add_after.append((Tokens.SPACING, ',' * num_commas))
                        total_token = total_token[:len(total_token) - num_commas]

                    # Check for something with spacing at the start/end
                    if total_token[0] in '._' or total_token[1] in '._':
                        raise TokenMismatchError("Found an opcode + instruction prefix string that either starts or ends "
                                                 "with a non-whitespace spacing token: %s, in string: %s" 
                                                 % (repr(token), repr(string)))
                    
                    # If there are multiple commas, then consider those multiple attempted branch predictions, which is invalid
                    if total_token.count(',') > 1:
                        raise TokenMismatchError("Found multiple occurances of ',' used for branch predictions in token "
                                                 "%s, only one branch prediction token is allowed. In string: %s" 
                                                 % (repr(token), repr(string)))

                    # Pull out the branch prediction, which should only be at the end
                    if ',' in total_token:
                        reorganize_tokens = True
                        total_token, branch_pred = token.split(',')

                        # Check that it is a known branch prediction
                        if re.fullmatch(RE_BRANCH_PREDICTION, branch_pred) is None:
                            raise TokenMismatchError("Found an unknown branch prediction token %s in string: %s"
                                                     % (repr(branch_pred), repr(string)))
                        
                        add_after += [(Tokens.SPACING, ','), (Tokens.BRANCH_PREDICTION, branch_pred)]
                    
                    # Now split on any '.' and '_', capturing them. Add an extra spacing for the loop
                    op_pre_splits = re.split(r'([._])', total_token) + ['']
                    
                    prefixes_after = []
                    opcode = None
                    in_pre = True
                    for i in range(len(op_pre_splits) // 2):
                        curr_token, spacing = op_pre_splits[i * 2], op_pre_splits[i * 2 + 1]
                        spacing_arr = [(Tokens.SPACING, spacing)] if spacing != '' else []

                        if re.fullmatch(RE_INSTRUCTION_PREFIX_CODES, curr_token) is not None:
                            reorganize_tokens = True
                            prefix_arr = [(Tokens.INSTRUCTION_PREFIX, curr_token)] + spacing_arr
                            if in_pre:
                                app += prefix_arr
                            else:
                                prefixes_after += prefix_arr
                        
                        # Otherwise we have an opcode, check to make sure it is the only one
                        elif opcode is not None:
                            raise TokenMismatchError("Cannot have multiple opcodes in a single opcode + instruction "
                                                     "prefix token. In subtoken %s within token %s, in string %s" 
                                                     % (repr(curr_token), repr(token), repr(string)))
                        
                        else:
                            opcode = curr_token
                            prefixes_after += spacing_arr
                            in_pre = False
                    
                    # Add the prefixes_after to add_after, before branch prediction
                    add_after = prefixes_after + add_after

                    # Set token to our final opcode
                    token = opcode

                # If this is an immediate after newline (non-negative hex), then it should be an instruction address if match_instruction_address is True
                elif name == Tokens.IMMEDIATE and previous_newline and match_instruction_address and len(token) > 2 and token[0] != '-' and token[1] == 'x':  
                    name = Tokens.INSTRUCTION_ADDRESS
                
                # If this is an instruction address without a colon and match_instruction_address is False, consider it an immediate
                elif name == Tokens.INSTRUCTION_ADDRESS and token[-1] != ':' and not match_instruction_address:
                    name = Tokens.IMMEDIATE
                

                # No matter what, if this is an instruction prefix, reorganize the tokens
                if name == Tokens.INSTRUCTION_PREFIX:
                    reorganize_tokens = True
                
                previous_newline = name == Tokens.NEWLINE  # Now we can switch previous_newline around if needed
                app.append((name, token))
                app += add_after

            if newline_tup is not None:
                app.append(newline_tup)
            
            if reorganize_tokens:
                # Move all instruction prefixes to the front before the opcode, including branch prediction at the end of them
                # Do this after the check_assembly_rules since that wants the tokens in the same order for error checking
                final_app = []
                opcode = None
                found_prefix = False
                only_prefix = True
                prefix_spacing = None
                for token_type, token in app:
                    
                    # Make sure we can't accidentally override opcodes, eg: "lock opcode1 opcode2"
                    # Otherwise if we have already found an opcode before, this new opcode should break us out of looking for
                    #   instruction prefixes for the first opcode, and instead start looking at the second one
                    if token_type in [Tokens.OPCODE]:
                        if opcode is not None:
                            final_app.append(opcode)
                            prefix_spacing = final_app.append(prefix_spacing) if prefix_spacing is not None else None  # Heh
                        opcode = (token_type, token)
                        only_prefix = False
                        continue

                    # If this is one of the spacings used after opcodes for instruction prefixes ('.' and '_' and ','), then keep
                    #   track of it to possibly put it after the next instruction prefix.
                    # Make sure there isn't already a prefix spacing
                    elif token_type in [Tokens.SPACING] and token in ['.', '_', ','] and opcode is not None and prefix_spacing is None:
                        prefix_spacing = (token_type, token)
                        continue
                    
                    # Otherwise if this token is an instruction prefix and we have an active opcode, add it and any spacing
                    elif token_type in [Tokens.INSTRUCTION_PREFIX, Tokens.BRANCH_PREDICTION] and opcode is not None:
                        found_prefix = True
                        final_app.append((token_type, token))
                        prefix_spacing = final_app.append(prefix_spacing) if prefix_spacing is not None else None  # Heh
                        continue
                    
                    # Otherwise we have no more instruction prefixes. Add opcode/spacing if needed, and add this token normally
                    else:
                        if opcode is not None:
                            opcode = final_app.append(opcode)
                            prefix_spacing = final_app.append(prefix_spacing) if prefix_spacing is not None else None  # Heh
                        
                        # If this is a newline token, check if we should merge this line into the next one because it
                        #   only contains instruction prefixes
                        if token_type in [Tokens.NEWLINE]:
                            if found_prefix and only_prefix:
                                found_prefix = False
                                final_app.append((Tokens.SPACING, ' '))
                                continue

                            found_prefix = False
                            only_prefix = True
                        
                        elif token_type in [Tokens.INSTRUCTION_PREFIX, Tokens.BRANCH_PREDICTION]:
                            found_prefix = True
                        
                        elif token_type not in [Tokens.SPACING, Tokens.ROSE_INFO]:
                            only_prefix = False
                    
                        final_app.append((token_type, token))
            
            else:
                final_app = app
        
            # Check the assembly is correct if using
            if enforce_asm_rules:
                check_assembly_rules(final_app, app, clean_string, newline_tup)
            
            ret += final_app

        return ret

    def __call__(self, *strings, enforce_asm_rules=None, newline_tup=DEFAULT_NEWLINE_TUPLE, match_instruction_address=True):
        """Tokenizes some number of strings in the order they were recieved returning a list of 2-tuples. 
        
        Each tuple is (name, token) where name is the string name of the token, and token is the substring in the given 
        string corresponding to that token. Extra 'newline' tuples will be added inbetween each string.

        Initially cleans the string. See :func:`~bincfg.normalization.norm_utils.clean_incoming_instruction` for more details.

        Also pulls prefixes out of opcodes. See top of file for possible placements of instruction prefixes. These prefixes
        are returned in order before the opcode, with no extra newlines or anything.

        Args:
            strings (str): arbitrary number of strings to tokenize.
            enforce_asm_rules (Optional[bool]): if True, then extra processing and checks will be done to make sure the 
                tokenized assembly language matches the rules of assembly. See self.check_assembly_rules() for more info. 
                If False, these checks aren't done and bad assembly could make its way through without error, but should 
                be noticeably faster. If None, will use the default value. The default value starts as False at the
                beginning of program execution but can be modified using 
                :func:`~bincfg.normalization.base_tokenizer._set_default_enforce_asm_rules`
            newline_tup (Tuple[str, str], optional): the tuple to insert inbetween each passed string, or None to not 
                insert anything. Defaults to `self.__class__.DEFAULT_NEWLINE_TUPLE`.
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
        return self.tokenize(*strings, enforce_asm_rules=enforce_asm_rules, newline_tup=newline_tup, 
                             match_instruction_address=match_instruction_address)
    
    def __eq__(self, other):
        """Checks equality between this tokenizer and another. 
        
        Defaults to checking if class types are the same and tokens are all equal and in same order. Children should 
            also include other kwargs, etc.
        """
        return self is other or (type(self) == type(other) and all((n1 == n2 and t1 == t2) for (n1, t1), (n2, t2) in zip(self.tokens, other.tokens)))
    
    def on_token_mismatch(self, token, string, mo):
        """What to do when there is a token mismatch in a string. Raises a ``TokenMismatchError`` with info on the mismatch

        Args:
            token (str): the token that is mismatched
            string (str): the string in which the mismatch occurred
            mo (Match): the re match object

        Raises:
            TokenMismatchError: by default
        """
        err_start = "Mismatched token '%s' at index %d in string: \"" % (token, mo.start())
        raise TokenMismatchError("%s%s\"\n%s" % (err_start, string, '-' * (mo.start() + len(err_start) + len(TokenMismatchError.__name__) + 2) + '^'))
    
    def __repr__(self) -> str:
        return self.__class__.__name__ + "()"
    
    def __str__(self) -> str:
        return self.__class__.__name__
    
    def __getstate__(self):
        """For some reason, sufficiently complex regex patterns cannot be deepcopied (Perhaps only a python 3.6 problem?).
        Instead, get the string pattern itself and copy that, then re-compile in __setstate__
        """
        ret = self.__dict__.copy()
        ret['tokenizer'] = self.tokenizer.pattern
        return ret
    
    def __setstate__(self, state):
        state['tokenizer'] = re.compile(state['tokenizer'])
        for k, v in state.items():
            setattr(self, k, v)


# A default tokenizer class
DEFAULT_TOKENIZER = BaseTokenizer()
