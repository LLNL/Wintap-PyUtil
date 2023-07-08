"""Methods to check that the assmebly tokens generated are working correctly, and the data inputted is valid"""

from .norm_utils import imm_to_int
from .tokenization_constants import ARCH, Tokens
import re
import os


def check_assembly_rules(token_list, original_token_list, string, newline_tup, arch=ARCH.X86_64):
    """Checks assembly rules on the given token list

    This exists mostly so that I know I am tokenizing assembly instructions correctly and there are no bugs
    
    Makes sure that:

        - opcodes are known opcodes
        - opcodes have correct number/types of operands
        - memory addresses are non-negative and under the max memory address size
        - instruction prefixes are placed on acceptable instructions
        - branch predictions are only placed on jump instructions
        - token order makes sense (IE: operands/registers only follow opcodes, etc.)
        - memory addressing uses acceptible registers and is used in acceptable opcodes

    Information on things that exist:

        - Instruction prefixes: these exist on x86 and x86_64 instructions. More information about them can be
            found here: https://wiki.osdev.org/X86-64_Instruction_Encoding#ModR.2FM_and_SIB_bytes.

            It seems that any instruction prefixes that we care about can exist on both x86 and x86_64 instruction
            sets.

            There are 4 instruction prefix groups for x86:

            1. LOCK, REP, REPE/REPZ, REPNE/REPNZ

                * the slashes mean both mnemonics correspond to the same byte squence and therefore the same 
                    instruction prefix, they are aliases)

            2. cs/ss/ds/es/fs/gs segment overrides, branch prediction hints

                * I think the segment overrides only appear as the "cs:", "gs:", etc. strings that appear before
                    memory addresses and those are already tokenized
                * the branch prediction hints are strings ',pt' and ',pn' for "predict taken" and "predict not
                    taken", and should only appear after branching instructions. (which I believe are the instructions
                    that start with a 'j', and only those)
            
            3. Operand-size override prefix

                * REX prefix - allows access in long mode to 64-bit registers, and extra added registers 
                    (rax, r15, etc.). These exist in the strings as simply the register names, so no need to parse
                * VEX/XOP

            4. Address-size override prefix

                * allows for 64-bit addressing. This will be handled by checking that the address size does not
                    exceed 64-bits. There is no prefix in the string since it's just the memory address that would
                    appear in the instruction
            
            There also exist other opcode prefixes (EG: the VEX/XOP prefixes for AVX/SSE instructions), but these also
            just seem to change the instruction mnemonic that would appear in the strings.

            This leaves us with only the instruction prefixes: LOCK, REPNE/REPNZ, REP, REPE/REPZ, along with the branch
            prediction hints ',pt' and ',pn'.

            - LOCK: allows for atomic operations on certain instructions. Allowed on any instruction
            - REP: repeats the instruction up to CX times, decrementing CX after each time. Allowed instructions:
              INS, LODS, MOVS, OUTS and STOS
            - REPE/REPZ: repeat the instruction until CX reaches 0 or when ZF is set to 0. Allowed instructions:
              CMPS, CMPSB, CMPSD, CMPSW, SCAS, SCASB, SCASD and SCASW
            - REPNE/REPNZ: repeat the instruction until CX reaches 0 or when ZF is set to 1. Allowed instructions:
              CMPS, CMPSB, CMPSD, CMPSW, SCAS, SCASB, SCASD and SCASW (same as REPE/REPZ)
            - branch prediction hints: ',pt' and ',pn' that can only appear after branching instructions. As far as
              I can tell, an instruction is a branching instruction iff it starts with a 'j' in x86/x86_64
        
            It also seems that only one instruction prefix per group can be used. This means a max of one of the LOCK, 
            REP, etc. instructions, as well as a max of one of the branch prediction hints can be used on any one
            instruction at a time.
        
        - Memory access methods. There are a few forms that are allowed for a memory access:
            
            {memsize}?{ptr}?{segment}?*{memory_expression}

            - memsize (optional): a Tokens.MEMORY_SIZE token specifying the memory access size. Optional, but if it exists,
              must be at the start of the memory access expression and be followed by one or more spacing. Max of one of these
            - ptr (optional): a Tokens.PTR token. Optional, but if it exists, must follow any 'memsize' expressions and come
              before everything else in the memory access expression, as well as be followed by one or more spacing. Max of 
              one of these
            - segment (optional): a Tokens.SEGMENT token specifying the memory segment override for a memory access. Optional,
              but if it exists, must follow any 'memsize' and 'ptr' expressions. Can optionally be followed by any amount of
              spacing, but must come directly before a 'memory_expression' expression. Max of one of these
            - memory_expression: a general memory expression (see format below). Must exist, and must be the last thing in
              a memory access expression. These use some combination of displacement (D), base (B), scale (S), index (I), and
              possibly program counter-relative (P).

              * displacement (D): an immediate value to read from/add to addresses. Must be a 32-bit value (with one exception
                listed below for format "[D]"). Can be either a positive value fitting in 32-bits, or a negative value whose
                two's complement representation would fit within 32 bits (IE: it can be a value in the range [-(2**31), 2**32 - 1])
              * base (B): a general purpose register to start the memory address from
              * index (I): a general purpose register to add to addresses
              * scale (S): a value to multiply sections of the computation by to allow for different scales/sizes of objects
                in memory. Can only be 1, 2, 4, or 8 (a 2-bit constant factor). Can only be used on the index (I) register
              * program-counter-relative (P): a special mode added in x86_64 that allows one to compute addresses based
                on the program counter ('rip' register, specifically the address of the *next* instruction). As far as I
                can tell, this is only used with the 'rip' register (EG: not the 'eip' register). It also seems that this
                addressing mode can be used with any instructions that have memory accesses
            
            Caveat: you can use any register you'd like in 'nop' instructions as they are just meant to take up space
            and will not be interpreted
            
            Allowed formats:

                * [D] - In this mode, the displacement must be a 32-bit value, unless the opcode is a 'movabs' instruction
                  with one operand being this displacement, and the other being a part of the 'a' register (rax, eax, ax, al).
                  NOTE: I'm not sure about the 'ah' register, but I'll say it can't be used for now unless I get shown otherwise
                * [B]
                * [B + I]
                * [B + D]
                * [B + I + D]
                * [B + I*S]
                * [I*S + D]
                * [B + I*S + D]
                * [P]
                * [P + D] - In this mode, displacement must be a 32-bit value. See: 
                  https://xem.github.io/minix86/manual/intel-x86-and-64-manual-vol2/o_b5573232dd8f1481-72.html
            
            See more info here: https://blog.yossarian.net/2020/06/13/How-x86_64-addresses-memory

            Memory_expressions must start with a '[' (Tokens.OPEN_BRACKET) token, and end with a ']' (Tokens.CLOSE_BRACKET)
            token. The '+' and '*' are Tokens.PLUS_SIGN and Tokens.TIMES_SIGN tokens respectively
    
    Args:
        token_list (List[Tuple[str, str]]): output from self.tokenize, list of (token_name, token) tuples
        original_token_list (List[Tuple[str, str]]): the original token list, before any tokens are moved around
        string (str): string used to generate all of these tokens. The token values will be concatenated together to 
            ensure they are equal to the this string, just to make sure we aren't accidentally deleting string 
            information in the tokenization process.
            Assumes these strings are the exact, post-processed/post-cleaned strings that are being tokenized
        newline_tup (Tuple[Literal[Tokens.NEWLINE], str]): the newline_tup that was used in the tokenizer. This
            should be a Tokens.NEWLINE token, otherwise I can't guarantee that this will work (how would we know if a 
            token is a newline_tup token or a misplaced token?, and if None, how would we know when one instruction
            stops and another starts?)
        arch (ARCH): architecture being used
    """
    # This function won't work if the newline tuple isn't a Tokens.NEWLINE token or None
    if newline_tup is not None and newline_tup[0] != Tokens.NEWLINE:
        raise ValueError("Cannot check the assembly tokens with a newline_tup that is not a Tokens.NEWLINE: %s" % newline_tup)
    
    if len(token_list) == 0:
        return
    
    check_tokens = token_list if newline_tup is None else token_list[:-1]
    curr_tokens = []
    for token_type, token in check_tokens:
        if token_type in [Tokens.NEWLINE]:
            _check_instruction(curr_tokens)
            curr_tokens = []
        else:
            curr_tokens.append((token_type, token))
    
    # Check everything that is left if there is any
    _check_instruction(curr_tokens)

    # Now that everything is all good, we can check that the concatenation of the strings is good
    expected = string + (newline_tup[1] if newline_tup is not None else '')
    
    generated = ""
    for _, token in original_token_list:
        generated += token
    
    # Things may be rearranged if there are instruction prefixes or branch predictions
    if expected != generated:
        raise BadAssemblyError("Original string reconstruction differs after tokenizing!\nOriginal: "
                               "%s\nAfter tokenizing: %s" % (repr(expected), repr(generated)), original_token_list, None)
    
    # We passed our checks!


def _check_instruction(token_list):
    """Checks that a single instruction is valid
    
    Args:
        token_list (List[Tuple[str, str]]): list of tokens for one instruction
    """
    if len(token_list) == 0:
        return
    
    instr_as_str = ''.join([TOKEN_TO_REGEX_CHAR[tt] for tt, _ in token_list])
    m = re.fullmatch(INSTRUCTION_REGEX, instr_as_str)
    if m is None:
        raise BadAssemblyError("Could not match to instruction", token_list, instr_as_str)
    
    groups = m.groupdict()
    opcode = _scan_for_token(Tokens.OPCODE, token_list)[1]
    
    # Check that the instruction address is not negative and fits in 64-bit
    if groups['addr'] is not None:
        address = _scan_for_token(Tokens.INSTRUCTION_ADDRESS, token_list)[1]
        if not _fits_in_bits(imm_to_int(address if ':' not in address else address[:-1]), 64, signed=False):
            raise BadAssemblyError("Found instruction address that cannot fit in a 64-bit unsigned integer", token_list, instr_as_str)
    
    # Check that there is only one instruction prefix, and that it is used correctly
    if groups['prefix'] is not None:
        if len(groups['prefix']) > 1:
            raise BadAssemblyError("Found multiple instruction prefixes", token_list, instr_as_str)
        
        elif len(groups['prefix']) == 1:
            prefix_token = _scan_for_token(Tokens.INSTRUCTION_PREFIX, token_list)[1]
            if opcode not in INSTRUCTION_PREFIX_ALLOWED_OPCODES[prefix_token]:
                raise BadAssemblyError("Cannot use instruction prefix %s with opcode %s, acceptable opcodes: %s"
                                        % (prefix_token, repr(opcode), INSTRUCTION_PREFIX_ALLOWED_OPCODES[prefix_token]), 
                                        token_list, instr_as_str)
        
    # Check that there is only one branch prediction max, and that it is used on a jump opcode
    if groups['bp'] is not None:
        if len(groups['bp']) > 1:
            raise BadAssemblyError("Found multiple branch prediction prefixes", token_list, instr_as_str)
        if len(groups['bp']) == 1 and not _is_jump_opcode(opcode):
            raise BadAssemblyError("Found a branch prediction prefix on a non-jump opcode. Opcode: %s" % repr(groups['opcode']),
                                token_list, instr_as_str)
    
    # Parse out all of the operands. List of tuples of (operand_type: Literal['r', 's', 'i', 'm'], operand_str, operand_tokens_list)
    operands = []
    start_idx = 0
    for op_match in re.finditer(INSTRUCTION_OPERAND, groups['operands']):
        op_groups = op_match.groupdict()

        if op_groups['register'] is not None:
            start_idx = _scan_for_token(Tokens.REGISTER, token_list, start_idx=start_idx, ret_idx=True)[1] + 1
            operands.append(('r', op_groups['register'], [token_list[start_idx - 1]]))
        elif op_groups['segment_addr'] is not None:
            start_idx = _scan_for_token(Tokens.SEGMENT_ADDRESS, token_list, start_idx=start_idx, ret_idx=True)[1] + 1
            operands.append(('s', op_groups['segment_addr'], [token_list[start_idx - 1]]))
        elif op_groups['immediate'] is not None:
            start_idx = _scan_for_token(Tokens.IMMEDIATE, token_list, start_idx=start_idx, ret_idx=True)[1] + 1
            operands.append(('i', op_groups['immediate'], [token_list[start_idx - 1]]))
        
        # Memory expressions will be handled specially
        elif op_groups['mem_expr'] is not None:
            start_idx = _scan_for_token([Tokens.MEMORY_SIZE, Tokens.PTR, Tokens.SEGMENT, Tokens.OPEN_BRACKET], token_list, 
                                        start_idx=start_idx, ret_idx=True)[1] + 1
            end_idx = _scan_for_token(Tokens.CLOSE_BRACKET, token_list, start_idx=start_idx, ret_idx=True)[1] + 1
            op_token_list = token_list[start_idx:end_idx]
            
            operands.append(('m', op_groups['mem_expr'], op_token_list))
            start_idx = end_idx

        else:
            raise NotImplementedError(op_groups)
    
    # Check that any memory expressions are valid. This needs to be done after getting all of the operands so we can
    #   check if a 64-bit displacement is there and if so, that it uses the rax (or subset) register
    for ot, operand_str, operand_list in operands:
        if ot == 'm':
            _check_mem_expr(operand_str, operands, opcode, operand_list, token_list, instr_as_str)
    
    # Check that this is a known opcode
    if opcode not in X86_64_ISA:
        raise BadAssemblyError("Unknown opcode: %s" % repr(opcode), token_list, instr_as_str)
    
    # Check that the operands match a known specification for that opcode
    valid_spec = False
    for op_spec in X86_64_ISA[opcode]:
        if _valid_spec(operands, op_spec):
            valid_spec = True
            break
    
    if not valid_spec:
        raise BadAssemblyError("Invalid operand specification for %s opcode. Parsed operands: %s\n" % (repr(opcode), operands)
                               + "Valid specifications: %s" % X86_64_ISA[opcode], token_list, instr_as_str)

    # We passed all checks!


def _check_mem_expr(memexpr_str, operands, opcode, op_token_list, instruction_list, instr_as_str):
    """Checks that a given memory expression (including the [] brackets) is valid
    
    Assumes the instruction has already been parsed enough to get a list of operands

    Args:
        memexpr_str (str): the memory expression string from the operator groups
        operands (List[Tuple[str, str, List[Tuple[str, str]]]]): total operand list from _check_instruction
        opcode (str): the opcode being used
        op_token_list (List[str, str]): list of (token_type, token) tuples for the operand
        instruction_list (List[str, str]): list of (token_type, token) tuples for the instruction, used for error messages
        instr_as_str (str): instruction as an encoded string, used for error messages
    """
    m = re.fullmatch(INSTRUCTION_MA_TYPES, memexpr_str)
    
    if m is None:
        raise BadAssemblyError("Invalid memory expression operand: %s\nOperand token list: %s" 
                                % (repr(memexpr_str), op_token_list), instruction_list, instr_as_str)
    
    # There should only be one non-null value in the groupdict. Get the info dictionary for our memory access type
    # You can specify what the first and second registers and immediates types should be.
    # For registers, we have: 'g' - general-purpose register, 'r' - general-purpose or 'rip' register
    # For immediates, we have: '32' - 32-bit value, '64' - 64-bit value (with restrictions), 's' - scale value (1, 2, 4, or 8)
    info_dict = {'r1': 'g', 'r2': 'g', 'i1': '32', 'i2': '32'}
    _, new_info = INSTRUCTION_MEMORY_ACCESS_TYPES_DICT[[k for k, v in m.groupdict().items() if v is not None][0]]
    info_dict.update(new_info)

    # Make sure values are what they should be
    num_registers, num_immediates = 1, 1
    for token_type, token in op_token_list:

        if token_type == Tokens.REGISTER:
            # Caveat: we allow any registers in nop instructions
            if info_dict['r%d' % num_registers] == 'g' and token not in MEMORY_ACCESS_GP_REGISTERS and opcode != 'nop':
                raise BadAssemblyError("Invalid use of a non-general-purpose register (%s) in a memory expression" 
                                        % repr(token), instruction_list, instr_as_str)
            elif info_dict['r%d' % num_registers] == 'r' and token not in MEMORY_ACCESS_GP_REGISTERS and token != 'rip':
                raise BadAssemblyError("Invalid use of a non-general-purpose (and non-rip_ register (%s) in a memory expression" 
                                        % repr(token), instruction_list, instr_as_str)
            num_registers += 1
        
        elif token_type == Tokens.IMMEDIATE:
            immval = imm_to_int(token)
            if info_dict['i%d' % num_immediates] == '32' and not _fits_in_bits(immval, 32, signed=True):
                # But, do allow negative values in 64-bits
                if not (_fits_in_bits(immval, 64) and immval > 2**61):
                    raise BadAssemblyError("Immediate value cannot fit in 32 bits: %d" % immval,
                                            instruction_list, instr_as_str)
            elif info_dict['i%d' % num_immediates] == 's' and immval not in [1, 2, 4, 8]:
                raise BadAssemblyError("Scale value must be 1, 2, 4, or 8, not: %d" % immval,
                                        instruction_list, instr_as_str)
            elif info_dict['i%d' % num_immediates] == '64':
                
                # Here is where we have to check for the 64-bit displacement that can occur
                if not _fits_in_bits(immval, 32, signed=True):
                    if not _fits_in_bits(immval, 64, signed=True):
                        raise BadAssemblyError("Immediate value cannot fit in 32 bits (nor 64 bit): %d" % immval,
                                        instruction_list, instr_as_str)
                    if opcode not in DISPLACEMENT_64_OPCODES:
                        raise BadAssemblyError("64-bit immediate displacement can only be used with the instructions: %s"
                                                % DISPLACEMENT_64_OPCODES, instruction_list, instr_as_str)
                    if len(operands) != 2:
                        raise BadAssemblyError("All 64-bit immediate displacement opcodes require exactly 2 operands, "
                                                "found: %d" % len(operands), instruction_list, instr_as_str)
                    
                    other_operand = operands[0 if operands[0][0] == 'r' else 1][2][0][1]
                    if other_operand not in DISPLACEMENT_64_REGISTERS:
                        raise BadAssemblyError("64-bit immediate displacement used with invalid register %s, " % repr(other_operand)
                                               + "this can only be used with the registers: %s" 
                                               % DISPLACEMENT_64_REGISTERS, instruction_list, instr_as_str)
            
            num_immediates += 1


def _valid_spec(operands, spec):
    """Returns True if the given list of operands fits the given specification, False otherwise

    If the string 'any' is in the specification, then it will match to any combination of operands.
    
    Args:
        operands (List[Tuple[Literal['r', 's', 'i', 'm'], str, List[str, str]]]): The operands. List of tuples of 
            (operand_type, operand_str, operand_tokens_list)
        spec (List[str]): list of string specification tokens
    """
    # Currently only checking that the number of operands matches the specification
    return len(operands) == len(spec) or 'any' in spec
            

def _scan_for_token(token_type, token_list, start_idx=0, ret_idx=False):
    """Scans the token list for the given token type, returning the first (token_type, token) tuple found
    
    Args:
        token_type (Union[str, List[str]]): the token type to find, or list of token types to search for in parallel
        token_list (List[Tuple[str, str]]): the token list to search through
        start_idx (int): index to start searching at (inclusive)
        ret_idx (bool): if True, will return a tuple (scanned_token, token_index) instead
    """
    token_types = [token_type] if isinstance(token_type, str) else list(token_type)
    for i in range(start_idx, len(token_list)):
        if token_list[i][0] in token_types:
            return (token_list[i], i) if ret_idx else token_list[i]
    return None


def _fits_in_bits(val, bits, signed=False):
    """Returns True if the given value can be stored in the given number of bits, False otherwise
    
    NOTE: passing 'signed' means that either val or it's two's complement (if negative) can be expressed as a `bits`-bit
    UNSIGNED integer
    """
    if signed:
        if val < - (2 ** (bits - 1)) or val >= 2 ** bits:
            return False
        return True
    else:
        if val < 0 or val >= 2 ** bits:
            return False
        return True


def _is_jump_opcode(opcode):
    """Checks if the given opcode is a jump instruction
    
    For now, assumes an opcode is a jump instruction iff it starts with a 'j'

    Args:
        opcode (str): the opcode to check
    
    Returns:
        bool: True if the given opcode is a jump instruction, False otherwise
    """
    return opcode.startswith('j')


class BadAssemblyError(Exception):
    def __init__(self, message, instruction_list=None, instr_as_str=None):
        super().__init__("%s\nInstruction(s): %s\nTokenized List: %s\nInstruction as String: %s" 
                         % (message, repr(''.join(t for _, t in instruction_list) if instruction_list is not None else None), 
                            instruction_list, instr_as_str))


def _read_instruction_set(path):
    """Reads an instruction set from the given path
    
    Returns a dictionary where each key is an opcode, and each value is a list of lists of possible operand specifications.
    If it expects no operands, then the value will be [[]]
    """
    with open(path) as f:
        lines = f.readlines()

    ret = {}

    for line in lines:
        # Split on tab to get the opcode + operands. One space after opcode, comma+space after operands
        opcode, _, operands = line.partition('\t')[0].partition(' ')
        ret.setdefault(opcode.lower(), []).append([] if operands == '' else [o.lower().strip() for o in operands.split(',')])

    # If the opcode is 'nop', then add the 'any' string as a specification to match to any combination of operands
    ret['nop'].append(['any'])

    # Insert the 'unknown' instruction for when rose can't parse it. Shouldn't have any operands
    ret['unknown'] = [[]]
    
    return ret


# The entire x86_64 ISA. Each key is an opcode, and each value is a list of expected operand types
# Table was copied from: https://shell-storm.org/x86doc/
# But, I did have to add a few instructions to it
X86_64_ISA = _read_instruction_set(os.path.join(os.path.dirname(__file__), 'isa', 'x86_64.txt'))

# Dictionary mapping instruction prefix tokens to their allowed following opcodes
_REPENEZ_LIST = ['cmps', 'cmpsb', 'cmpsd', 'cmpsw', 'scas', 'scasb', 'scasd', 'scasw']
INSTRUCTION_PREFIX_ALLOWED_OPCODES = {
    # Everything I can find online says that lock must be used with only a subset of the instructions, but more and more
    #   CFG's I build have it being used with other ones, so I'll just assume it can be used with any of them
    # The ones that online says it can only be used with: ['adc', 'add', 'and', 'btc', 'btr', 'bts', 'cmpxchg', 
    #   'cmpxchg8b', 'cmpxchg16b', 'dec', 'inc', 'neg', 'not', 'or', 'sbb', 'sub', 'xadd', 'xchg', 'xor']
    # Extra ones I found before I stopped looking: ['cmp', 'retf', 'mov', 'pop', 'jmp', 'seta', 'lea']
    'lock': set(X86_64_ISA.keys()),
    'rep': ['ins', 'insb', 'insw', 'insd', 'insq',
            'lods', 'lodsb', 'lodsw', 'lodsd', 'lodsq',
            'movs', 'movsb', 'movsw', 'movsd', 'movsq',
            'outs', 'outsb', 'outsw', 'outsd', 'outsq',
            'stos', 'stosb', 'stosw', 'stosd', 'stosq'],
    'repe': _REPENEZ_LIST,
    'repz': _REPENEZ_LIST,
    'repne': _REPENEZ_LIST,
    'repnz': _REPENEZ_LIST,
}

# Set of registers allowed for use in memory access base/index registers
MEMORY_ACCESS_GP_REGISTERS = {'rax', 'eax', 'ax', 'al', 'rdi', 'edi', 'di', 'dil', 'rsi', 'esi', 'si', 'sil', 'rdx', 
                              'edx', 'dx', 'dl', 'rcx', 'ecx', 'cx', 'cl', 'rsp', 'esp', 'sp', 'spl', 'rbx', 'ebx', 
                              'bx', 'bl', 'rbp', 'ebp', 'bp', 'bpl', 'r8', 'r8d', 'r8w', 'r8b', 'r9', 'r9d', 'r9w', 
                              'r9b', 'r10', 'r10d', 'r10w', 'r10b', 'r11', 'r11d', 'r11w', 'r11b', 'r12', 'r12d', 
                              'r12w', 'r12b', 'r13', 'r13d', 'r13w', 'r13b', 'r14', 'r14d', 'r14w', 'r14b', 'r15', 
                              'r15d', 'r15w', 'r15b'}

# For the 64-bit displacement that can be used in some special memory accesses
DISPLACEMENT_64_OPCODES = ['mov', 'movabs']
DISPLACEMENT_64_REGISTERS = ['rax', 'eax', 'ax', 'al']

# Converts token types to a single character for regex checking of valid token combinations
TOKEN_TO_REGEX_CHAR = {
    Tokens.INSTRUCTION_ADDRESS: 'a',
    Tokens.NEWLINE: '|',
    Tokens.ROSE_INFO: '',
    Tokens.SPACING: '',
    Tokens.INSTRUCTION_START: 'x',
    Tokens.PTR: 'P',
    Tokens.OPEN_BRACKET: '[',
    Tokens.CLOSE_BRACKET: ']',
    Tokens.PLUS_SIGN: '+',
    Tokens.TIMES_SIGN: '*',
    Tokens.INSTRUCTION_PREFIX: '_',
    Tokens.SEGMENT_ADDRESS: 'S',
    Tokens.IMMEDIATE: 'i',
    Tokens.SEGMENT: 's',
    Tokens.MEMORY_SIZE: 'm',
    Tokens.REGISTER: 'r',
    Tokens.BRANCH_PREDICTION: 'b',
    Tokens.OPCODE: 'o', 
    Tokens.MISMATCH: '%',
}


# Gives each possible memory access type a name and info on what values registers/immediates within them can take
# Also allow for commutativity
INSTRUCTION_MEMORY_ACCESS_TYPES_DICT = {
    'ma1':          (r'i',              {'i1': '64'}),          # [D]
    'ma2and9':      (r'r',              {'r1': 'r'}),           # [B] and [P]
    'ma3':          (r'r[+]r',          {}),                    # [B + I]
    'ma4and10':     (r'r[+]i',          {'r1': 'r'}),           # [B + D] and [P + D]
    'ma4and10r':    (r'i[+]r',          {'r1': 'r'}),           # [B + D] and [P + D], reversed
    'ma5':          (r'r[+]r[+]i',      {}),                    # [B + I + D]
    'ma6':          (r'r[+]r[*]i',      {'i1': 's'}),           # [B + I*S]
    'ma6r':         (r'r[*]i[+]r',      {'i1': 's'}),           # [I*S + B]
    'ma7':          (r'r[*]i[+]i',      {'i1': 's'}),           # [I*S + D]
    'ma7r':         (r'i[+]r[*]i',      {'i2': 's'}),           # [D + I*S], reversed
    'ma8':          (r'r[+]r[*]i[+]i',  {'i1': 's'}),           # [B + I*S + D]
}

# Regular expressions for converted token lists
INSTRUCTION_MA_TYPES = r'm?P?s?\[(?:' + '|'.join(['(?P<%s>%s)' % (k, v) for k, (v, _) in INSTRUCTION_MEMORY_ACCESS_TYPES_DICT.items()]) + r')\]'
INSTRUCTION_MEMORY_EXPRESSION = r'(?:m?P?s?\[(?P<memexpr_val>[^]]*)\])'
INSTRUCTION_OPERAND = r'(?:(?P<register>r)|(?P<segment_addr>S)|(?P<immediate>i)|(?P<mem_expr>{mem_expr}))'.format(mem_expr=INSTRUCTION_MEMORY_EXPRESSION)
INSTRUCTION_REGEX = r'x?(?P<addr>a)?(?P<prefix>_+)?(?P<bp>b)?(?P<opcode>o)(?P<operands>{operand}*)'.format(operand=INSTRUCTION_OPERAND)

