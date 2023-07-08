"""Constants and regular expressions involving tokenization of assembly lines"""

from enum import Enum


# Extra information given by rose
RE_ROSE_INFO = r'<[^>]*>'

# Spacing inbetween opcodes/operands. Empty spacing, either space ' ', comma ',', or tab '\t' some number of times
RE_SPACING = r'[, \t]+'

# Both pipes '|' like in ghidra output and literal '\n' newline characters
RE_NEWLINE = r'[|\n]'

# The delimiter lookahead. Allows for lookahead to make sure there is some delimiter, and we don't greedily grab 
#   registers/values/etc in case their names interfere with anything else.
# Can be a spacing, newline, or end of string
RE_DELIMITER = r'{spacing}|{newline}|{rose_info}|$'.format(spacing=RE_SPACING, newline=RE_NEWLINE, rose_info=RE_ROSE_INFO)

# Dictionary mapping all x86_64 registers to their sizes in bytes
X86_REG_SIZES = {
    k:v for a, v in [
        (['xmm%d' % i for i in range(16)], 16),                             # XMM 16-byte registers
        (['ymm%d' % i for i in range(16)], 32),                             # YMM 32-byte registers
        (['zmm%d' % i for i in range(32)], 64),                             # ZMM 64-byte registers

        (['st%d' % i for i in range(16)], 10),                              # ST 10-byte registers (rose interpretation)
        (['st(%d)' % i for i in range(16)], 10),                          # ST 10-byte registers (ghidra interpretation)
        (['mm%d' % i for i in range(8)], 8),                                # MM 8-byte registers [lower 64 bits of ST]
        
        (['cw', 'fp_ip', 'fp_dp', 'fp_cs', 'sw', 'tw', 
            'fp_ds', 'fp_opc', 'cs', 'ss', 'ds', 'es', 'fs', 
            'gs', 'gdtr', 'idtr', 'ldtr', 'tr', 'msw'], 2),                 # Various special 2-byte registers
        (['fp_dp', 'fp_ip', 'mxcsr'], 4),                                   # Various special 4-byte registers

        (['%sr%d' % (k, v) for k in ['c', 'd'] for v in range(16)], 8),     # DR(0-15)/CR(0-15) 8-byte registers

        (['r%d' % v for v in range(8, 16)], 8),                             # R(8-15)  8-byte registers
        (['r%dd' % v for v in range(8, 16)], 4),                            # R(8-15)d 4-byte registers
        (['r%dw' % v for v in range(8, 16)], 2),                            # R(8-15)w 2-byte registers
        (['r%db' % v for v in range(8, 16)], 1),                            # R(8-15)b 1-byte registers

        (['rax', 'rbx', 'rcx', 'rdx'], 8),                                  # Named general 8-byte r-registers
        (['eax', 'ebx', 'ecx', 'edx'], 4),                                  # Named general 4-byte r-registers
        (['ax', 'bx', 'cx', 'dx'], 2),                                      # Named general 2-byte r-registers

        (['rflags', 'rip', 'rbp', 'rsi', 'rdi', 'rsp'], 8),                 # Various special 8-byte r-registers
        (['eflags', 'eip', 'ebp', 'esi', 'edi', 'esp'], 4),                 # Various special 4-byte e-registers
        (['flags', 'ip', 'bp', 'si', 'di', 'sp'], 2),                       # Various special 2-byte registers

        (['bpl', 'sil', 'dil', 'spl'], 1),                                  # Various special 1-byte registers
        (['%s%s' % (a, b) for a in 'abcd' for b in 'lh'], 1),               # Various general 1-byte registers (low and high)
    ]
    for k in a
}

# Dictionary mapping all base memory sizes to their size in bytes
# NOTE: Should find someone who can confirm sizes here, because I get different values when googling what 'ldouble' is...
MEMORY_SIZES = {
    'byte': 1, 'word': 2, 'dword': 4, 'qword': 8, 'tword': 10, 'float': 4, 'tfloat': 10, 'double': 8, 'ldouble': 16,
    'xmmword': 16, 'ymmword': 32, 'zmmword': 64,
}

# All register sizes
REGISTER_SIZES = {k:v for d in [X86_REG_SIZES] for k, v in d.items()}

# Regex matches to registers. This is ~15% faster than brute-force matching all keys in REGISTER_SIZES
# Registers can be followed by a '*', ']', or delimiter
RE_X86_REG_RE_MATCH = r'[xyz]?mm[0-9]+|st\(?[0-9]+\)?|(?:[sb]p|[ds]i)l|[re]?(?:flags|ip|[bs]p|[sd]i|[abcd]x)|[cd]r[0-9]+|r[0-9]+[dwb]?|[abcd][lh]|[cst]w|fp_(?:[id]p|[cd]s|opc)|[cdefgs]s|(?:[gil]d)?tr|msw|mxcsr'
RE_ALL_REGISTERS = r'(?:{all_reg})(?=[*]|\]|{delim})'.format(all_reg='|'.join([RE_X86_REG_RE_MATCH]), delim=RE_DELIMITER)

# x86_64 instruction prefixes, stolen from: http://web.mit.edu/rhel-doc/3/rhel-as-en-3/i386-prefixes.html.
# These must be followed by either an underscore (which will be captured), or a delimiter
# We split these up into codes + needing delimiter for some postprocessing that is done in the tokenizers
RE_INSTRUCTION_PREFIX_CODES = r'(?:lock|rep(?:ne|nz|e|z)?)'
RE_INSTRUCTION_PREFIX = r'{codes}(?={delim})'.format(delim=RE_DELIMITER, codes=RE_INSTRUCTION_PREFIX_CODES)

# For branch prediction. Not used by re for tokenization, but used in postprocessing of token stream
RE_BRANCH_PREDICTION = r'p[tn]'

# Possible immediate values: hexadecimal, octal, decimal (NOTE: the need to be processed in that order so decimal immediate
#    doesn't take up the initial '0' in front of hexadecimal/octal)
RE_IMM_HEX = r'-?0x[0-9a-f]+'
RE_IMM_OCT = r'-?0o[0-7]+'
RE_IMM_BIN = r'-?0b[01]+'
RE_IMM_INT = r'-?[0-9]+'
RE_IMMEDIATE = r'(?:{imm_hex}|{imm_oct}|{imm_bin}|{imm_int})(?=\]|{delim})'.format(imm_hex=RE_IMM_HEX, imm_oct=RE_IMM_OCT, imm_int=RE_IMM_INT, imm_bin=RE_IMM_BIN, delim=RE_DELIMITER)

# Memory segment identifiers. Should have an open brackets right after (allowing for spacing inbetween)
RE_SEGMENT_REGISTERS = r'[cdefgs]s'
RE_SEGMENT = r'(?:{seg_reg}):(?=(?:{spacing})?\[)'.format(spacing=RE_SPACING, seg_reg=RE_SEGMENT_REGISTERS)

# A memory segment. Must use either "immediate:immediate" or "segment:reg" format.
RE_SEGMENT_ADDRESS = r'(?:{imm}):(?:{imm})|(?:{seg_reg}):(?:{reg})'.format(imm=RE_IMMEDIATE, segment=RE_SEGMENT, seg_reg=RE_SEGMENT_REGISTERS, reg=RE_ALL_REGISTERS)

# Known memory sizes. Includes a 'v%d' in front of all of them to denote a vector of multiple of these.
RE_MEM_SIZES = r'(?:v[0-9]+)?(?:byte|[dqt]?word|t?float|l?double|[xyz]mmword)(?={delim})'.format(delim=RE_DELIMITER)

# Check for any opcode mnemonic. For now, we just get any and all characters that could correspond to an opcode, 
#   instruction prefix, or branch prediction. These will be sorted out later when parsing. Must be followed by delimiter
RE_OPCODE = r'(?:[a-z][a-z0-9_.,]*)(?={delim})'.format(delim=RE_DELIMITER)


# Token type names
class Tokens:
    INSTRUCTION_ADDRESS = 'inst_addr'
    NEWLINE = 'newline'
    INSTRUCTION_START = 'inst_start'
    SPACING = 'spacing'
    PTR = 'ptr'
    ROSE_INFO = 'rose'
    OPEN_BRACKET = 'open_bracket'
    CLOSE_BRACKET = 'close_bracket'
    PLUS_SIGN = 'plus_sign'
    TIMES_SIGN = 'times_sign'
    INSTRUCTION_PREFIX = 'prefix'
    SEGMENT_ADDRESS = 'segment_addr'
    IMMEDIATE = 'immediate'
    SEGMENT = 'segment'
    REGISTER = 'register'
    MEMORY_SIZE = 'memory_size'
    BRANCH_PREDICTION = 'branch_prediction'
    OPCODE = 'opcode'
    MISMATCH = 'mismatch'

# Enum for different levels of tokenization
class TokenizationLevel(Enum):
    OPCODE = ['op', 'opcode', 'operand']
    INSTRUCTION = ['inst', 'instruction', 'line']
    AUTO = ['auto', 'automatic']

# Token to insert at the start of each instruction for opcode-level tokenization
INSTRUCTION_START_TOKEN = r'#start_instr#'

# Match names to tokens, and define the order in which they should be matched
DEFAULT_TOKENS = [
    # The instruction address. Should be at start of a line (use positive lookbehind to confirm, or match to start of line), 
    #   may contain a colon ':', and must be followed by a space ' '. Must always be in hexadecimal
    (Tokens.INSTRUCTION_ADDRESS, r'(?:(?<=\n|[|])|^)0x[0-9a-f]*:?(?= )'),
    (Tokens.NEWLINE, RE_NEWLINE),  # Both pipes '|' like in ghidra output and literal '\n' newline characters
    (Tokens.ROSE_INFO, RE_ROSE_INFO),  # Match to rose information in <brackets>. Doesn't actually need to be this high since all other regex matches couldn't match to this, but who cares
    (Tokens.INSTRUCTION_START, INSTRUCTION_START_TOKEN),  # Only appears if one is trying to tokenize base-normalized instructions
    (Tokens.SPACING, RE_SPACING),  # Empty spacing, either space ' ', comma ',', or tab '\t' some number of times
    (Tokens.PTR, r'ptr'),  # The literal string 'ptr'. Needs to be before the opcode check
    (Tokens.OPEN_BRACKET, r'\['),  # Open brackets for memory address info
    (Tokens.CLOSE_BRACKET, r'\]'),  # Close brackets for memory address info
    (Tokens.PLUS_SIGN, r'\+'),  # Plus's inside memory address info
    (Tokens.TIMES_SIGN, r'\*'),  # Times inside memory address info
    (Tokens.INSTRUCTION_PREFIX, RE_INSTRUCTION_PREFIX),  # Instructions prefixes (EG: lock, rep, etc.), possible underscore after them
    (Tokens.SEGMENT_ADDRESS, RE_SEGMENT_ADDRESS),  # A far jump-like segment addressing. Must be before IMMEDIATE
    (Tokens.IMMEDIATE, RE_IMMEDIATE),  # An immediate value: hexadecimal, decimal, octal, binary
    (Tokens.SEGMENT, RE_SEGMENT),  # Memory segment info. Must go before reg
    (Tokens.MEMORY_SIZE, RE_MEM_SIZES),  # Known memory sizes. Must go before reg/opcode
    (Tokens.REGISTER, RE_ALL_REGISTERS),  # All known registers accross all architectures
    (Tokens.OPCODE, RE_OPCODE),  # Opcode. Should go last since it's the most general matching. 
    (Tokens.MISMATCH, r'.'),  # Finally, match to any character to notify that there has been a mismatch
]
"""Default list of (token_name, regex) token tuples to match to"""


ROSE_REPLACE_STR = '__STR__'


class ARCH(Enum):
    X86_64 = 'x86_64'
    X86 = 'x86'


class TokenMismatchError(Exception):
    pass
