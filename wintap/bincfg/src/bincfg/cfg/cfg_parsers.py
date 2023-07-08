"""
Functions to parse cfg inputs into ``CFG`` objects.
"""

import re
import html
import sys
import bincfg
import copy
from .cfg_function import CFGFunction
from .cfg_basic_block import CFGBasicBlock, RE_RETURN_INSTRUCTION
from .cfg_edge import CFGEdge, EdgeType
from ..utils import get_address, get_module


DIGRAPH_START_STRINGS = ['digraph', 'graph', 'node', 'edge']

# Regex matches
FUNC_STR_MATCH = re.compile(r'label=".*\\""')
FUNC_STR_MATCH_NO_NAME = re.compile(r'label="function 0x[0-9a-fA-F]*"')
ASM_LINE_MATCH = re.compile(r'label=<.*/>>')
FUNCTIONLESS_GV_BLOCK = re.compile(r'V_0x[0-9a-fA-F]* \[ .*')

# List of strings that are names for 'indeterminate' nodes
INDETERMINATE = ['indeterminate', 'nonexisting']

# Map a rose name to its in/out edge type ints
ROSE_EDGE_TYPES = {
    'call': EdgeType.FUNCTION_CALL,
    'cret': EdgeType.NORMAL,
    'cret\\nassumed': EdgeType.NORMAL,
    '': EdgeType.NORMAL,
    'other': EdgeType.NORMAL
}

# Regular expressions that denote an external function
EXTERN_FUNC_NAME_REGEXS = [re.compile(r'.*@plt'), re.compile(r'.*@.*[.]dll')]


def parse_cfg_data(cfg, data):
    """Parses the incoming cfg data. Infers type of data

    Args:
        cfg (CFG): the cfg to parse into
        data (Union[str, Sequence[str], TextIO, pd.DataFrame]): the data to parse, can be:

            - string: either string with newline characters that will be split on all newlines and treated as either a
              text or graphviz rose input, or a string with no newline characters that will be treated as a filename.
              Filenames will be opened as ghidra parquet files if they end with either '.pq' or '.parquet', and
              text/graphviz rose input otherwise
            - Sequence of string: will be treated as already-read-in text/graphviz rose input
            - open file object: will be read in using `.readlines`, then treated as text/graphviz rose input
            - pandas dataframe: will be parsed as ghidra parquet file

    Raises:
        ValueError: bad ``str`` filename, or an unknown file start string
        TypeError: bad ``data`` input type
        CFGParseError: if there is an error during CFG parsing (but data type was inferred correctly)
    """
    if isinstance(data, str):

        # Check for the empty string, and initialize empty
        if data == '':
            return
            
        # Check for single string to split on newlines
        if '\n' in data:
            data = [l.strip() for l in data.split('\n') if l.strip()]

        # Otherwise, assume it is a file
        else:
            cfg.metadata['filepath'] = data
            
            # Check for a parquet file
            if data.endswith(('.pq', '.parquet')):
                func = parse_ghidra_parquet

            # Otherwise, assume it is a text file
            else:
                try:
                    with open(data, 'r') as f:
                        data = [l.strip() for l in f.readlines() if l.strip()]
                except:
                    raise ValueError("Data was assumed to be a filename, but that file could not be opened/read!: %s" % repr(data))

    # Check for a pandas dataframe
    elif get_module('pandas', raise_err=False) and isinstance(data, sys.modules['pandas'].DataFrame):
        func = parse_ghidra_parquet

    # Check for an open file
    elif hasattr(data, 'readlines') and callable(data.readlines):
        data = [l.strip() for l in data.readlines() if l.strip()]

    # Check for a copy constructor
    elif isinstance(data, bincfg.CFG):
        cfg.add_function(*[
            CFGFunction(address=func.address, name=func.name, is_extern_func=func._is_extern_function, blocks=[
                CFGBasicBlock(
                    address=block.address, 
                    edges_in=[(e.from_block.address, e.to_block.address, e.edge_type) for e in block.edges_in],
                    edges_out=[(e.from_block.address, e.to_block.address, e.edge_type) for e in block.edges_out],
                    labels=copy.deepcopy(block.labels),
                    asm_lines=copy.deepcopy(block.asm_lines),
                ) for block in func.blocks
            ]) for func in data.functions_dict.values()
        ])

        # Also need to copy the metadata
        cfg.metadata = copy.deepcopy(data.metadata)
        return

    # Check for a networkx to read in
    elif get_module('networkx', raise_err=False) and isinstance(data, (sys.modules['networkx'].DiGraph)):
        bincfg.CFG.from_networkx(data, cfg)
        return
    
    # Check for a dictionary to read in
    elif isinstance(data, dict):
        bincfg.CFG.from_cfg_dict(data, cfg)
        return

    # Otherwise, assume it is a sequence of string lines
    else:
        try:
            data = [l.strip() for l in data if l.strip()]
        except:
            raise TypeError("Could not parse CFG data from data of type: '%s'" % type(data).__name__)
    
    # If data is a list right now, assume we need to get the function from a list of lines
    if isinstance(data, list):
        func = _get_parse_func_from_lines(data)
    
    func(cfg, data)


def _get_parse_func_from_lines(lines):
    """Returns the function that should be used to parse this list of lines.

    Assumes all empty lines have already been stripped/removed

    Args:
        lines (Sequence[str]): the list of lines

    Returns:
        Callable[[CFG, Any], None]: the function to use to parse
    """

    # If lines is empty, return a function that does nothing
    if len(lines) == 0:
        return lambda *args, **kwargs: None
    
    # Otherwise check for different start lines
    else:
        if lines[0].startswith('digraph'):
            return parse_rose_gv
        elif lines[0].startswith('function'):
            return parse_rose_txt
        else:
            raise ValueError("Unknown file start string, could not infer file type!:\n%s\n..." % repr(lines[0][:100]))


##################
# Rose Text File #
##################


def parse_rose_txt(cfg, lines):
    """Reads input as a .txt file

    Args:
        cfg (CFG): an empty/loading CFG() object
        lines (str, Iterable[str], TextIO): the data to parse. Can be a string (which will be split on newlines to get each
            individual line), a list of string (each element will be considered one line), or an open file to call
            `.readlines()` on

    Raises:
        CFGParseError: when file does not fit expected format
    """
    if isinstance(lines, str):
        lines = lines.split('\n')
    elif hasattr(lines, 'readlines') and callable(lines, 'readlines'):
        lines = lines.readlines()

    try:
        # Clean up the lines a bit
        lines = [l.strip() for l in lines if l.strip()]
    except:
        raise TypeError("Could not parse rose txt input of type: '%s'" % type(lines).__name__)

    cfg.metadata['file_type'] = 'txt'
    
    # Make the dictionary of the current blocks
    curr_blocks = {}
    funcs = []
    
    # Go through lines finding each function
    curr_func_lines = [lines[0]]
    for line in lines[1:]:
        
        # Make the next function with the current list of lines
        if line.startswith('function 0x'):
            funcs.append(_parse_txt_function(cfg, curr_func_lines, curr_blocks))
            curr_func_lines = [line]
        else:
            curr_func_lines.append(line)
    
    funcs.append(_parse_txt_function(cfg, curr_func_lines, curr_blocks))
    cfg.add_function(*funcs)


def _parse_txt_function(cfg, func_lines, curr_blocks):
    """Parses the function lines from a rose txt file into a ``CFGFunction``, and returns the function

    Args:
        cfg (CFG): the ``CFG`` to which this function would belong
        func_lines (List[str]): list of string lines from file to parse for this function
        curr_blocks (Dict[int, CFGBasicBlock]): a dictionary mapping basic block addresses to ``CFGBasicBlock`` objects. 
            We need this to create new basic blocks on the fly in order to make ``CFGEdge``'s work properly

    Returns:
        CFGFunction: the cfg function
    """
    # Create the CFGFunction() object with its parent_cfg, name (while removing quotes from rose text), and is_extern_func
    _, address, *func_name_lines = func_lines[0].split(" ")
    name = ''.join(func_name_lines)[1:-1] if func_name_lines else None
    func = CFGFunction(parent_cfg=cfg, address=get_address(address), name=name, is_extern_func=_is_extern_func_name(name))

    # Build up every basic block
    curr_block_lines = [func_lines[1]]
    for line in func_lines[2:]:
        # Make the next block
        if line.startswith("B"):
            # Check if this is the first basic block and has the same starting address as the function
            _parse_txt_block(func, curr_block_lines, curr_blocks)
            curr_block_lines = [line]
        else:
            curr_block_lines.append(line)
    
    # Add in final block
    _parse_txt_block(func, curr_block_lines, curr_blocks)

    return func


def _parse_txt_block(func, block_lines, curr_blocks):
    """Parses the incoming block lines from a rose text file, and appends it to func's blocks

    Args:
        func (CFGFunction): the function to which this basic block belongs
        block_lines (List[str]): list of string block lines to build this block from
        curr_blocks (Dict[int, CFGBasicBlock]): a dictionary mapping basic block addresses to ``CFGBasicBlock`` objects. 
            We need this to create new basic blocks on the fly in order to make ``CFGEdge``'s work properly

    Raises:
        ValueError: on an unknown edge line
    """

    # Parse the block name and address, and check if it is a function entry point
    *_, block_address = block_lines[0].rpartition(" ")
    address = func.address if block_address[0] == 'p' else block_address[:-1] if block_address[-1] == ':' else block_address

    block = _create_basic_block(curr_blocks, address=get_address(address), parent_function=func)
    
    # If block is None, then this block already exists in another function, no need to recreate it
    if block is None:
        return

    for line in block_lines[1:]:

        # If this line is to tell us that this is a function return block "block is a function return/call"
        if line[0] == 'b':
            continue
        
        # This is an assembly line. Add the memory address and string line as a tuple
        # IMPORTANT: do this before the " edge " detection in case of string literals in rose <> info
        elif line.startswith("0x"):
            address, _, asm_line = line.partition(": ")
            block.asm_lines.append((get_address(address), asm_line.strip()))
        
        # Currently just ignoring the 'also_owned_by' for now
        elif line[0] == 'a':
            #owned_by = line[23:].partition(" ")[0]
            #block.also_owned_by.add(int(owned_by, 16))
            pass
        
        # Otherwise this must be an edge line
        else:
            # Check for "function entry point", then lines using function names in quotes, then just normal address
            edge_addr = block.parent_function.address if line[-1] == 't' else \
                line.rpartition(' "')[0].rpartition(' ')[-1] if line[-1] == '"' else line.rpartition(' ')[-1]
            
            # edge_addr might already be an int from it's parent address
            if not isinstance(edge_addr, int):
                # Check for indeterminate/nonexistant edges. We ignore these, but check to see if this is a function return
                if edge_addr[0] != '0':
                    if line[0] == 'f' and line[9] == 'r':  # Check for 'function return edge to indeterminate'
                        assert RE_RETURN_INSTRUCTION.fullmatch(block.asm_lines[-1][1]), block.asm_lines
                    continue

                # Convert edge_addr to int
                edge_addr = int(edge_addr, 16)
            
            # Check for lines like "function call edge from/to", and "function return edge to" 
            if line[0] == 'f':
                if line[9] == 'c':
                    if line[19] == 't':
                        block.edges_out.add(CFGEdge(block, _create_basic_block(curr_blocks, edge_addr), 
                            edge_type=EdgeType.FUNCTION_CALL))
                else:
                    assert RE_RETURN_INSTRUCTION.fullmatch(block.asm_lines[-1][1])
            
            # Check for "call return edge to" or "normal edge to"
            elif (line[0] == 'c' and line[17] == 't') or (line[0] == 'n' and line[12] == 't'):
                block.edges_out.add(CFGEdge(block, _create_basic_block(curr_blocks, edge_addr), edge_type=EdgeType.NORMAL))
            
            # Check to make sure this line is an 'edge from' line. Otherwise this is an unknown line, raise an error
            elif 'edge from' not in line:
                raise ValueError("Unknown edge line: %s" % repr(line))
    
    func.blocks.append(block)


def _create_basic_block(curr_blocks, address, **kwargs):
    """Checks if there is a basic block with the given address in `curr_blocks`, and if not, creates it. Returns the block

    If the block does exist, then any kwargs in ``kwargs`` will be updated in the CFGBasicBlock, unless that block already
    has a parent_func in which case None will be returned and no blocks will be updated

    Args:
        curr_blocks (Dict[int, CFGBasicBlock]): curr_blocks: a dictionary mapping basic block addresses to 
            ``CFGBasicBlock`` objects. We need this to create new basic blocks on the fly in order to make 
            ``CFGEdge``'s work properly
        address (int): the integer memory address of the new basic block
        kwargs (Any): extra kwargs to pass to ``CFGBasicBlock`` object creation, or to update an already existing 
            CFGBasicBlock

    Raises:
        ValueError: _description_

    Returns:
        CFGBasicBlock: _description_
    """
    if address not in curr_blocks:
        curr_blocks[address] = CFGBasicBlock(address=address, **kwargs)
    elif len(kwargs) == 0:
        return curr_blocks[address]
    elif curr_blocks[address].parent_function is not None:
        return None
    else:
        for k, v in kwargs.items():
            if k in ['parent_function', 'edges_in', 'edges_out', 'asm_lines', 'labels']:
                setattr(curr_blocks[address], k, v)
            else:
                raise ValueError("Unknown basic block kwarg: %s" % repr(k))
    return curr_blocks[address]


######################
# Rose Graphviz File #
######################


def parse_rose_gv(cfg, lines):
    """Reads input as a graphviz file

    Args:
        cfg (CFG): an empty/loading CFG() object
        lines (str, Iterable[str], TextIO): the data to parse. Can be a string (which will be split on newlines to get each
            individual line), a list of string (each element will be considered one line), or an open file to call
            `.readlines()` on

    Raises:
        CFGParseError: when the file cannot be parsed correctly
    """
    if isinstance(lines, str):
        lines = lines.split('\n')
    elif hasattr(lines, 'readlines') and callable(lines, 'readlines'):
        lines = lines.readlines()

    try:
        # Clean up the lines a bit
        lines = [l.strip() for l in lines if l.strip()]
    except:
        raise TypeError("Could not parse rose graphviz input of type: '%s'" % type(lines).__name__)

    cfg.metadata['file_type'] = 'gv'

    subgraphs = []
    edges = {}
    curr_blocks = {}
    
    # Keeping track of states
    in_subgraph = False
    eof = False

    for line in lines:
        # Check for empty string, for beginning digraph strings to ignore, and indeterminate/nonexisting nodes
        if line == '' or any(line.startswith(s) for s in DIGRAPH_START_STRINGS + INDETERMINATE):
            continue
        
        # Check for subgraph cluster
        elif line[0] == 's':
            in_subgraph = True

            # Get the function string
            func_str_matches = FUNC_STR_MATCH.findall(line)

            # Check for functions with no name
            if len(func_str_matches) == 0:
                func_str_matches = FUNC_STR_MATCH_NO_NAME.findall(line)
            
            # Otherwise continue normally
            if len(func_str_matches) != 1:
                raise CFGParseError("Could not parse function string from: %s\n Found matches: %s" % (repr(line), func_str_matches))

            # Add a new subgraph to the list (getting the [7:-1] works in both named and unnamed cases)
            # The func_str should be something like 'function [MEMORY_ADDRESS] "[FUNCTION_NAME]"' or 'function [MEMORY_ADDRESS]'
            func_str = func_str_matches[0][7:-1]
            _, func_address, *func_name = func_str.split(' ')
            func_name = ' '.join(func_name)[2:-2] if func_name else None

            subgraphs.append((func_name, int(func_address, 0), []))
        
        # Check for end of subgraph cluster/eof
        elif line[0] == '}':
            # Check to make sure there is only one eof '}' line
            if not in_subgraph:
                if not eof:
                    eof = True
                else:
                    raise CFGParseError("Found multiple lines starting with '}' that did not end subgraphs")

            in_subgraph = False
        
        # Check for nodes/node edges
        elif line[0] == 'V':
            
            # Handle subgraph node, or if it is a block with no parent function
            if in_subgraph or FUNCTIONLESS_GV_BLOCK.fullmatch(line) is not None:

                # Get the node address
                address, _, rest = line.partition(" [ ")
                address = int(address[2:], 0)
                
                # Get the asm line string
                asm_line_match = ASM_LINE_MATCH.findall(rest)

                # Need to leave the first and last <>
                asm_line = asm_line_match[0][6:] if len(asm_line_match) > 0 else ''

                # The tuple for this current node
                node_tup = (address, asm_line)

                # Add this node to our current subgraph if we are in one
                if in_subgraph:
                    subgraphs[-1][2].append(node_tup)
                
                # Otherwise, we are parsing a functionless basic block, create a dummy function to wrap it
                else:
                    subgraphs.append(("__DUMMY_FUNCTION_AT_0x%x__" % address, address, [node_tup]))

            # Handle edge
            else:
                # Get the source and destination names
                source, rest = [a.strip() for a in line.split('->')]
                dest, rest = [a.strip() for a in rest.split(' [ ')]

                # Get the label name by splitting on quotes and getting first index, checking for empty string as well
                label = "" if 'label=""' in rest else rest.split('"')[1]

                # Don't deal with indeterminate edges, unless they are a function return, then send that info
                if dest in INDETERMINATE:
                    continue

                source, dest = int(source[2:], 0), int(dest[2:], 0)

                # Add the edge into the dictionary for the outgoing edge
                edges.setdefault(source, []).append((ROSE_EDGE_TYPES[label], dest))

        # Otherwise, raise error
        else:
            raise CFGParseError("Unknown line: '%s'" % line)
    
    funcs = [_parse_gv_function(cfg, name, address, nodes, edges, curr_blocks) for name, address, nodes in subgraphs]
    cfg.add_function(*funcs)


def _parse_gv_function(cfg, name, address, nodes, edges, curr_blocks):
    """Parses the func_info as a graphviz dot file, returns the function

    Args:
        cfg (CFG): the ``CFG`` to which this function belongs
        name (Union[str, None]): the function name, or None if it doesn't have one
        address (int): the integer address of this function
        nodes (Iterable[Tuple[int, str]]): an iterable of nodes in this subgraph. Each 'node' should be a tuple of 
            (node_address: int, node_asm_lines: str), with the 'node_asm_lines' being the unprocessed string from the 
            graphviz file
        edges (Dict[int, List[Tuple[EdgeType, int]]]): a dictionary of all edges in the cfg. Each key should be a 'from'
            basic block integer address, and values are tuples of outgoing edge information for the block with that 
            address. Each edge information is a tuple of (edge_type: EdgeType, to_address: int)
        curr_blocks (Dict[int, CFGBasicBlock]): a dictionary mapping basic block addresses to ``CFGBasicBlock`` objects. 
            We need this to create new basic blocks on the fly in order to make ``CFGEdge``'s work properly

    Returns:
        CFGFunction: the cfg function
    """
    func = CFGFunction(parent_cfg=cfg, address=get_address(address), name=name, is_extern_func=_is_extern_func_name(name))
    for address, asm_lines in nodes:
        _parse_gv_block(func, address, asm_lines, edges.get(address, []), curr_blocks)
    
    return func


def _parse_gv_block(func, address, asm_lines, node_edges, curr_blocks):
    """Parses the incoming block info assuming it is from a graphviz dot file, and appends it to func's blocks

    Args:
        func (CFGFunction): the ``CFGFunction`` this block belongs to
        address (int): integer memory address of the node
        asm_lines (str): the UNPARSED asm lines from the raw gv dot file
        node_edges (Iterable[Tuple[EdgeType, int]]): an iterable of information for all outgoing edges for this block. 
            Each element should be a tuple of (edge_type: EdgeType, to_address: int)
        curr_blocks (Dict[int, CFGBasicBlock]): a dictionary mapping basic block addresses to ``CFGBasicBlock`` objects. 
            We need this to create new basic blocks on the fly in order to make ``CFGEdge``'s work properly
    """
    # Get the CFGBasicBlock with this address
    block = _create_basic_block(curr_blocks, address, parent_function=func, asm_lines=get_asm_from_node_label(asm_lines))

    # Parse out the edges
    for edge_type, address in node_edges:
        block.edges_out.add(CFGEdge(block, _create_basic_block(curr_blocks, address), edge_type))
    
    func.blocks.append(block)


GV_SPLIT = re.compile(r'<br [^>]*/>')
def get_asm_from_node_label(label):
    """Converts a node's label into a list of assembly lines at that basic block.

    Args:
        label (str): the unparsed string label

    Returns:
        List[Tuple[int, str]]: a list of 2-tuples of (memory_address, asm_instruction)
    """
    if label == '' or label is None:
        return []

    # Remove the first and last <>, replace all "??" with empty string, and html-unescape the ampersand encoded things
    ret = [('0x' + html.unescape(l.replace("??", ""))) for l in GV_SPLIT.split(label[1:-1]) if l != ""]

    # Split on spaces and get the first one to get the memory address, the rest are joined to be the instruction
    return [(int(addr, 0), line.strip()) for r in ret for addr, _, line in [r.replace('\t', '').partition(' ')]]


def _is_extern_func_name(name):
    """Returns True if name is an external function name, False otherwise
    
    Args:
        name (Union[str, None]): the name
    
    Returns:
        bool: True if name is an external function name, False otherwise
    """
    return name is not None and any(s.fullmatch(name) is not None for s in EXTERN_FUNC_NAME_REGEXS)


class CFGParseError(Exception):
    pass
