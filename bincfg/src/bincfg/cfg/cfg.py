import pickle
import sys
import numpy as np
import math
from collections import Counter, namedtuple
from .cfg_parsers import parse_cfg_data
from .cfg_function import CFGFunction
from .cfg_edge import CFGEdge, EdgeType
from .cfg_basic_block import CFGBasicBlock
from ..utils import get_address, eq_obj, hash_obj, AtomicTokenDict, get_module
from ..normalization import normalize_cfg_data, TokenizationLevel
from ..normalization.base_normalizer import _Pickled_Normalizer
from ..labeling.parse_cfg_labels import parse_node_labels
from ..labeling.node_labels import NODE_LABELS_INT_TO_STR, NODE_LABELS_STR_TO_INT


# Info for the get_compressed_stats() method of cfg's
GRAPH_LEVEL_STATS_FUNCS = [
    lambda cfg: cfg.num_blocks,
    lambda cfg: cfg.num_functions,
    lambda cfg: cfg.num_asm_lines,
]
GLS_DTYPE = np.uint32
NODE_SIZE_HIST_BINS = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 18, 20, 25, 30, 35, 40, 50, 60, 80, 100, 150, 200]
FUNCTION_DEGREE_HIST_BINS = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 18, 20, 25, 30, 35, 40, 50, 60, 80, 100, 150, 200]
FUNCTION_SIZE_HIST_BINS = [0, 1, 2, 3, 4, 6, 8, 10, 15, 20, 25, 30, 40, 60, 80, 100, 150, 200, 300, 400, 500]

# Extra bytes to pad the insertion of libraries into the CFG so we don't mess up other assembly instructions
_INSERTION_PADDING_BYTES = 16


class CFG:
    """A Control Flow Graph (CFG) representation of a binary

    Can currently load in from:

        * Rose binary analysis tool (both text and graphviz dotfile outputs)
    
    NOTE: 'indeterminate' blocks/calls/etc. are completely ignored
    
    
    Parameters
    ----------
    data: `Optional[Union[str, TextIO, Sequence[str], pd.DataFrame]]`
        the data to use to make this CFG. Data type will be inferred based on the data passed:

            * string: either string with newline characters that will be split on all newlines and treated as either a
              text or graphviz rose input, or a string with no newline characters that will be treated as a filename.
              Filenames will be opened as ghidra parquet files if they end with either '.pq' or '.parquet', and
              text/graphviz rose input otherwise
            * Sequence of string: will be treated as already-read-in text/graphviz rose input
            * open file object: will be read in using `.readlines`, then treated as text/graphviz rose input
            * pandas dataframe: will be parsed as ghidra parquet file
            * anything else: an error will be raised

    normalizer: `Optional[Union[str, Normalizer]]`
        the normalizer to use to force-renormalize the incoming CFG, or None to not normalize
    metadata: `Optional[dict]`
        a dictionary of metadata to add to this CFG
        NOTE: passed dictionary will be shallow copied
    """

    normalizer = None
    """The normalizer used to normalize assembly lines in this ``CFG``, or None if they have not been normalized"""

    metadata = None
    """Dictionary of metadata associated with this ``CFG``"""

    functions_dict = None
    """Dictionary mapping integer function addresses to their ``CFGFunction`` objects"""

    blocks_dict = None
    """Dictionary mapping integer basic block addresses to their ``CFGBasicBlock`` objects"""

    def __init__(self, data=None, normalizer=None, metadata=None):
        # These store functions/blocks while allowing for O(1) lookup by address
        self.functions_dict = {}
        self.blocks_dict = {}

        self.normalizer = None
        self.metadata = {} if metadata is None else metadata.copy()

        # If data is not None, parse it
        if data is not None:
            parse_cfg_data(self, data)
        
        # Prune any of the padding node
        self._prune_padding_nodes()

        # Check for node labels
        parse_node_labels(self)

        # Finally, normalize if needed
        if normalizer is not None:
            self.normalize(normalizer)
    
    def _prune_padding_nodes(self):
        """Prunes all of the padding nodes (nodes that only have NOP instructions for alignment)"""
        # Got to keep track of the blocks to remove since dictionary will change size
        blocks_to_remove = []

        for block in self.blocks_dict.values():
            if block.is_padding_node:

                # Make sure this block has at most one edge out since it should be all NOP's
                if len(block.edges_out) > 1:
                    raise ValueError("Found a padding node with > 1 edges out!")
                next_block = None if len(block.edges_out) == 0 else list(block.edges_out)[0].to_block

                # Re-edge all of this block's incomming edges to point to either the one outgoing edge, or none, then
                #   remove those edges
                for edge in block.edges_in:
                    if next_block is not None:
                        new_edge = CFGEdge(edge.from_block, next_block, edge.edge_type)
                        edge.from_block.edges_out.add(new_edge)
                        next_block.edges_in.add(new_edge)
                    edge.from_block.remove_edge(edge)

                for edge in block.edges_out:
                    edge.to_block.remove_edge(edge)
                
                # Remove the block from its function, checking for if it is a function entry
                func = block.parent_function
                if block.is_function_entry and next_block is not None and next_block.parent_function is func and func.address is not None:
                    # Remove then add the function from the function_dict with new address
                    del self.functions_dict[func.address]
                    func.address = next_block.address
                    self.functions_dict[func.address] = func
                func.blocks.remove(block)

                # Add the block to those to remove if its address is not None
                if block.address is not None:
                    blocks_to_remove.append(block)

        # Remove all of the padding nodes from the blocks_dict
        for block in blocks_to_remove:
            del self.blocks_dict[block.address]

    
    def get_function(self, address, raise_err=True):
        """Returns the function in this ``CFG`` with the given address

        Args:
            address (Union[str, int, Addressable]): a string/integer memory address, or an addressable object 
                (EG: CFGBasicBlock/CFGFunction)
            raise_err (bool, optional): if True, will raise an error if the function with the given memory address was 
                not found, otherwise will return None. Defaults to True.

        Raises:
            ValueError: if the function with the given address could not be found

        Returns:
            Union[CFGFunction, None]: the function with the given address, or None if that function does not exist
        """
        address = get_address(address)
        if address not in self.functions_dict and raise_err:
            raise ValueError("Could not find function with address: (decimal) %d, (hex) 0x%x" % (address, address))
        return self.functions_dict.get(address, None)
    
    def get_function_by_name(self, name, raise_err=True):
        """Returns the function in this ``CFG`` with the given name

        NOTE: if the name of the function is None, then the expected string name to this method would be:
        "__UNNAMED_FUNC_%d" % func.address

        Args:
            name (str): the name of the function to get
            raise_err (bool, optional): if True, will raise an error if the function with the given memory address was 
                not found, otherwise will return None. Defaults to True.

        Raises:
            ValueError: if the function with the given address could not be found

        Returns:
            Union[CFGFunction, None]: the function with the given address, or None if that function does not exist
        """
        for func in self.functions_dict.values():
            if func.nice_name == name:
                return func
        if raise_err:
            raise ValueError("Could not find function with name: %s" % repr(name))
        return None
    
    def get_block(self, address, raise_err=True):
        """Returns the basic block in this CFG with the given address

        Args:
            address (Union[str, int, Addressable]): a string/integer memory address, or an addressable object 
                (EG: CFGBasicBlock/CFGFunction)
            raise_err (bool, optional): if True, will raise an error if the basic block with the given memory address 
                was not found, otherwise will return None. Defaults to True.

        Raises:
            ValueError: if the basic block with the given address could not be found

        Returns:
            Union[CFGBasicBlock, None]: the basic block with the given address
        """
        address = get_address(address)
        if address not in self.blocks_dict and raise_err:
            raise ValueError("Could not find basic block with address: (decimal) %d, (hex) %x" % (address, address))
        return self.blocks_dict.get(address, None)
    
    def get_block_containing_address(self, address, raise_err=True):
        """Returns the basic block in this CFG that contains the given address at the start of one of its instructions

        This will lazily compute an instruction lookup dictionary mapping addresses to the blocks that contain them
        
        Args:
            address (Union[str, int, Addressable]): a string/integer memory address, or an addressable object 
                (EG: CFGBasicBlock/CFGFunction)
            raise_err (bool, optional): if True, will raise an error if the basic block with the given memory address 
                was not found, otherwise will return None. Defaults to True.

        Raises:
            ValueError: if the basic block containing the given address could not be found

        Returns:
            Union[CFGBasicBlock, None]: the basic block that contains the given address
        """
        address = get_address(address)

        # Check if we have created an instruction lookup yet or not
        if hasattr(self, '_inst_lookup'):
            if address in self._inst_lookup:
                return self._inst_lookup[address]
            elif raise_err:
                raise ValueError("Could not find basic block containing the address: (decimal) %d, (hex) %x" % (address, address))
            else:
                return None
        
        self._inst_lookup = {}

        for block in self.blocks:
            for block_addr in block.instruction_addresses:
                self._inst_lookup[block_addr] = block
        
        return self.get_block_containing_address(address, raise_err=raise_err)
    
    def add_function(self, *functions, override=False):
        """Adds the given function(s) to this cfg. This should only be done once the given function(s) have been fully initialized

        This will do some housekeeping things such as:

            * setting the parent_cfg and parent_function attributes of functions and blocks respectively
            * adding missing edges to their associated edges_out and edges_in
            * converting edges from (None/address, None/address, edge_type) tuples into CFGEdge() objects
            * adding from_block and to_block in new edges if missing

        Args:
            function (CFGFunction): arbitrary number of CFGFunction's to add
            override (bool): if False, an error will be raised if a function or basic block contains an address that
                already exists in this CFG. If True, then that error will not be raised and those functions/basic blocks
                will be overriden (which has unsupported behavior). Defaults to False.
        """
        for func in functions:
            # Check for bad function type, address being None, or function address already existing
            if not isinstance(func, CFGFunction):
                raise TypeError("Can only add function of type CFGFunction, not '%s'" % type(func).__name__)
            if func.address is None:
                raise ValueError("Function cannot have a None address when adding to CFG: %s" % func)
            if func.address in self.functions_dict:
                if not override:
                    raise ValueError("Function has address 0x%x which already exists in this CFG!" % func.address)
            
            func.parent_cfg = self
            func.name = self._check_func_name(func.name)

            self.functions_dict[get_address(func.address)] = func
            for block in func.blocks:
                # Check for bad basic blocks
                if block.address is None:
                    raise ValueError("Block cannot have a None address when adding to CFG: %s" % block)
                if block.address in self.blocks_dict:
                    if not override:
                        raise ValueError("Basic block has address 0x%x which already exists in this CFG!" % block.address)
                
                block.parent_function = func
                        
                self.blocks_dict[get_address(block.address)] = block
        self._update_blocks()
    
    def _check_func_name(self, name):
        """Checks if the given function name already exists, and slightly modifies it if so since name collisions cause problems
        
        Replaces all None names with _UNNAMED_FUNC_%d with %d being an integer index that counts up for each unnamed function
        Or, in the case of the first function with None name, it will be called "_UNNAMED_FUNC"

        Any names that already exist will have a '_%d' appended with %d being an integer

        Otherwise, just returns the name
        """
        name = '_UNNAMED_FUNC' if name is None else name

        if not hasattr(self, '_temp_func_names'):
            self._temp_func_names = set(f.name for f in self.functions_dict.values())
        
        if name in self._temp_func_names:
            idx = 1
            while name + '_%d' % idx in self._temp_func_names:
                idx += 1
            
            name = name + '_%d' % idx
        
        self._temp_func_names.add(name)
        return name
    
    def _update_blocks(self):
        """Updates basic blocks in this cfg

        Specifically, makes sure all the `.edges_out` and `.edges_in` are filled correctly for all basic blocks
        """
        # Check the edges out
        for block in self.blocks:
            block.edges_out = set((CFGEdge(block, e[1] if isinstance(e[1], CFGBasicBlock) else self.get_block(e[1]), e[2]) \
                                   if isinstance(e, tuple) else e) for e in block.edges_out)
            for edge in block.edges_out:
                edge.to_block.edges_in.add(edge)

        # Check the edges in
        for block in self.blocks:
            block.edges_in = set((CFGEdge(e[0] if isinstance(e[0], CFGBasicBlock) else self.get_block(e[0]), block, e[2]) \
                                  if isinstance(e, tuple) else e) for e in block.edges_in)
            for edge in block.edges_in:
                edge.from_block.edges_out.add(edge)

    def insert_library(self, cfg, function_mapping, offset=None):
        """Inserts the cfg of a shared library into this cfg

        This will modify the memory addresses of `cfg` (adding an appropriate offset), then add all of the functions and
        basic blocks from `cfg` into this cfg. Finally, external functions in this cfg that have implemented functions
        in the function_mapping will have normal edges added.

        NOTE: this assumes that no other libraries will be added later that depend on this one that is currently being
        added (otherwise, the external function edges might not be added properly). Make sure you add them in the
        correct order!
        
        Args:
            cfg (CFG): the cfg of the library to insert. It will be copied
            function_mappping (Dict[str, int]): dictionary mapping known exported function names to their addresses
                within `cfg`. While we can sometimes determine these mappings from function names in the new `cfg`,
                that is not always the case (EG: stripping function names from binaries, or compilers/linkers emitting
                aliases for the functions in `cfg`), hence why this parameter exists. If you don't wish to add in new
                normal edges, or if you wish to add them in manually, you can pass an empty dictionary
            offset (Optional[int]): if None, then the library will be inserted in the first available memory location.
                Otherwise this can be an integer memory address to insert the cfg at (this will raise an error if it
                can't fit there)
        """
        # Determine an acceptable offset. We can't just insert at the end or something since we may call this function
        #   multiple times, and binaries can do just about anything that may mess up hard-coded placements
        _min_max = lambda s: (min(s), max(s))

        # Find the size of `cfg` (just the needed memory locations, plus some padding)
        min_addr, max_addr = 2**64, 0
        for block in cfg.blocks:
            new_min, new_max = _min_max(block.asm_memory_addresses)
            min_addr = min(min_addr, new_min)
            max_addr = max(max_addr, new_max)
        cfg_size = max_addr - min_addr + _INSERTION_PADDING_BYTES * 2

        # Sort all min/max's of memory addresses for blocks in this cfg. Insert a 0 so we could insert in beginning
        addresses = np.sort([0] + [s for block in self.blocks for s in _min_max(block.asm_memory_addresses)] + [2 ** 32])

        # If the user didn't pass an offset, determine an appropriate one on our own
        if offset is None:

            # Compute all of the differences to get sizes (all negative or 0 since sorted), get every other one since we 
            #   couldn't place it inside a block
            diffs = np.diff(addresses)[::2]

            # Find the first spot in which we could place the new cfg, raise an error if we can't fit it. Get the original
            #   starting memory address of that location
            loc = np.argwhere(diffs >= cfg_size)
            if len(loc) == 0:
                raise ValueError("Could not find space to insert a library of size %d" % cfg_size)
            offset = addresses[loc[0][0] * 2] + _INSERTION_PADDING_BYTES
        
        # Otherwise, check that the offset the user passed works. It should in an available and large enough gap, and
        #   should be at least _INSERTION_PADDING_BYTES away from the nearest used memory address in this cfg
        else:
            idx = np.searchsorted(addresses, offset, side='left')

            # If the index is even, then it is within a block (note: addresses is always even length-ed, and the 
            #   searchsorted call will always return the index after the last used memory address). Otherwise if
            #   idx is within _INSERTION_PADDING_BYTES of the nearest block, then it is also bad
            # Another note: if the idx is 1, then it doesn't need the padding since it's already at the start of the memory,
            #   but it does need it after for possible instruction lengths
            if idx % 2 == 0 or idx >= len(addresses) or (1 <= idx \
                and (offset - addresses[idx - 1] < _INSERTION_PADDING_BYTES or addresses[idx] - offset - cfg_size < _INSERTION_PADDING_BYTES)):
                raise InvalidInsertionMemoryAddressError("Cannot insert library at address: 0x%x" % offset)

        # Insert all the new functions/basic blocks, adding offsets to the addresses
        edges = []
        for func in cfg.functions:
            new_func = CFGFunction(parent_cfg=self, address=func.address + offset, name=func.name, 
                                    is_extern_func=func.is_extern_function, blocks=None)
            self.functions_dict[new_func.address] = new_func

            for block in func.blocks:
                new_block = CFGBasicBlock(parent_function=new_func, address=block.address + offset, labels=block.labels,
                                            asm_lines=[(a + offset, l) for a, l in block.asm_lines])
                self.blocks_dict[new_block.address] = new_block
                new_func.blocks.append(block)

                # Keep track of the edges, they will be added later with references to the new block objects
                for edge_set in [block.edges_in, block.edges_out]:
                    for edge in edge_set:
                        edges.append((edge.from_block.address + offset, edge.to_block.address + offset, edge.edge_type))
        
        # Add in the edges for resolved external function symbols
        for func in self.functions:
            if func.symbol_name is not None and func.symbol_name in function_mapping:
                extern_func = self.get_function(function_mapping[func.symbol_name] + offset)

                # Assume the function has one block for now, we'll have to fix that later if that isn't true
                if len(func.blocks) != 1:
                    raise ValueError("Attempting to insert resolved symbolic normal edge to external function, but the "
                                        "external function had %d blocks! (expected 1)" % len(func.blocks))
                
                edges.append((func.blocks[0].address, extern_func.address, EdgeType.NORMAL))
        
        # Add in all of the edges
        for from_addr, to_addr, edge_type in edges:
            from_block = self.get_block(from_addr)
            to_block = self.get_block(to_addr)
            new_edge = CFGEdge(from_block, to_block, edge_type)

            from_block.edges_out.add(new_edge)
            to_block.edges_in.add(new_edge)
    
    @property
    def functions(self):
        """A list of functions in this CFG (in order of memory address)"""
        return [f[1] for f in sorted(self.functions_dict.items(), key=lambda x: x[0])]
    
    @property
    def blocks(self):
        """A list of basic blocks in this CFG (in order of memory address)"""
        return [b[1] for b in sorted(self.blocks_dict.items(), key=lambda x: x[0])]
    
    @property
    def num_blocks(self):
        """The number of basic blocks in this cfg"""
        return len(self.blocks_dict)
    
    @property
    def num_functions(self):
        """The number of functions in this cfg"""
        return len(self.functions_dict)

    @property
    def num_edges(self):
        """The number of edges in this cfg"""
        return sum(b.num_edges for b in self.blocks_dict.values())

    @property
    def num_asm_lines(self):
        """The number of asm lines across all blocks in this cfg"""
        return sum(b.num_asm_lines for b in self.blocks_dict.values())

    @property
    def asm_counts(self):
        """A collections.Counter() of all unique assembly lines and their counts in this cfg"""
        return sum((f.asm_counts for f in self.functions_dict.values()), Counter())
    
    @property
    def edges(self):
        """A list of all outgoing ``CFGEdge``'s in this ``CFG``"""
        return [e for b in self.blocks for e in b.edges_out]

    def to_adjacency_matrix(self, type: str = 'np', sparse: bool = False):
        """Returns an adjacency matrix representation of this cfg's graph connections

        Currently is slow because I just convert to a MemCFG, then call that object's to_adjacency_matrix(). I should
        probably speed this up at some point...

        Connections will be directed and have values:

            - 0: No edge
            - 1: Normal edge
            - 2: Function call edge

        See :func:`bincfg.memcfg.to_adjacency_matrix` for more details

        Args:
            type (str, optional): the type of matrix to return. Defaults to 'np'. Can be:

                - 'np'/'numpy' for a numpy ndarray (dtype: np.int32)
                - 'torch'/'pytorch' for a pytorch tensor (type: LongTensor)
            
            sparse (bool, optional): whether or not the return value should be a sparse matrix. Defaults to False. Has 
                different behaviors based on type:

                - numpy array: returns a 2-tuple of sparse COO representation (indices, values). 
                    NOTE: if you want sparse CSR format, you already have it with self.graph_c and self.graph_r
                - pytorch tensor: returns a pytorch sparse COO tensor. 
                    NOTE: not using sparse CSR format for now since it seems to have less documentation/supportedness. 

        Returns:
            Union[np.ndarray, torch.Tensor]: an adjacency matrix representation of this ``CFG``
        """
        from .mem_cfg import MemCFG
        return MemCFG(self, normalizer='base' if self.normalizer is None else None).to_adjacency_matrix(type=type, sparse=sparse)
    
    def get_compressed_stats(self, tokens):
        """Returns some stats about this CFG in a compressed version
        
        These are meant to be very basic stats useful for simple comparisons (EG: dataset subsampling). These values
        are highly compressed/convoluted as they are used for generating statistics on 100+ million cfg's on HPC, and 
        thus output space requirements outweigh single-graph compute time. Will return a single numpy array 
        (1-d, dtype=np.uint8) with indices/values:

            - [0:12]: graph-level stats (number of nodes, number of functions, number of assembly lines), each a 4-byte 
              unsigned integer of the exact value in the above order. The bytes are always stored as little-endian.

            - [12:20]: node degree histogram. Counts the number of nodes with degrees: 0 incomming, 1 incomming, 2 incomming,
              3+ incomming, 0 outgoing, 1 outgoing, 2 outgoing, 3+ outgoing. See below in things that are not
              in these stats for reasoning. Values will be a list in the above order:

              [0-in, 1-in, 2-in, 3+in, 0-out, 1-out, 2-out, 3+out]

              Reasoning: the vast majority of all nodes will have 0, 1 or 2 incomming normal edges, and 0, 1, or 2 outgoing
              normal edges, so this should be a fine way of storing that data for my purposes. Function call edges will
              be handled by the function degrees.

            - [20:46]: a histogram of node sizes (number of assembly lines per node). Histogram bins (left-inclusive, 
              right-exclusive, 26 of them) will be:

              [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 18, 20, 25, 30, 35, 40, 50, 60, 80, 100, 150, 200+]

              Reasoning: different compiler optimizations (inlining, loop unrolling, AVX instructions, etc.) will likely
              drastically change the sizes of nodes. The histogram bin edges were chosen arbitrarily in a way that tickled
              my non-neurotypical, nice-number-loving brain.

            - [46:72]: a histogram of (undirected) function degrees (in the function call graph). Histogram bins 
              (left-inclusive, right-exclusive, 26 of them) will be:

              [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 18, 20, 25, 30, 35, 40, 50, 60, 80, 100, 150, 200+]

              Reasoning: most functions will only be called a relatively small number of unique times across the 
              binary (EG: <10), while those that are called much more are likely

            - [72:93]: a histogram of function sizes (number of nodes in each function). Histogram bins (left-inclusive, 
              right-exclusive, 21 of them) will be:

              [0, 1, 2, 3, 4, 6, 8, 10, 15, 20, 25, 30, 40, 60, 80, 100, 150, 200, 300, 400, 500+]
              
              Reasoning: different compiler optimizations (especially inlining) will drastically change the size of 
              functions. The histogram bins can be more spread out (IE: not as focused on values near 0, and across
              a larger range) since the number of nodes in a function has a noticeably different distribution than,
              say, the histogram of node sizes

            - [93:]: a histogram of assembly tokens. One value per token. You should make sure the normalization method
              you are using is good, and doesn't create too many unique tokens.
              
              Reasoning: obvious
        
        The returned array will be of varrying length based on the number of unique tokens in the tokens dictionary.
        
        Values above (unless otherwise stated) are stored as 1-byte percentages of the number of nodes in the graph that
        are in that bin. EG: 0 would mean there are 0 nodes with that value, 1 would mean between [0, 1/255) of the 
        nodes/functions in the graph have that value, 2 would be between [1/255, 2/255), etc., until a 255 which would
        be [245/255, 1.0]
        
        Things that are NOT in these stats and reasons:

            - Other node degrees: these likely don't change too much between programs (specifically their normalized values)
              as even with different programs/compiler optimizations. Changes between cfg's will likely only change the 
              relative proportions of nodes with 1 vs. 2 incoming edges, and those with 1 vs. 2 vs. 3 outgoing edges. Any
              other number of edges are pretty rare, hence why we only keep those edge measures and only those using 
              normal edges (since function call edges will be gathered in the func_degree_hist and would mess with this premise)
            - Edge statistics (EG: number of normal vs. function call edges): this information is partially hidden in the
              histograms already present, and exact values do not seem too important
            - Other centrality measures: I belive (but have not proven) that node-based centrality measures would not
              contain enough information to display differences between CFG's to be worth it. Because of the linear
              nature of sequential programs, I believe their centrality measures would be largely similar and/or
              dependant on other graph features already present in the stats above (EG: number of nodes in a function).
              I think any differences between centrality measurements on these graphs will be mostly washed out by the
              linear nature, especially since we would only be looking at normal edges, not function call ones. The only
              differences that would be highlighted would be information about the number of branches/loops in each
              function (which is already partially covered by the assembly line info), and a small amount of information
              on where within functions these branches tend to occur. However, combining these features into graph-level
              statistics would likely dilute these differences even further. It may, however, be useful to include one
              or more of these measures on the function call graph, but I am on the fence about its usefulness vs extra
              computation time/space required. I think for my purposes, the stats above work just fine
            
        Args:
            tokens (Union[Dict[str, int], AtomicData]): the token dictionary to use and possibly add to. Can also be
                an AtomicData object for atomic token dictionary file.
        
        Returns:
            np.ndarray: the compressed stats, a numpy 1-d uint8 array of shape (97 + len(tokens), )
        """
        # Get the token histogram
        token_counts = self.asm_counts

        # Create the token dictionary and update it with the new tokens
        token_dict = tokens if tokens is not None else {}
        if isinstance(token_dict, AtomicTokenDict):
            token_dict.addtokens({k for k in token_counts if k not in token_dict})
        else:
            for k in token_counts:
                token_dict.setdefault(k, len(token_dict))

        # Make sure this starts off a bit larger than the final length. I think the current val would be 97, but I made
        #   it a little larger for some more wiggle room. The correct size is returned anyways
        ret = np.zeros([100 + len(token_dict)], dtype=np.uint8)
        curr_idx = 0

        # Adding graph statistics as multi-byte unsigned integer values
        nb = GLS_DTYPE().nbytes
        for f in GRAPH_LEVEL_STATS_FUNCS:
            ret[curr_idx: curr_idx + nb] =  _get_np_int_as_little_endian_list(GLS_DTYPE(f(self)))
            curr_idx += nb

        # Get the node degree histograms
        in_0, in_1, in_2, in_other, out_0, out_1, out_2, out_other = 0, 0, 0, 0, 0, 0, 0, 0
        for block in self.blocks:
            n_in, n_out = block.get_sorted_edges(edge_types=EdgeType.NORMAL)

            # Check edges in
            if len(n_in) == 0:
                in_0 += 1
            elif len(n_in) == 1:
                in_1 += 1
            elif len(n_in) == 2:
                in_2 += 1
            else:
                in_other += 1

            # Check edges out
            if len(n_out) == 0:
                out_0 += 1
            elif len(n_out) == 1:
                out_1 += 1
            elif len(n_out) == 2:
                out_2 += 1
            else:
                out_other += 1
        
        num_blocks = self.num_blocks
        for v in [in_0, in_1, in_2, in_other, out_0, out_1, out_2, out_other]:
            ret[curr_idx] = _get_single_byte_ratio(v, num_blocks)
            curr_idx += 1

        # Get the node size histogram
        curr_idx = _get_single_byte_histogram([len(b.asm_lines) for b in self.blocks_dict.values()], 
            NODE_SIZE_HIST_BINS, ret, curr_idx)

        # Get the function undirected degree histogram
        curr_idx = _get_single_byte_histogram([f.num_fc_edges for f in self.functions_dict.values()], 
            FUNCTION_DEGREE_HIST_BINS, ret, curr_idx)
        
        # Get the function size histogram
        curr_idx = _get_single_byte_histogram([f.num_blocks for f in self.functions_dict.values()],
            FUNCTION_SIZE_HIST_BINS, ret, curr_idx)
        
        # Get the asm line histogram
        # Invert the token dict to get a mapping from index to asm line
        inv_token_dict = {v:k for k, v in token_dict.items()}
        num_asm_lines = sum(v for v in token_counts.values())
        ret[curr_idx: curr_idx + len(inv_token_dict)] = [_get_single_byte_ratio(token_counts[inv_token_dict[i]], num_asm_lines) for i in range(len(inv_token_dict))]
        curr_idx += len(inv_token_dict)

        return ret[:curr_idx]
    
    @classmethod
    def uncompress_stats(cls, stats, dtype=np.uint32):
        """Uncompressed the stats from cfg.get_compressed_stats()
        
        Will return a numpy array with specified dtype (defaults to np.uint32) of stats in the same order they appreared 
        in get_compressed_stats(). The size will decrease by around 12 indices as the initial 4-byte values are converted
        back into a one-index integer.

        Args:
            stats (np.ndarray): either a 1-d or 2-d numpy array of stats. If 2-d, then it is assumed that these are multiple
                stats for multiple cfgs, one cfg per row
            dtype (np.dtype): the numpy dtype to return as. Defaults to np.uint32
        
        Returns:
            np.ndarray: either a 1-d or 2-d numpy array of uncompressed stats, depending on what was passed to `stats`
        """
        if stats.ndim not in [1, 2]:
            raise ValueError("`stats` array must have dimension 1 or 2, not %d" % stats.ndim)
        
        # Get the return array, removing the elements for the multi-byte ints. Determine what dimension to be using
        if stats.ndim == 2:
            ret = np.empty([stats.shape[0], stats.shape[1] - (len(GRAPH_LEVEL_STATS_FUNCS) * (GLS_DTYPE().nbytes - 1))], dtype=np.uint32)
            ret_one_dim = False
        else:
            ret = np.empty([1, stats.shape[0] - (len(GRAPH_LEVEL_STATS_FUNCS) * (GLS_DTYPE().nbytes - 1))], dtype=np.uint32)
            stats = [stats]
            ret_one_dim = True

        # Iterate through all rows in stats to uncompress
        for row_idx, stat_arr in enumerate(stats):
            stats_idx = ret_idx = 0

            # Unpack the multi-byte ints
            nb = GLS_DTYPE().nbytes
            for _ in GRAPH_LEVEL_STATS_FUNCS:
                ret[row_idx, ret_idx] = _get_np_int_from_little_endian_list(stat_arr[stats_idx: stats_idx + nb])
                stats_idx += nb
                ret_idx += 1
            
            num_blocks, num_functions, num_asm_lines = ret[row_idx, 0:3]

            # Unpack the histograms: node degrees, node sizes, function degrees, function sizes, asm lines
            ret_idx, stats_idx = _uncompress_hist(row_idx, ret, stat_arr[stats_idx: stats_idx + 8], ret_idx, stats_idx, num_blocks)
            ret_idx, stats_idx = _uncompress_hist(row_idx, ret, stat_arr[stats_idx: stats_idx + len(NODE_SIZE_HIST_BINS)], ret_idx, stats_idx, num_blocks)
            ret_idx, stats_idx = _uncompress_hist(row_idx, ret, stat_arr[stats_idx: stats_idx + len(FUNCTION_DEGREE_HIST_BINS)], ret_idx, stats_idx, num_functions)
            ret_idx, stats_idx = _uncompress_hist(row_idx, ret, stat_arr[stats_idx: stats_idx + len(FUNCTION_SIZE_HIST_BINS)], ret_idx, stats_idx, num_functions)
            ret_idx, stats_idx = _uncompress_hist(row_idx, ret, stat_arr[stats_idx:], ret_idx, stats_idx, num_asm_lines)

        # Return a 1-d if needed
        ret = ret.reshape([-1]) if ret_one_dim else ret
        return ret.astype(dtype)
    
    def normalize(self, normalizer, inplace=True, force_renormalize=False):
        """Normalizes this cfg in-place.

        Args:
            normalizer (Union[str, Normalizer], optional): the normalizer to use. Can be a ``Normalizer`` object, or a 
                string of a built-in normalizer to use.
            inplace (bool, optional): whether or not to normalize inplace. Defaults to True.
            force_renormalize (bool, optional): by default, this method will only normalize this cfg if the passed 
                `normalizer` is != `self.normalizer`. However if `force_renormalize=True`, then this will be renormalized
                even if it has been previously normalized with the same normalizer. Defaults to False.

        Returns:
            CFG: this ``CFG`` normalized
        """
        return normalize_cfg_data(self, normalizer=normalizer, inplace=inplace, force_renormalize=force_renormalize)

    def to_networkx(self):
        """Converts this CFG to a networkx DiGraph() object
        
        Requires that networkx be installed.

        Creates a new MultiDiGraph() and adds as attributes to that graph:

            - 'normalizer': string name of normalizer, or None if it had none
            - 'metadata': a dictionary of metadata
            - functions: a dictionary mapping integer function addresses to named tuples containing its data with the
              structure ('name': Union[str, None], 'is_extern_func': bool, 'blocks': Tuple[int, ...]).

                * The 'name' element (first element) is a string name of the function, or None if it doesn't have a name
                * The 'is_extern_func' element (second element) is True if this function is an extern function, False otherwise.
                  An extern function is one that is located in an external library intended to be found at runtime, and
                  that doesn't have its code here in the CFG, only a small function meant to jump to the external function
                  when loaded at runtime
                * The 'blocks' element (third element) is an arbitrary-length tuple of integers, each integer being the
                  memory address (equivalently, the block_id) of a basic block that is a part of that function. Each
                  basic block is only part of a single function, and each function should have at least one basic block
            
              NOTE: the ADDRESS value will be and uppercase hex starting with a '0x'
        
        NOTE: we use a multidigraph because edges are directed (in order of control flow), and it is theoretically
        possible (and occurs in some data) to have a node that calls another node, then has a normal edge back out
        to it. This has occured in some libc setup code
        
        Then, each basic block will be added to the graph as nodes. Their id in the graph will be their integer address.
        Each block will have the following attributes:

            - 'asm_lines' (Tuple[Tuple[int, str]]): tuple of assembly lines. Each assembly line is a (address, line)
              tuple where `address` is the integer address of that assembly line, and `line` is a cleaned, space-separated
              string of tokens in that assembly line
            - 'labels' (Set[str]): a set of string labels for nodes, empty meaning it is unlabeled
        
        Finally, all edges will be added (directed based on control flow direction), and with the attributes:

            - 'edge_type' (str): the edge type, will be 'normal' for normal edges and 'function_call' for function call
              edges

        """
        # Done like this so I have IDE autocomplete while making sure the package is installed
        _netx = get_module('networkx', raise_err=True)
        import networkx

        # Add all of the functions to a dictionary to set as an attribute on the graph
        functions = {func.address: _NetXTuple(func.name, func._is_extern_function, tuple(b.address for b in func.blocks))
                     for func in self.functions_dict.values()}

        ret = networkx.MultiDiGraph(normalizer=None if self.normalizer is None else self.normalizer.__class__.__name__.lower().replace("normalizer", ''),
                             functions=functions, metadata=self.metadata)
        
        # Add all of the blocks to the graph
        for block in self.blocks_dict.values():
            ret.add_node(block.address, labels=set(NODE_LABELS_INT_TO_STR[l] for l in block.labels),
                         asm_lines=tuple((a, l if isinstance(l, str) else ' '.join(l)) for a, l in block.asm_lines))
        
        # Finally, add all the edges
        for edge in self.edges:
            ret.add_edge(edge.from_block.address, edge.to_block.address, key=edge.edge_type.name.lower())
        
        return ret
    
    @classmethod
    def from_networkx(cls, graph, cfg=None):
        """Converts a networkx graph to a CFG

        Expects the graph to have the exact same structure as is shown in CFG().to_networkx()

        You can optionally pass a cfg, in which case this data will be added to (and override) that cfg
        """
        if cfg is None:
            ret = CFG(normalizer=graph.graph['normalizer'], metadata=graph.graph['metadata'])
        else:
            ret = cfg
            ret.metadata.update(graph.graph['metadata'])
            if graph.graph['normalizer'] is not None:
                ret.normalize(graph.graph['normalizer'])

        ret.add_function(*[
            CFGFunction(address=addr, name=name, is_extern_func=ef, blocks=[
                CFGBasicBlock(
                    address=block_addr,
                    labels=set(NODE_LABELS_STR_TO_INT[l] for l in graph.nodes[block_addr]['labels']),
                    edges_out=[(None, a, et) for _, a, et in graph.edges(block_addr, keys=True)],
                    asm_lines=[(a, l if ret.normalizer is None else l.split(' ')) for a, l in graph.nodes[block_addr]['asm_lines']]
                )
                for block_addr in blocks
            ])
            for addr, (name, ef, blocks) in graph.graph['functions'].items()
        ])

        return ret

    def to_cfg_dict(self):
        """Converts this cfg to a dictionary of cfg information
        
        The cfg dictionary will have the structure::

            {
                'normalizer': the string name of the normalizer used,
                'metadata': a dictionary of metadata,
                'functions': {
                
                    func_address_1: {
                        'name' (str): string name of the function, or None if it has no name,
                        'is_extern_func' (bool): True if this function is an extern function, False otherwise.
                            An extern function is one that is located in an external library intended to be found at 
                            runtime, and that doesn't have its code here in the CFG, only a small function meant to jump to 
                            the external function when loaded at runtime

                        'blocks': {
                            block_address_1: {
                                'labels' (Set[str]): a set of string labels for nodes, empty meaning it is unlabeled
                                'edges_out' (Tuple[Tuple[int, str], ...]): tuple of all outgoing edges. Each 'edge' is a tuple
                                    of (other_basic_block_address: int, edge_type: str), where `edge_type` can be 'normal'
                                    for a normal edge and 'function_call' for a function call edge
                                'asm_lines' (Tuple[Tuple[int, str], ...]): tuple of all assembly lines in this block. Each 
                                    assembly line is a (address, line) tuple where `address` is the integer address of that 
                                    assembly line, and `line` is a cleaned, space-separated string of tokens in that assembly 
                                    line
                            },

                            block_address_1: ...,
                            ...
                        }

                    },

                    func_address_2: ...,
                    ...
                }
            }

        - func_address_X: integer address of that function
        - block_address_X: integer address of that block
        """
        return {
            'normalizer':  None if self.normalizer is None else self.normalizer.__class__.__name__.lower().replace("normalizer", ''),
            'metadata': self.metadata,
            'functions': {
                func.address: {
                    'name': func.name,
                    'is_extern_func': func._is_extern_function,
                    'blocks': {
                        block.address: {
                            'labels': set(NODE_LABELS_INT_TO_STR[l] for l in block.labels),
                            'edges_out': tuple((e.to_block.address, e.edge_type.name.lower()) for e in block.edges_out),
                            'asm_lines': tuple((a, l if isinstance(l, str) else ' '.join(l)) for a, l in block.asm_lines)
                        }
                        for block in func.blocks
                    }
                }
                for func in self.functions
            }
        }
    
    @classmethod
    def from_cfg_dict(cls, cfg_dict, cfg=None):
        """Converts a cfg dict object into a CFG
        
        Expects the cfg_dict to have the exact same structure as that listed in CFG().to_cfg_dict()

        You can optionally pass a cfg, in which case this data will be added to (and override) that cfg
        """
        if cfg is None:
            ret = CFG(normalizer=cfg_dict['normalizer'], metadata=cfg_dict['metadata'])
        else:
            ret = cfg
            ret.metadata.update(cfg_dict['metadata'])
            if cfg_dict['normalizer'] is not None:
                ret.normalize(cfg_dict['normalizer'])

        ret.add_function(*[
            CFGFunction(address=func_addr, name=func_dict['name'], is_extern_func=func_dict['is_extern_func'], blocks=[
                CFGBasicBlock(
                    address=block_addr,
                    labels=set(NODE_LABELS_STR_TO_INT[l] for l in block_dict['labels']),
                    edges_out=[(None, a, et) for a, et in block_dict['edges_out']],
                    asm_lines=[(a, l if ret.normalizer is None else \
                                    [l] if ret.normalizer.tokenization_level == TokenizationLevel.INSTRUCTION else \
                                    l.split(' ')) for a, l in block_dict['asm_lines']]
                )
                for block_addr, block_dict in func_dict['blocks'].items()
            ])
            for func_addr, func_dict in cfg_dict['functions'].items()
        ])

        return ret
    
    def save(self, path):
        """Saves this CFG to path"""
        with open(path, 'wb') as f:
            pickle.dump(self, f)
    
    @classmethod
    def load(cls, path):
        """Loads this CFG from path"""
        with open(path, 'rb') as f:
            return pickle.load(f)
    
    def __getstate__(self):
        """State for pickling
        
        Pickling should be done like so:

            - normalizers are wrapped with _Pickled_Normalizer() object
            - edges are converted into 3-tuples of (from_address: int, to_address: int, edge_type: EdgeType) (done in
              CFGBasicBlock)
            - references to parent_objects are removed
        """
        state = {k: v for k, v in self.__dict__.items() if k not in ['functions_dict', 'blocks_dict', '_inst_lookup']}
        state['normalizer'] = _Pickled_Normalizer(state['normalizer'])
        state['functions'] = tuple(f._get_pickle_state() for f in self.functions_dict.values())
        return state
    
    def __setstate__(self, state):
        """State for unpickling"""
        state['normalizer'] = state['normalizer'].unpickle()
        for k, v in state.items():
            if k == 'functions':
                continue
            setattr(self, k, v)
        
        self.functions_dict = {func_addr: CFGFunction(parent_cfg=self)._set_pickle_state([func_addr,] + rest) for func_addr, *rest in state['functions']}
        self.blocks_dict = {b.address: b for f in self.functions_dict.values() for b in f.blocks}
        
        # Recreate all the edges
        edges = set(e for b in self.blocks_dict.values() for e in (b._temp_edges_in + b._temp_edges_out))
        for from_addr, to_addr, edge_type in edges:
            edge = CFGEdge(self.get_block(from_addr), self.get_block(to_addr), edge_type)
            edge.from_block.edges_out.add(edge)
            edge.to_block.edges_in.add(edge)
        
        # Delete the _temp_edges attributes for all blocks
        for block in self.blocks_dict.values():
            if hasattr(block, '_temp_edges_in'):
                del block._temp_edges_in
            if hasattr(block, '_temp_edges_out'):
                del block._temp_edges_out
    
    def __eq__(self, other):
        return isinstance(other, CFG) and all(eq_obj(self, other, selector=s) for s in ['normalizer', 'functions_dict', 'metadata'])
    
    def __hash__(self):
        return sum(hash(f) for f in self.functions_dict.values()) * 7 + hash_obj(self.metadata, return_int=True) * 17

    def __str__(self):
        norm_str = 'no normalizer' if self.normalizer is None else ('normalizer: ' + repr(str(self.normalizer)))
        return "CFG with %s and %d functions, %d basic blocks, %d edges, and %d lines of assembly\nMetadata: %s" \
            % (norm_str, len(self.functions_dict), self.num_blocks, self.num_edges, self.num_asm_lines, self.metadata)

    def __repr__(self):
        return str(self)
    
    def get_cfg_build_code(self):
        """Returns python code that will build the given cfg. Used for testing

        Args:
            cfg (CFG): the cfg
        
        Returns:
            str: string of python code to build the cfg
        """
        all_functions = "\n    ".join([("%d: CFGFunction(parent_cfg=__auto_cfg, address=%d, name=%s, is_extern_func=%s)," % 
            (f.address, f.address, repr(f.name), f.is_extern_function)) for f in self.functions])
        
        all_blocks = "\n    ".join([("%s: CFGBasicBlock(parent_function=__auto_functions[%d], address=%d, labels=%s, asm_lines=[\n        %s\n    ])," % 
            (b.address, b.parent_function.address, b.address, repr(b.labels), 
                '\n        '.join([("(%d, %s)," % (addr, repr(inst))) for addr, inst in b.asm_lines])
            )) for b in self.blocks])
        
        all_edges = "\n\n".join([("__auto_blocks[%d].edges_out = set([\n    %s\n])" % (b.address, 
            "\n    ".join([("CFGEdge(from_block=__auto_blocks[%d], to_block=__auto_blocks[%d], edge_type=EdgeType.%s)," % (edge.from_block.address, edge.to_block.address, edge.edge_type.name)) for edge in b.edges_out])
        )) for b in self.blocks])

        add_blocks = '\n\n'.join([("__auto_functions[%d].blocks = set([\n    %s\n])" % (f.address,
            "\n    ".join([("__auto_blocks[%d]," % b.address) for b in f.blocks])
        )) for f in self.functions])

        return _CFG_BUILD_CODE_STR % (self.num_functions, self.num_blocks, self.num_edges, self.num_asm_lines, all_functions,
            all_blocks, all_edges, add_blocks)


def _get_np_int_as_little_endian_list(val):
    """returns a list of bytes for the given numpy integer in little-endian order"""
    ret = list(val.tobytes())

    # Get the byte order and check if we need to swap endianness
    bo = np.dtype(GLS_DTYPE).byteorder
    if (bo == '=' and sys.byteorder == 'big') or bo == '>':
        return reversed(ret)

    return ret


def _get_np_int_from_little_endian_list(l):
    """Returns a numpy integer from the given list of little-endian bytes
    
    NOTE: `l` MUST be either a python built-in (list/tuple/etc), or a numpy array with dtype np.uint8!
    """
    return int.from_bytes(l, byteorder='little', signed=False)


def _get_single_byte_ratio(val, total):
    """Computes the ratio val/total (assumes total >= val), and converts that resultant value to a byte
    
    The byte value will be determined based on what 'chunk' in the range [0, 1] the value is, with there being 256
    available chunks for one byte. EG: 0 would mean val == 0, 1 would mean val is between [0, 1/255), 2 would be between
    [1/255, 2/255), etc., until a 255 which would be between [245/255, 1].
    """
    assert total >= val, "Total was < val! Total: %d, val: %d" % (total, val)
    return 0 if total == 0 else math.ceil(val / total * 255)


def _get_single_byte_histogram(vals, bins, ret, curr_idx):
    """Does a full histogram thing
    
    Args:
        vals (Iterable[int]): the values to bin/histogram
        bins (Iterable[int]): the bins to use. Should start with the lowest value, and have right=False.
            EG: with bins [0, 3, 7, 9] and values [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10], they would be digitized into
            [0, 0, 0, 1, 1, 1, 1, 2, 2, 3, 3]
        ret (np.ndarray): the values to insert into
        curr_idx (int): the index in ret to insert into
    
    Returns:
        int: the starting index in ret to continue inserting values
    """
    binned = np.digitize(vals, bins, right=False) - 1
    uniques, counts = np.unique(binned, return_counts=True)
    ret[uniques + curr_idx] = [_get_single_byte_ratio(c, len(binned)) for c in counts]
    return curr_idx + len(bins)


def _uncompress_hist(row_idx, ret, stats, ret_idx, stats_idx, val):
    """Uncompresses histogram values, and returns new ret_idx and stats_idx, with `val` being the value that was used
    for the percentages (IE: self.num_blocks). Stores uncompressed values into ret (a 2-d array)"""
    ret[row_idx, ret_idx: ret_idx + len(stats)] = np.ceil(stats * 1/255 * val)
    return ret_idx + len(stats), stats_idx + len(stats)


class InvalidInsertionMemoryAddressError(Exception):
    pass


# NamedTuple used for conversion to networkx graph
_NetXTuple = namedtuple('CFGFunctionDataTuple', 'name is_extern_func blocks')

        
_CFG_BUILD_CODE_STR = """
##################
# AUTO-GENERATED #
##################

# Create the cfg object. This cfg has %d functions, %d basic blocks, %d edges, and %d lines of assembly.
__auto_cfg = CFG()

# Building all functions. Dictionary maps integer address to CFGFunction() object
__auto_functions = {
    %s
}

# Building basic blocks. Dictionary maps integer address to CFGBasicBlock() object
__auto_blocks = {
    %s
}

# Building all edges
%s

# Adding basic blocks to their associated functions
%s

# Adding functions to the cfg
__auto_cfg.add_function(*__auto_functions.values())

######################
# END AUTO-GENERATED #
######################
"""
