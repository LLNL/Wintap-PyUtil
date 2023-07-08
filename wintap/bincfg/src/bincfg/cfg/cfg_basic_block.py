import re
import warnings
from collections import Counter
from .cfg_edge import get_edge_type, EdgeType, CFGEdge
from ..utils import get_address, eq_obj, hash_obj
from ..labeling.node_labels import RAW_LABEL_VAL_DICT, CLEAR_NODE_LABELS_VAL, RAW_LABEL_TO_NODE_LABEL_INT, NODE_LABELS_INT_TO_STR


# Define which instruction strings are 'return' instructions for CFGBasicBlock().is_function_return
RE_RETURN_INSTRUCTION = re.compile(r'[ \t\n]*(?:lock )*(?:ret[nf]?).*')

# Find start/end label nop instructions
# NOTE: these must be searched for before normalizing
# NOTE: this only works with rose for now
LABEL_RE = re.compile(r'nop[ \t]+word ds:\[(0x[0-9a-fA-F]+)(?:<.*>)?\]')


class CFGBasicBlock:
    """A single basic block in a ``CFG``.

    Can be initialized empty, or with attributes. Assumes its memory address is always unique within a cfg.

    NOTE: these objects should not be pickled/copy.deepcopy()-ed by themselves, only as a part of a cfg

    Parameters
    ----------
    parent_function: `CFGFunction`
        the ``CFGFunction`` this basic block belongs to
    address: `Union[int, str, Addressable]`
        the memory address of this ``CFGBasicBlock``. Should be unique to the ``CFG`` that contains it
    edges_in: `Optional[Iterable[CFGEdge]]` 
        an iterable of incoming CFGEdge objects
    edges_out: `Optional[Iterable[CFGEdge]]`
        an iterable of outgoing CFGEdge objects
    asm_lines: `Optional[Union[Iterable[Tuple[int, str]], Iterable[Tuple[int, Tuple[str]]]]]`
        an iterable of assembly lines present at this basic block. Each element should be a 2-tuple of 
        (address: int, instruction: inst_strs), where `inst_strs` can either be a single, non-normalized string, or a
        tuple of normalized strings
    labels: `Optional[Iterable[int]]`
        an iterable of integer labels for this basic block. Labels are indices in the ``bincfg.labelling.NODE_LABELS`` 
        list of node labels. Duplicate labels will be ignored
    """

    parent_function = None
    """The parent function containing this ``CFGBasicBlock``"""

    address = None
    """The unique integer memory address of this ``CFGBasicBlock``"""

    edges_in = None
    """List of incoming ``CFGEdge``'s"""

    edges_out = None
    """List of outgoing ``CFGEdge``'s"""

    asm_lines = None
    """List of assembly instructions in this ``CFGBasicBlock``
    
    Each element is a tuple of (address, instruction). Each `instruction` is either a string (if this block has not yet
    been normalized) or a tuple of strings (if this block has been normalized)
    """

    labels = None
    """Set of all labels for this ``CFGBasicBlock``
    
    Labels should be indices in the ``bincfg.labelling.NODE_LABELS`` list of node labels
    """

    def __init__(self, parent_function=None, address=None, edges_in=None, edges_out=None, asm_lines=None, labels=None):
        self.parent_function = parent_function
        self.address = get_address(address) if address is not None else address
        self.edges_in = set() if edges_in is None else set(edges_in)
        self.edges_out = set() if edges_out is None else set(edges_out)
        self.asm_lines = [] if asm_lines is None else list(asm_lines)
        self.labels = set() if labels is None else set(labels)

        # Used for the recursive child node labelling
        self._labeling_parent = None
    
    @property
    def num_edges(self):
        """The number of edges out in this basic block"""
        return self.num_edges_out
    
    @property
    def num_edges_out(self):
        """The number of outgoing edges in this basic block"""
        return len(self.edges_out)

    @property
    def num_edges_in(self):
        """The number of incoming edges in this basic block"""
        return len(self.edges_in)
    
    @property
    def num_asm_lines(self):
        """The number of assembly lines in this basic block"""
        return len(self.asm_lines) if len(self.asm_lines) > 0 and isinstance(self.asm_lines[0][1], str) else sum(len(l[1]) for l in self.asm_lines)
    
    @property
    def asm_counts(self):
        """A ``collections.Counter`` of all unique assembly lines/tokens and their counts in this basic block"""
        if len(self.asm_lines) > 0 and isinstance(self.asm_lines[0][1], str):
            return Counter(l[1] for l in self.asm_lines)
        return Counter(l for t in self.asm_lines for l in t[1])
    
    @property
    def asm_memory_addresses(self):
        """A set containing all memory addresses for assembly lines in this block"""
        return set(a for a, *_ in self.asm_lines)
    
    @property
    def is_function_return(self):
        """True if this block is a function return, False otherwise
        
        Specifically, returns True iff this block's final assembly instruction is a 'return' instruction (IE: if the
        final assembly instruction re.fullmatch()'s RE_RETURN_INSTRUCTION), or if this is a block in an external function
        """
        if len(self.asm_lines) == 0: 
            return self.parent_function.is_extern_function and len(self.parent_function.blocks) == 0

        last_line = self.asm_lines[-1][1] if isinstance(self.asm_lines[-1][1], str) else self.asm_lines[-1][1][0]
        return (RE_RETURN_INSTRUCTION.fullmatch(last_line) is not None) or (self.parent_function.is_extern_function and len(self.parent_function.blocks) == 0)
    
    @property
    def is_function_entry(self):
        """True if this block is a function entry block, False otherwise
        
        Specifically, returns True if this block's address matches its parent function's address. If this block has
        no parent, False is returned.
        """
        return self.parent_function is not None and self.address == self.parent_function.address
    
    @property
    def is_function_call(self):
        """True if this block is a function call, False otherwise
        
        Checks if this block has one or more outgoing function call edges
        """
        return any(e.edge_type is EdgeType.FUNCTION_CALL for e in self.edges_out)

    @property
    def is_function_jump(self):
        """True if this block is a function jump, False otherwise
        
        Checks if this block has a 'jump' instruction to a basic block in a different function. Specifically, checks if
        this block has an outgoing EdgeType.NORMAL edge to a basic block who's parent_function has an address different
        than this basic block's parent_function's address.
        """
        return any((e.edge_type is EdgeType.NORMAL and e.to_block.parent_function.address != self.parent_function.address) for e in self.edges_out)
    
    @property
    def is_multi_function_call(self):
        """True if this block is a multi-function call, False otherwise
        
        IE: this block has either two or more function call edges out, or one function call and two or more normal edges out
        """
        edge_lists = self.get_sorted_edges(edge_types=None, direction='out')
        return len(edge_lists[1]) >= 2 or (len(edge_lists[1]) == 1 and len(edge_lists[0]) > 1)
    
    @property
    def all_edges(self):
        """Returns a set of all edges in this basic block"""
        return self.edges_in.union(self.edges_out)

    @property
    def normal_parents(self):
        """Returns a set of all basic blocks that are normal parents to this one (have an outgoing normal edge to this block, and are within the same function"""
        return set(b for b in [e.from_block for e in self.get_sorted_edges(edge_types='normal', direction='in')[0]] 
                    if b.parent_function is self.parent_function)
    
    @property
    def normal_children(self):
        """Returns a set of all basic blocks that are normal children to this one (have an incoming normal edge from this block, and are within the same function"""
        return set(b for b in [e.to_block for e in self.get_sorted_edges(edge_types='normal', direction='out')[0]] 
                    if b.parent_function is self.parent_function)

    @property
    def _raw_nop_labels(self):
        """Returns all of the integer labels present in nop instructions in this block that are in RAW_LABEL_VAL_DICT
        
        This is only meant to be used by internal code as the start labels will be removed when creating a CFG
        """
        matches = [LABEL_RE.fullmatch(l) for a, l in self.asm_lines]
        ret = set(int(m.groups()[0], 0) for m in matches if m is not None)

        # Raise an error if there are any unknown labels
        failed = [i for i in ret if i not in RAW_LABEL_VAL_DICT]
        if len(failed) > 0:
            raise ValueError("Unknown node labels: %s\nAvailable node labels: %s" % (failed, RAW_LABEL_VAL_DICT))

        return ret
    
    @property
    def _start_labels(self):
        """Returns all of the start label indices in NODE_LABELS in this block
        
        This is only meant to be used by internal code as the start labels will be removed when creating a CFG

        Returns:
            set[int]: a set of the integer NODE_LABELS
        """
        return set(RAW_LABEL_TO_NODE_LABEL_INT[l] for l in self._raw_nop_labels if RAW_LABEL_VAL_DICT[l].endswith('_start'))
    
    @property
    def _end_labels(self):
        """Returns all of the end label indices in NODE_LABELS in this block
        
        This is only meant to be used by internal code as the end labels will be removed when creating a CFG

        Returns:
            set[int]: a set of the integer NODE_LABELS
        """
        raw_labels = self._raw_nop_labels

        # Check for a clear_node_labels instruction. If so, return all NODE_LABELS that are not in this block's start labels
        if CLEAR_NODE_LABELS_VAL in raw_labels:
            return set(k for k in NODE_LABELS_INT_TO_STR.keys() if k not in self._start_labels)
        
        return set(RAW_LABEL_TO_NODE_LABEL_INT[l] for l in self._raw_nop_labels if RAW_LABEL_VAL_DICT[l].endswith('_end'))

    @property
    def is_padding_node(self):
        """Returns True if this is a padding node (contains only NOP instructions for memory alignment)"""
        return all(l.lower().strip().startswith('nop') for a, l in self.asm_lines)
    
    @property
    def instruction_addresses(self):
        """Returns a set of addresses for all instructions in this basic block"""
        return set(l[0] for l in self.asm_lines)

    def _clear_node_label_instructions(self):
        """Removes all of the nop node label instructions. Should only be called after all nodes have been labelled."""
        self.asm_lines = [(a, l) for a, l in self.asm_lines if LABEL_RE.fullmatch(l) is None]
    
    def remove_edge(self, edge):
        """Removes the given edge from this block's edges (both incoming and outgoing)
        
        Args:
            edge (CFGEdge): the CFGEdge to remove
        
        Raises:
            ValueError: if the edge doesn't exist in the incomming/outgoing edges
        """
        if not isinstance(edge, CFGEdge):
            raise TypeError("edge must be a CFGEdge, not %s" % repr(type(edge).__name__))
        if edge not in self.edges_in and edge not in self.edges_out:
            raise ValueError("%s does not exist in this block's (%s) edges in or out!" % 
                             (edge, ("0x%x" % self.address) if self.address is not None else "NoAddr"))
        
        if edge in self.edges_in:
            self.edges_in.remove(edge)
        if edge in self.edges_out:
            self.edges_out.remove(edge)

    def has_edge(self, address, edge_types=None, direction=None):
        """Checks if this block has an edge from/to the given address

        Args:
            address (AddressLike): a string/integer memory address, or an addressable object 
                (EG: ``CFGBasicBlock``/``CFGFunction``). 
            edge_types (Union[EdgeType, str, Iterable[Union[EdgeType, str]], None], optional): either an edge type or an
                iterable of edge types. Only edges with one of these types will be considered. If None, then all edge 
                types will be considered. Defaults to None.
            direction (Union[str, None], optional): the direction to check (strings 'in'/'from' or 'to'/'out'), 
                or None to check both. Defaults to None.

        Returns:
            bool: True if this block has an edge from/to the given address, False otherwise
        """
        addr, edge_types, directions = get_address(address), _get_edge_types(edge_types, as_set=True), self._get_directions(direction)

        # Check for that edge
        for edge_set in directions:
            for edge in edge_set:
                # Check both that it is an allowable edge_type, and that the edge from/to address matches the given address
                if edge.edge_type in edge_types and \
                    ((edge_set is self.edges_in and edge.from_block.address == addr) or (edge_set is self.edges_out and edge.to_block.address == addr)):
                    return True

        return False
    
    def has_edge_from(self, address, edge_types=None):
        """Checks if this block has an incoming edge from the given address

        Args:
            address (Union[str, int, Addressable]): a string/integer memory address, or an addressable object 
                (EG: CFGBasicBlock/CFGFunction)
            edge_types (Union[EdgeType, str, Iterable[Union[EdgeType, str]], None], optional): either an edge type or an
                iterable of edge types. Only edges with one of these types will be considered. Defaults to None.

        Returns:
            bool: True if this block has an incoming edge from the given address, False otherwise
        """
        return self.has_edge(address=address, edge_types=edge_types, direction='from')

    def has_edge_to(self, address, edge_types=None):
        """Checks if this block has an outgoing edge to the given address

        Args:
            address (Union[str, int, Addressable]): a string/integer memory address, or an addressable object 
                (EG: CFGBasicBlock/CFGFunction)
            edge_types (Union[EdgeType, str, Iterable[Union[EdgeType, str]], None], optional): either an edge type or an 
                iterable of edge types. Only edges with one of these types will be considered. Defaults to None.

        Returns:
            bool: True if this block has an outgoing edge to the given address, False otherwise
        """
        return self.has_edge(address=address, edge_types=edge_types, direction='to')
    
    def calls(self, address):
        """Checks if this block calls the given address

        IE: checks if this block has an outgoing `function_call` edge to the given address

        Args:
            address (Union[str, int, Addressable]): a string/integer memory address, or an addressable object 
                (EG: CFGBasicBlock/CFGFunction)

        Returns:
            bool: True if this block calls the given address, False otherwise
        """
        return self.has_edge(address=address, edge_types='function_call', direction='to')
    
    def get_sorted_edges(self, edge_types=None, direction=None, as_sets=False):
        """Returns a tuple of sorted lists of edges (sorted by address of the "other" block) of each type/direction in this block
        
        Will return edge lists ordered first by edge type (their order of appearance in the cfg_edge.EdgeType enum),
        then by direction ('in', then 'out'). Unless, if `edge_types` is passed, then only those edge types will be
        returned and the edge lists will be returned in the order of the edge types in `edge_types`, then by direction
        ('in', then 'out').

        For example, with `edge_types=None` and `direction=None`, this would return the 4-tuple of:
        (normal_edges_in, normal_edges_out, function_call_edges_in, function_call_edges_out)
        Where each element is a list of CFGEdge objects.

        Args:
            edge_types (Union[EdgeType, str, Iterable[Union[EdgeType, str]], None], optional): either an edge type or an
                iterable of edge types. Only edges with one of these types will be returned. If not None, then the edge 
                lists will be returned sorted based on the order of the edge types listed here, then by direction. Defaults to None.
            direction (Union[str, None], optional): the direction to get (strings 'in'/'from' or 'to'/'out'), or None to
                get both (in order ['in', 'out']). Defaults to None.
            as_sets (bool, optional): if True, then this will return unordered sets of edges instead of sorted lists. This may
                save a ~tiny~ bit of time in the long run, but will hinder deterministic behavior of this method. 
                Defaults to False.

        Returns:
            Union[Tuple[List[CFGEdge], ...], Tuple[Set[CFGEdge], ...]]: a tuple of lists/sets of CFGEdge's
        """
        edge_types, directions = _get_edge_types(edge_types, as_set=False), self._get_directions(direction)

        # Iterate through all edges, adding them to their appropriate sets
        ret_sets = [set() for d in directions for et in edge_types]
        for i, edge_set in enumerate(directions):
            for edge in edge_set:
                if edge.edge_type in edge_types:
                    ret_sets[edge_types.index(edge.edge_type) * len(directions) + i].add(edge)
        
        # Check if we need to sort the edges. If so, sort based on the address of the "other" block
        return ret_sets if as_sets else [list(sorted(s, key=lambda edge: 
                (edge.from_block.address if directions[i % len(directions)] is self.edges_in else edge.to_block.address))) 
            for i, s in enumerate(ret_sets)]

    def _get_directions(self, direction):
        """Gets the directions

        Args:
            direction (Union[str, None]): the direction to get (strings 'in'/'from' or 'to'/'out'), or None to get both 
                (in order ['in', 'out'])

        Raises:
            ValueError: on an unknown direction

        Returns:
            List[List[CFGEdge]]: a list of edges in/out based on direction
        """
        if direction not in [None, 'from', 'to', 'in', 'out']:
            raise ValueError("Unknown `direction`: %s" % repr(direction))
        return [self.edges_in, self.edges_out] if direction is None else [self.edges_in] if direction in ['from', 'in'] else [self.edges_out]
        
    def __str__(self):
        asm = '\n'.join([("\t0x%s: %s" % (addr if isinstance(addr, str) else ('%08x' % addr), line)) for addr, line in self.asm_lines])
        func_name = '' if self.parent_function is None else '' if self.parent_function.name is None else self.parent_function.name
        labels = ('given labels %s' % self.labels) if len(self.labels) > 0 else 'unlabeled'

        func_str = ('in function \"%s\"' % func_name) if self.parent_function is not None else 'with NO PARENT'
        addr_str = ('0x%08x' % self.address) if self.address is not None else 'NO_ADDRESS'

        return "CFGBasicBlock %s at %s with %d edges out, %d edges in, %s with %d lines of assembly:\n%s\nEdges Out: %s\nEdges In: %s" \
            % (func_str, addr_str, self.num_edges_out, self.num_edges_in, labels, len(self.asm_lines), asm, self.edges_out, self.edges_in)
    
    def __repr__(self):
        return str(self)
    
    def __eq__(self, other):
        return isinstance(other, CFGBasicBlock) and all(eq_obj(self, other, selector=s) for s in \
            ['address', 'edges_in', 'edges_out', 'labels', 'asm_lines'])
    
    def __hash__(self):
        return hash(self.address) * 7 + sum(hash(e) for e in self.edges_in) * 11 + sum(hash(e) for e in self.edges_out) * 13 \
            + sum(hash(l) for l in self.labels) * 17 + hash_obj(self.asm_lines, return_int=True) * 31
    
    def __getstate__(self):
        """Print a warning about pickling singleton basic block objects"""
        warnings.warn("Attempting to pickle a singleton basic block object! This will mess up edges unless you know what you're doing!")
        return self._get_pickle_state()
    
    def __setstate__(self, state):
        """Print a warning about pickling singleton basic block objects"""
        warnings.warn("Attempting to unpickle a singleton basic block object! This will mess up edges unless you know what you're doing!")
        self._set_pickle_state(state)
    
    def _get_pickle_state(self):
        """Returns info of this CFGBasicBlock as a tuple"""
        edges_in = tuple((e.from_block.address, e.to_block.address, e.edge_type) for e in self.edges_in)
        edges_out = tuple((e.from_block.address, e.to_block.address, e.edge_type) for e in self.edges_out)
        return (self.address, edges_in, edges_out, self.asm_lines, self.labels)
    
    def _set_pickle_state(self, state):
        """Set the pickled state, looking at parent function.parent_cfg for info for building edges"""
        self.address, self._temp_edges_in, self._temp_edges_out, self.asm_lines, self.labels = state
        return self


def _get_edge_types(edge_types=None, as_set=False):
    """Gets the edge types passed by the user
    
    Args:
        edge_types (Union[EdgeType, str, Iterable[Union[EdgeType, str]], None], optional): either an edge type or an 
            iterable of edge types. Only edges with one of these types will be considered. Defaults to None.
        as_set (bool, optional): if True, will return the result as a set instead. Defaults to False.

    Raises:
        TypeError: on an unknown `edge_types`

    Returns:
        Union[List[EdgeType], Set[EdgeType]]: a list/set of EdgeType objects
    """
    ret_type = set if as_set else list

    if edge_types is None:
        return ret_type(e for e in EdgeType)
    else:
        # Attempt a single EdgeTypeLike first
        try:
            return ret_type([get_edge_type(edge_types)])
        except (ValueError, TypeError):
            try:
                return ret_type(get_edge_type(et) for et in edge_types)
            except Exception:
                raise TypeError("Could not get acceptable edge types from `edge_types` of type %s" % type(edge_types).__name__)
