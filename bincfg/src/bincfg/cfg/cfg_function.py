import warnings
import sys
from collections import Counter
from .cfg_basic_block import CFGBasicBlock
from ..utils import eq_obj


class CFGFunction:
    """A single function in a ``CFG``

    Can be initialized empty, or by passing kwarg values.

    NOTE: these objects should not be pickled/copy.deepcopy()-ed by themselves, only as a part of a cfg

    Parameters
    ----------
    parent_cfg: `CFG`
        the parent ``CFG`` object to which this ``CFGFunction`` belongs
    address: `Union[str, int, Addressable]`
        the memory address of this function
    name: `Optional[str]`
        the string name of this function, or None if it doesn't have one
    blocks: `Optional[Iterable[CFGBasicBlock]]`
        if None, will be initialized to an empty list, otherwise an iterable of ``CFGBasicBlock`` objects that are within 
        this function
    is_extern_func: `bool`
        if True, then this function is an external function (a dynamically loaded function)
    """

    parent_cfg = None
    """the parent ``CFG`` object to which this ``CFGFunction`` belongs"""

    address = None
    """the integer memory address of this function"""

    name = None
    """the string name of this function, or ``None`` if it doesn't have a name"""

    blocks = None
    """list of all basic blocks in this function"""

    def __init__(self, parent_cfg=None, address=None, name=None, blocks=None, is_extern_func=False):
        self.parent_cfg = parent_cfg
        self.name = name
        self.address = address
        self.blocks = [] if blocks is None else list(blocks)
        self._is_extern_function = is_extern_func
    
    @property
    def num_blocks(self):
        """The number of basic blocks in this function"""
        return len(self.blocks)
    
    @property
    def num_asm_lines(self):
        """The total number of assembly lines across all blocks in this function"""
        return sum(b.num_asm_lines for b in self.blocks)
    
    @property
    def asm_counts(self):
        """A ``collections.Counter`` of all unique assembly lines and their counts in this function"""
        return sum((b.asm_counts for b in self.blocks), Counter())
    
    @property
    def is_root_function(self):
        """True if this function is not called by any other functions, False otherwise"""
        return len(self.called_by) == 0
    
    @property
    def is_recursive(self):
        """True if this function calls itself at some point
        
        Specifically, if at least one ``CFGBasicBlock`` in this ``CFGFunction.blocks`` list has an `edges_out` function
        call address that is equal to this ``CFGFunction``'s address
        """
        return self.address is not None and any(block.calls(self.address) for block in self.blocks)
    
    @property
    def is_extern_function(self):
        """True if this function is an external function, False otherwise"""
        return self._is_extern_function
    
    @property
    def is_intern_function(self):
        """True if this function is an internal function, False otherwise"""
        return not self._is_extern_function
    
    @property
    def function_entry_block(self):
        """The ``CFGBasicBlock`` that is the function entry block
        
        Specifically, returns the first ``CFGBasicBlock`` found that has the same address as this function (there ~should~
        only be one as each basic block ~should~ have a unique memory address)
        """
        for block in self.blocks:
            if block.address == self.address:
                return block
        raise ValueError("Function %s does not have an entry block!" % (repr(self.name) if self.name is not None else 'with no name'))
    
    @property
    def called_by(self):
        """A list of ``CFGBasicBlock``'s that call this function
        
        Specifically, the list of all ``CFGBasicBlock`` objects in this ``CFGFunction.parent_cfg`` cfg object that call 
        this function. If this ``CFGFunction`` has no parent, then the empty list will be returned.
        
        NOTE: this is computed dynamically each call (as ``CFG`` objects are mutable), so it may be useful to compute it
        once per function and save it if needed
        """
        if self.parent_cfg is None:
            return list()

        return [block for block in self.parent_cfg.blocks if block.calls(self)]
    
    @property
    def nice_name(self):
        """Returns the name of this function, returning ("__UNNAMED_FUNC_%d" % self.address) if the name is None"""
        return self.name if self.name is not None and self.name != '' else ("__UNNAMED_FUNC_%d" % self.address)
    
    @property
    def symbol_name(self):
        """Returns the symbol name of this function if it is an external function, or None if it isnt or the name is None"""
        return None if self.name is None or not self.is_extern_function else self.name.rpartition('@plt')[0]
    
    @property
    def num_fc_edges(self):
        """Returns the number of function call edges in/out of this function"""
        return sum(sum(len(s) for s in b.get_sorted_edges(edge_types='function', direction=None)) for b in self.blocks)
    
    def __str__(self):
        un_funcs = set([self.parent_cfg.get_block(b).parent_function.address for b in self.called_by])
        include_self = " (including self)" if self.is_recursive else ""
        extra_str = ", called by %d basic blocks across %d functions%s" % (len(self.called_by), len(un_funcs), include_self)
        addr_str = ("0x%08x" % self.address) if self.address is not None else "NO_ADDRESS"

        func_name_str = ("'%s'" % self.name) if self.name is not None else "with no name"
        num_asm_lines = sum([len(b.asm_lines) for b in self.blocks])
        return "CFGFunction %s %s at %s with %d blocks, %d assembly lines%s" \
            % ('externfunc' if self.is_extern_function else 'innerfunc', func_name_str, addr_str, len(self.blocks), 
                num_asm_lines, extra_str)
    
    def __repr__(self):
        return str(self)
    
    def __eq__(self, other):
        if not isinstance(other, CFGFunction) or len(self.blocks) != len(other.blocks) or \
            not all(eq_obj(self, other, selector=s) for s in ['address', 'name', 'is_extern_function']):
            return False
        
        # Check all the blocks are the same using a dictionary since sets don't work for some reason...
        d_self, d_other = {}, {}
        for blocks, count_dict in [(self.blocks, d_self), (other.blocks, d_other)]:
            for b in blocks:
                _hash = hash(b)
                if _hash not in count_dict:
                    count_dict[_hash] = (1, b)
                else:
                    count_dict[_hash] = (count_dict[_hash][0] + 1, b)
        
        return eq_obj(d_self, d_other)
    
    def __hash__(self):
        return hash(self.name) * 7 + hash(self.address) * 9 + hash(self._is_extern_function) * 11 + sum(hash(b) for b in self.blocks) * 31

    def __getstate__(self):
        """Print a warning about pickling singleton function objects"""
        warnings.warn("Attempting to pickle a singleton function object! This will mess up edges unless you know what you're doing!")
        return self._get_pickle_state()
    
    def __setstate__(self, state):
        """Print a warning about pickling singleton function objects"""
        warnings.warn("Attempting to unpickle a singleton function object! This will mess up edges unless you know what you're doing!")
        self._set_pickle_state(state)
    
    def _get_pickle_state(self):
        """Returns info of this CFGFunction as a tuple"""
        return (self.address, self.name, tuple(b._get_pickle_state() for b in self.blocks), self.is_extern_function)

    def _set_pickle_state(self, state):
        """Sets state from _get_pickle_state"""
        self.address, self.name, blocks, self._is_extern_function = state
        self.blocks = [CFGBasicBlock(parent_function=self)._set_pickle_state(b) for b in blocks]
        return self
