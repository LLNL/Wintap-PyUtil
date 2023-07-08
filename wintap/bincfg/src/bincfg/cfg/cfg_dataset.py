import os
import pickle
from collections import Counter
from .cfg import CFG
from ..normalization import normalize_cfg_data, get_normalizer
from ..normalization.base_normalizer import _Pickled_Normalizer
from ..utils import progressbar, isinstance_with_iterables, hash_obj, eq_obj


class CFGDataset:
    """A dataset of ``CFG``'s.

    Parameters
    ----------
    cfg_data: `Optional[Union[CFG, CFGDataset, Iterable]]`
        a ``CFG``, ``CFGDataset`` or iterable of ``CFG``'s or ``CFGDataset``'s to add to this dataset, or None to
        initialize this ``CFGDataset`` empty
    normalizer: `Optional[Union[str, Normalizer]]`
        if not None, then a normalizer to use. Will normalize all incoming ``CFG``'s if they do not already have the name 
        normalization (will attempt to renormalize incoming ``CFG``'s if they already have a normalization). Can be a 
        ``Normalizer`` object or string.
    load_path: `str`
        if not None, loads all files in this directory that end with '.txt' or '.dot'. Will raise an error if there are 
        no files. Will ignore any files that end with '.txt' or '.dot', but cannot be parsed.
    max_files: `Optional[int]`
        stops after loading this many files. If None, then there is no max
    allow_multiple_norms: `bool`
        by default, ``CFGDataset`` will only allow unnormalized cfg's when `normalizer=None` (if `normalizer` is not None, 
        then any normalized cfg added will be renormalized). Setting `allow_multiple_norms` to True will allow this 
        ``CFGDataset`` to store cfg data with any normalization method (assuming `normalizer=None`)
    progress: `bool`
        if True, will show a progressbar when loading cfg's from load_path
    metadata: `Optional[Dict]`
        a dictionary of metadata to attach to this CFGDataset
        NOTE: passed dictionary will be shallow copied
    add_data_kwargs: `Any`
        extra kwargs to pass to add_data while adding cfgs
    """

    cfgs = None
    """The list of all cfgs in this dataset"""

    normalizer = None
    """The normalizer used in this dataset, or None if there is no normalizer"""

    metadata = None
    """A dictionary of metadata associated with this ``CFGDataset``"""

    def __init__(self, cfg_data=None, normalizer=None, load_path=None, max_files=None, allow_multiple_norms=False, 
        progress=False, metadata=None, **add_data_kwargs):

        self.allow_multiple_norms = allow_multiple_norms
        self.normalizer = get_normalizer(normalizer) if normalizer is not None else None
        self.metadata = {} if metadata is None else metadata.copy()
        self.cfgs = []

        if cfg_data is not None:
            self.add_data(*cfg_data, progress=progress, **add_data_kwargs)

        # Load in files if needed
        if load_path is not None:
            files = list(sorted([f for f in os.listdir(load_path) if f.endswith('.txt') or f.endswith('.dot')]))
            if len(files) == 0:
                raise ValueError("No files found ending in '.txt' or '.dot'")
            if max_files is not None:
                files = files[:max_files]
            
            for file in progressbar(files, progress=progress):
                metadata = {'uid': file}
                self.add_data(CFG(os.path.join(load_path, file), metadata=metadata), progress=False, **add_data_kwargs)
    
    def add_data(self, *cfg_data, inplace=True, force_renormalize=False, progress=False):
        """Adds data to this dataset

        Args:
            cfg_data (Union[CFG, CFGDataset, Iterable]): arbitrary amount of ``CFG``/``CFGDataset``'s, or iterables of 
                them, to add to this dataset
            inplace (bool, optional): whether or not to normalize the incoming cfg_data inplace. Defaults to True.
            force_renormalize (bool, optional): by default, this method will only normalize cfg's whose 
                .normalizer != to this dataset's normalizer. However if `force_renormalize=True`, then all cfg's will 
                be renormalized even if they have been previously normalized with the same normalizer. Defaults to False.
            progress (bool, optional): if True, will show a progressbar when adding multiple cfgs. Defaults to False.

        Raises:
            TypeError: when attempting to add something that is not a ``CFG``, ``CFGDataset``, or iterables of them
            ValueError: when attempting to use multiple different normalizers and `self.allow_multiple_norms=False`
        """
        # Check that all elements in cfg_data are CFG's or CFGDataset's, or iterables of them
        temp = []
        for cfg in cfg_data:
            try:
                temp += isinstance_with_iterables(cfg, (CFG, CFGDataset), recursive=False, ret_list=True)
            except:
                raise TypeError("Can only add CFG's/CFGDataset's, or iterables of them, to CFGDataset, not '%s'" % type(cfg).__name__)
        cfg_data = temp

        # Check to see if we need to normalize
        if self.normalizer is not None:
            cfg_data = normalize_cfg_data(cfg_data, normalizer=self.normalizer, inplace=inplace, 
                force_renormalize=force_renormalize, convert_to_mem=False, unpack_cfgs=True, progress=progress)
        
        # Otherwise make sure normalizers are all None on data, and unpack cfgs
        else:
            temp_data = []
            for cfg in cfg_data:
                if cfg.normalizer is not None and not self.allow_multiple_norms:
                    raise ValueError("Found normalization '%s' on data to add to CFGDataset that has no normalizer" %
                        cfg.normalizer)
                temp_data += [cfg] if isinstance(cfg, CFG) else cfg.cfgs if isinstance(cfg, CFGDataset) else list(cfg)
            cfg_data = temp_data

        self.cfgs += cfg_data
    
    def normalize(self, normalizer=None, inplace=True, force_renormalize=False, progress=False):
        """Normalize this ``CFGDataset``.

        Args:
            normalizer (Union[str, Normalizer]): the normalizer to use. Can be a ``Normalizer`` object, or a 
                string, or None to use the default BaseNormalizer(). Defaults to None.
            inplace (bool, optional): by default, normalizes this dataset inplace (IE: without copying objects). Can set
                to False to return a copy. Defaults to True.
            force_renormalize (bool, optional): by default, this method will only normalize cfg's whose 
                .normalizer != to the passed normalizer. However if `force_renormalize=True`, then all cfg's will be 
                renormalized even if they have been previously normalized with the same normalizer.. Defaults to False.
            progress (bool, optional): if True, will show a progressbar while normalizing. Defaults to False.

        Returns:
            CFGDataset: this dataset normalized
        """
        return normalize_cfg_data(self, normalizer, inplace=inplace, force_renormalize=force_renormalize, progress=progress)
    
    @property
    def num_blocks(self):
        """Return total number of blocks across all cfg's"""
        return sum(cfg.num_blocks for cfg in self.cfgs)

    @property
    def num_functions(self):
        """Return total number of functions across all cfg's"""
        return sum(cfg.num_functions for cfg in self.cfgs)
        
    @property
    def num_edges(self):
        """Return total number of edges across all cfg's"""
        return sum(cfg.num_edges for cfg in self.cfgs)
        
    @property
    def num_asm_lines(self):
        """Return total number of assembly lines across all cfg's"""
        return sum(cfg.num_asm_lines for cfg in self.cfgs)
    
    @property
    def num_cfgs(self):
        """Return the number of cfgs in this dataset"""
        return len(self.cfgs)
    
    @property
    def asm_counts(self):
        """A collections.Counter() of all unique assembly lines and their counts accross all cfg's in this dataset"""
        return sum((cfg.asm_counts for cfg in self.cfgs), Counter())

    def __str__(self):
        stat_names = ["CFG's", 'Functions', 'Edges', 'Basic Blocks', 'Assembly Lines']
        c = [self.num_cfgs, self.num_functions, self.num_edges, self.num_blocks, self.num_asm_lines]
        stats = _get_stats(stat_names, c)
        norm = ('with normalizer: %s' % self.normalizer) if self.normalizer is not None else 'with no normalizer'
        return "%s %s\nStats:\n%s" % (self.__class__.__name__, norm, stats)

    def __repr__(self):
        return self.__str__()
    
    def __len__(self):
        return len(self.cfgs)
    
    def __getitem__(self, idx):
        return self.cfgs[idx]
    
    def __iter__(self):
        return iter(self.cfgs)
    
    def save(self, path):
        """Saves this CFGDataset to path"""
        with open(path, 'wb') as f:
            pickle.dump(self, f)
    
    @classmethod
    def load(cls, path):
        """Loads this CFGDataset from path"""
        with open(path, 'rb') as f:
            return pickle.load(f)
    
    def __getstate__(self):
        """State for pickling"""
        state = self.__dict__.copy()
        state['normalizer'] = _Pickled_Normalizer(state['normalizer'])
        return state
    
    def __setstate__(self, state):
        """State for unpickling"""
        state['normalizer'] = state['normalizer'].unpickle()
        for k, v in state.items():
            setattr(self, k, v)
        # NOTE: we do not set the normalizer on all cfg's since they may have different normalizers
    
    def __hash__(self):
        return sum(hash(c) for c in self.cfgs) * 17 + hash_obj(self.metadata, return_int=True) * 31
    
    def __eq__(self, other):
        return isinstance(other, CFGDataset) and all(eq_obj(self, other, selector=s) for s in ['normalizer', 'metadata']) \
            and eq_obj(self, other, selector='cfgs')


def _get_stats(stat_names, counts):
    """Returns a nicely-printable set of statistics for the CFGDataset

    Args:
        stat_names (Iterable[str]): the names for each statistic
        counts (Iterable[int]): the values for each statistic
    
    Returns:
        str: the nicely formatted set of statistics for the CFGDataset
    """
    return '\n'.join([('\t' + name + ': ' + str(c)) for name, c in zip(stat_names, counts)])
