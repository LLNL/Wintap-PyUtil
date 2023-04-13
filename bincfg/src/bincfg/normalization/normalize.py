"""
Provides function(s) to perform normalization techniques on CFG's
"""

import copy
import numpy as np
from ..utils import progressbar, update_memcfg_tokens, AtomicTokenDict
from .builtin_normalizers import get_normalizer
import bincfg


def normalize_cfg_data(cfg_data, normalizer, inplace=False, using_tokens=None, force_renormalize=False, convert_to_mem=False, 
    unpack_cfgs=False, progress=False):
    """Normalizes some cfg data.

    Args:
        cfg_data (Union[CFG, MemCFG, CFGDataset, MemCFGDataset, Iterable]): some cfg data. Can be either: CFG, MemCFG, 
            CFGDataset, MemCFGDataset, or iterable of previously mentioned types. Will return the same type as that passed.
        normalizer (Union[str, Normalizer]): the normalizer to use. Can be either a ``Normalizer`` class with a 
            `.normalize()` method, or a string to use a built-in normalizer. See :func:`bincfg.normalization.get_normalizer`
            for acceptable strings.
        inplace (bool, optional): if True, will modify data in-place instead of creating new objects. Defaults to False.
            NOTE: if `inplace=False`, and the incoming data has already been normalized with the passed `normalizer`, then
            the original cfg will be returned, NOT a copy.
        using_tokens (TokenDictType, optional): only used for ``MemCFG``'s. If not None, then a dictionary mapping string
            tokens to integer token values that will be used as any ``MemCFG``'s tokens. Defaults to None.
        force_renormalize (bool, optional): by default, this method will only normalize cfg's whose .normalizer != to the passed
            normalizer. However if `force_renormalize=True`, then all cfg's will be renormalized even if they have been
            previously normalized with the same normalizer. Defaults to False.
        convert_to_mem (bool, optional): if True, will convert all ``CFG``'s and ``CFGDatasets`` to their memory-efficient 
            versions after normalizing. Defaults to False.
        unpack_cfgs (bool, optional): by default, this method will return the same types that were passed to be normalized. 
            However if `unpack_cfgs=True`, then instead, a list of all cfgs unpacked (EG: unpacked from lists, and pulled 
            out of datasets) will be returned. Defaults to False.
            NOTE: if only a single ``CFG``/``MemCFG`` was passed, a list will still be returned of only that single element.
        progress (bool, optional): if True, will show a progressbar for normalizations of multiple cfg's. Defaults to False.

    Raises:
        TypeError: Unknown input `cfg_data` type(s)

    Returns:
        Union[CFG, MemCFG, CFGDataset, MemCFGDataset, List, Tuple]: the normalized data
    """
    normalizer = get_normalizer(normalizer)

    # If input is not a CFG/MemCFG/dataset, then it should be an iterable, loop or use multiprocessing
    if not isinstance(cfg_data, (bincfg.CFG, bincfg.MemCFG, bincfg.CFGDataset, bincfg.MemCFGDataset)):
        cfg_data = list(cfg_data)  # Copy the element references so we don't modify the original iterable, and convert to list
        cfgs, cfg_parts = _unpack_cfgs(cfg_data)

        # Normalize all the cfg's
        for i, cfg in enumerate(progressbar(cfgs, progress=progress)):
            cfgs[i] = normalize_cfg_data(cfg, normalizer=normalizer, inplace=inplace, using_tokens=using_tokens,
                force_renormalize=force_renormalize, convert_to_mem=convert_to_mem, progress=False)
        
        # Now that all our cfg's have been normalized, sort them back to where they belong (if needed)
        if not unpack_cfgs:
            for i, (start, end) in enumerate(cfg_parts):

                # Append the data if it is just a cfg, otherwise make it into a dataset
                if isinstance(cfg_data[i], (bincfg.CFG, bincfg.MemCFG)):
                    cfg_data[i] = cfgs[start]
                
                # Otherwise make it into a dataset
                else:
                    if isinstance(cfg_data[i], bincfg.CFGDataset) and convert_to_mem:
                        cfg_data[i] = bincfg.MemCFGDataset()
                    elif not inplace:  # Create a new object if not inplace
                        cfg_data[i] = cfg_data[i].__class__()

                    cfg_data[i].cfgs = cfgs[start:end]
                    cfg_data[i].normalizer = normalizer
            return cfg_data

        return cfgs

    # Check if we even need to normalize
    if cfg_data.normalizer is not None and normalizer == cfg_data.normalizer and not force_renormalize:
        # Check if we need to unpack cfgs, and/or convert to memcfg's
        if convert_to_mem:
            if isinstance(cfg_data, bincfg.CFG):
                cfg_data = bincfg.MemCFG(cfg_data, using_tokens=using_tokens)
            elif isinstance(cfg_data, bincfg.CFGDataset):
                cfg_data = bincfg.MemCFGDataset(cfg_data, tokens=using_tokens)
            
            # Otherwise it is already a MemCFG/MemCFGDataset, and we need to check if we should update the tokens
            elif using_tokens is not None:
                update_memcfg_tokens(cfg_data, using_tokens)
        
        # Otherwise, check if this is already a memcfg/memcfgdataset and we need to update them tokens
        elif using_tokens is not None:
            if isinstance(cfg_data, (bincfg.MemCFG, bincfg.MemCFGDataset)):
                update_memcfg_tokens(cfg_data, using_tokens)

        if unpack_cfgs:
            if isinstance(cfg_data, (bincfg.CFGDataset, bincfg.MemCFGDataset)):
                cfg_data = cfg_data.cfgs
            else:
                cfg_data = [cfg_data]

        return cfg_data

    # Create return object depending on input type and inplace, set the return object's normalizer
    ret = cfg_data if inplace else copy.deepcopy(cfg_data) if isinstance(cfg_data, (bincfg.CFG, bincfg.MemCFG)) else cfg_data.__class__()
    ret.normalizer = normalizer
    
    # Single CFG's and MemCFG's will be normalized here
    if isinstance(cfg_data, bincfg.CFG):
        for block in ret.blocks:
            # Check for non-normalized assembly lines at each block, or if they have been normalized already and are lists
            pre_normed = len(block.asm_lines) > 0 and not isinstance(block.asm_lines[0][1], str)
            block.asm_lines = [(a, normalizer.normalize(*(l if pre_normed else [l]), cfg=ret, block=block)) for a, l in block.asm_lines]

        # Convert to MemCFG if needed
        if convert_to_mem and isinstance(cfg_data, bincfg.CFG):
            ret = bincfg.MemCFG(ret, inplace=True, using_tokens=using_tokens)

    elif isinstance(cfg_data, bincfg.MemCFG):
        # Need to recompute the asm_lines, block_asm_idx, and tokens
        # Need to start new_block_asm_idx with a 0
        norm_lines, new_block_asm_idx = [], [0]
        inv_tokens = {v: k for k, v in ret.tokens.items()}

        # Go through each block re-normalizing its lines, and keeping track of the block_asm_idx
        for block_idx in range(ret.num_blocks):
            norm_lines += normalizer.normalize(*[inv_tokens[t] for t in ret.get_block_asm_lines(block_idx)], cfg=ret, block=block_idx)
            new_block_asm_idx.append(len(norm_lines))
        
        # Update the new tokens if needed, allowing for use of atomic tokens
        if isinstance(using_tokens, AtomicTokenDict):
            using_tokens.addtokens({t for t in norm_lines if t not in using_tokens})
            new_tokens = using_tokens

        else:
            new_tokens = {} if using_tokens is None else using_tokens
            for t in norm_lines:
                new_tokens.setdefault(t, len(new_tokens))
        
        # Convert to integer tokens
        new_asm_lines = [new_tokens[t] for t in norm_lines]

        # Set the new data in ret
        ret.asm_lines, ret.block_asm_idx, ret.tokens = np.array(new_asm_lines), np.array(new_block_asm_idx), new_tokens
    
    # Datasets will have their cfg's normalized as a list, then will need their tokens recounted
    elif isinstance(cfg_data, (bincfg.CFGDataset, bincfg.MemCFGDataset)):
        using_tokens = None if isinstance(cfg_data, bincfg.CFGDataset) else using_tokens if using_tokens is not None else {}
        ret.cfgs = normalize_cfg_data(cfg_data.cfgs, normalizer=normalizer, inplace=inplace, using_tokens=using_tokens, 
            progress=progress)
        
        if isinstance(cfg_data, bincfg.MemCFGDataset):
            ret.tokens = using_tokens

        # Convert to MemCFGDataset if needed
        if convert_to_mem and isinstance(cfg_data, bincfg.CFGDataset):
            ret = bincfg.MemCFGDataset(ret, normalizer=normalizer, tokens=ret.tokens)
    
    else:
        raise TypeError("Got an unknown type: '%s'" % type(cfg_data).__name__)
    
    # Unpack cfg's if needed
    if unpack_cfgs:
        return [ret] if isinstance(ret, (bincfg.CFG, bincfg.MemCFG)) else ret.cfgs

    return ret


def _unpack_cfgs(cfg_data):
    """Helper to unpack lists/tuples of cfg's/datasets's, and return their type info
    
    :param cfg_data: 
    :return: 

    Args:
        cfg_data (Union[CFG, MemCFG, CFGDataset, MemCFGDataset, Iterable]): some cfg data. Can be either: CFG, MemCFG, 
            CFGDataset, MemCFGDataset, or iterable of previously mentioned types.

    Returns:
        Tuple[List[Union[bincfg.CFG, bincfg.MemCFG]], List[Tuple[int, int]]]: a 2-tuple of `(cfgs, type_info)`, where 
            `cfgs` is a list of CFG's/MemCFG's containing all of the cfgs that exist in cfg_data in order, and `type_info` 
            is a list of 2-tuples of (start_idx, end_idx) where: `start_idx` is the integer start index of this chunk of 
            data in the `cfgs` list, and `end_idx` is the end index of this chunk of data
    """
    cfgs, cfg_parts = [], []

    # If the user didn't pass an iterable, make it one
    if isinstance(cfg_data, (bincfg.CFG, bincfg.MemCFG, bincfg.CFGDataset, bincfg.MemCFGDataset)):
        cfg_data = [cfg_data]
    
    start_idx = 0
    for cfgd in cfg_data:
        
        # CFG's/MemCFG's can simply be added, but Dataset's must have their cfgs lists added
        if isinstance(cfgd, (bincfg.CFG, bincfg.MemCFG)):
            cfgs.append(cfgd)
        elif isinstance(cfgd, (bincfg.CFGDataset, bincfg.MemCFGDataset)):
            cfgs += cfgd.cfgs

        # Update the start_idx and end_idx
        cfg_parts.append((start_idx, len(cfgs)))
        start_idx = len(cfgs)
    
    return cfgs, cfg_parts
