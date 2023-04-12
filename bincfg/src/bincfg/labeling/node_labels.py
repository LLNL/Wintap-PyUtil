import argparse
import numpy as np


NODE_LABELS = ['encryption', 'file_io', 'network_io', 'string_parser', 'error_handler']
"""List of currently available node labels"""

NODE_LABELS_INT_TO_STR = {i: l for i, l in enumerate(NODE_LABELS)}
NODE_LABELS_STR_TO_INT = {v: k for k, v in NODE_LABELS_INT_TO_STR.items()}

# A dictionary mapping integer values to their associated start/end nop label values
CLEAR_NODE_LABELS_VAL = 0
NODE_LABEL_INDEX_START = 1
RAW_LABEL_VAL_DICT = {CLEAR_NODE_LABELS_VAL: 'clear_node_labels'}
RAW_LABEL_VAL_DICT.update({i + NODE_LABEL_INDEX_START: (l + '_' + s) for i, (l, s) in 
                      enumerate(zip(np.repeat(NODE_LABELS, 2), np.tile(['start', 'end'], len(NODE_LABELS))))})

# Maps raw label start/end integers to their corresponding NODE_LABELS integer, ignoring the 'clear_node_labels' instruction
RAW_LABEL_TO_NODE_LABEL_INT = {v: NODE_LABELS_STR_TO_INT[l.rpartition('_')[0]] for v, l in RAW_LABEL_VAL_DICT.items() if v >= NODE_LABEL_INDEX_START}


def get_node_label(node_label):
    """Gets the node label value from the given node label string

    Args:
        node_label (str): the string node label to get the value of. Case-insentitive.

    Raises:
        ValueError: When an unknown node_label is passed

    Returns:
        int: integer node label value (index of given string `node_label` in ``NODE_LABELS`` list)
    """
    lower_node_label = node_label.lower()
    if lower_node_label not in NODE_LABELS:
        raise ValueError("Unknown node label: %s" % node_label)
    return NODE_LABELS.index(lower_node_label)


# Strings for generating the C/C++ header file to import for manual labellling
HEADER_STRING = """/* Automatically generated header file for manual basic block labelling.
This is currently only to be used with GCC family of compilers.
*/

#define __CLEAR_NODE_LABELS__ __asm__(\"nopw (0)\");\n

{labels}
"""
HEADER_FUNC_STRING = "#define __NODE_LABEL_{label}__ __asm__(\"nopw ({val})\");\n"
DEFAULT_HEADER_PATH = './NODE_LABEL_HEADER.h'
"""Default path to save header file"""


def generate_c_header_file(path=DEFAULT_HEADER_PATH):
    """Creates the C/C++ header file that will be imported in files that call our labelling functions.

    Args:
        path (str, optional): path to output file. Defaults to ``DEFAULT_HEADER_PATH``.
    """
    labels = "".join([HEADER_FUNC_STRING.format(label=label.upper(), val=val) for val, label in RAW_LABEL_VAL_DICT.items()])
    header_str = HEADER_STRING.format(labels=labels)
    with open(path, 'w') as f:
        f.write(header_str)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generates a .h file for label headers.')
    parser.add_argument('output_path', type=str, default=DEFAULT_HEADER_PATH, action='store', help='The path to the output file.')
    args = parser.parse_args()
    generate_c_header_file(args.output_path)
