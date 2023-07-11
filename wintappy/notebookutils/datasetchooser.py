"""
 Settings, imports and notebook specific functions
 Note: this module needs to be "%run" from notebooks to define imports/functions at the notebook scope.
"""

import os

from dotenv import load_dotenv
from ipyfilechooser import FileChooser


def dataset_chooser():
    """
    Provide user a list of top level directories that *should* be datasets.
    Once they choose one, following cells can get the path with fc.selected_path
    """
    load_dotenv()
    # Setup reasonable defaults if the user hasn't created an .env file
    path = os.path.expanduser("~")
    select_default = False
    if (os.getenv("DEFAULT_PATH")) != None and os.path.exists(os.getenv("DEFAULT_PATH")):
        path = os.getenv("DEFAULT_PATH")
        select_default = True
    elif (os.getenv("DATAPATH")) != None and os.path.exists(os.getenv("DATAPATH")):
        path = os.getenv("DATAPATH")
    else:
        print("Defaulting to your home dir. Check .env file.")

    fc = FileChooser(
        path=path,
        select_default=select_default,
        title="<b>Select Wintap Dataset Path</b>",
        show_only_dirs=True,
    )
    return fc


# Set jupyter options
# pd.set_option("display.max_columns", None)
# pd.set_option("display.max_colwidth", None)

# # Set altair options
# alt.data_transformers.disable_max_rows()

# # Load magic support for DuckDB
# ipython = get_ipython()
# ipython.run_line_magic("load_ext", "magic_duckdb")
