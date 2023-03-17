'''
 Settings, imports and notebook specific functions
 Note: this module needs to be "%run" from notebooks to define imports/functions at the notebook scope.
'''

import os

import altair as alt
import pandas as pd
from dotenv import load_dotenv
from ipyfilechooser import FileChooser
from IPython.display import display

import stdviewutil as sv


def dataset_chooser():
    """
    Provide user a list of top level directories that *should* be datasets.
    Once they choose one, following cells can get the path with fc.selected_path
    """
    load_dotenv()
    path=os.getenv("DATAPATH")
    select_default=False
    if (os.getenv("DEFAULT_PATH")) != None:
        path=os.getenv("DEFAULT_PATH")
        select_default=True
    fc = FileChooser(path=path, select_default=select_default, title='<b>Select Wintap Dataset Path</b>', show_only_dirs=True)
    return fc


# Set jupyter options
pd.set_option("display.max_columns", None)
pd.set_option("display.max_colwidth", None)

# Set altair options
alt.data_transformers.disable_max_rows()
