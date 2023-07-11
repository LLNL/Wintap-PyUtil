import altair as alt
import pandas as pd
from IPython import get_ipython

# Set jupyter options
pd.set_option("display.max_columns", None)
pd.set_option("display.max_colwidth", None)

# Set altair options
alt.data_transformers.disable_max_rows()

# Load magic support for DuckDB
ipython = get_ipython()
ipython.run_line_magic("load_ext", "magic_duckdb")

from .datasetchooser import dataset_chooser
