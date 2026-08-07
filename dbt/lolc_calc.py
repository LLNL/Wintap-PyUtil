# This calculates the badness using LOLC: https://github.com/adobe/libLOL
# Setup is finicky:

# conda create -n lolc python=3.8
# conda activate lolc
# pip install lolc
# pip install scikit-learn==0.24.2


import duckdb
from duckdb.typing import VARCHAR
from lol.api import LOLC, PlatformType
lolc=LOLC(PlatformType.LINUX)

def is_bad(cmd:str) -> str:
    classification, tags = lolc([cmd])
    return classification[0]

con.create_function("lol_is_bad", is_bad)

sql='''
create table lolc_results
as
select *, lol_is_bad(concat_ws(' ',process_name, args)) lolc_sez
from (
    select p.process_name, p.args, list(distinct command_category) lolbas_cats, count(*) num_rows
    from main.process_uber_summary p
    join lolbas l on lower(l.filename)=p.process_name
    group by all
)
'''

res = con.sql(sql).fetchall()