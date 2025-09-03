# Install DBT

- `pip install dbt-duckdb`
- Deprecate prettytable
    - `pip3 install "prettytable==3.11.0"`
    - Ref: https://github.com/microsoft/jupyter-Kqlmagic/issues/114


# Convert Wintap to XDR

- Copy `local_vars.yml-example` to `local_vars.yml` and edit the file paths as needed.

- Using DBT to create the XDR format. This will create a duckdb database in the current directory and write parquet files to the `xdr` directory inside the data_set path.
    - `dbt run --vars "$(cat local_vars.yml)"`

- Run just the tests
    - `dbt test --vars "$(cat local_vars.yml)"`

- Generate documentaion
    - `dbt generate --vars "$(cat local_vars.yml)"`

- Serve documentaion, this will open a local browser
    - `dbt serve --vars "$(cat local_vars.yml)"`

## Testing KQL locally

### Install kusto.emulator
### Use JupyterLab to interact



### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
