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

# Testing KQL locally using the Kusto Emulator

Its possible to run a KQL engine in a docker container, using the Kusto Emulator, and then load the data. From there, you can connect via a python library and use either scripts or Jupyter.


```sh
docker pull mcr.microsoft.com/azuredataexplorer/emulator:latest
docker run -p 8080:8080 mcr.microsoft.com/azuredataexplorer/emulator:latest
```

# **References**

## **Microsoft Defender Advanced Hunting and Schemas**

| Topic                                    | URL                                                                                                                           | Description                                |
|------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------|
| Defender XDR Main Page               | [https://learn.microsoft.com/en-us/defender-xdr/](https://learn.microsoft.com/en-us/defender-xdr/) | Main landing page for Defender XDR(?) |
| Advanced Hunting Schema Tables           | [https://learn.microsoft.com/en-us/microsoft-365/security/defender/advanced-hunting-schema-tables?view=o365-worldwide](https://learn.microsoft.com/en-us/microsoft-365/security/defender/advanced-hunting-schema-tables?view=o365-worldwide) | Full list of schema tables                  |

## **Kusto KQL Emulator**

| Reference Topic                   | Link                                                                 |
|-----------------------------------|----------------------------------------------------------------------|
| Kusto Emulator Overview           | [Emulator Overview](https://learn.microsoft.com/en-us/azure/data-explorer emulator-overview) |
| Kusto Emulator in Docker          | [Emulator Docker Guide](https://learn.microsoft.com/en-us/azure/data-explorer/kusto-emulator-install) |

## **Kqlmagic Source Code**

| Topic            | URL                                                                          | Description                        |
|------------------|------------------------------------------------------------------------------|------------------------------------|
| Kqlmagic GitHub  | [https://github.com/Microsoft/jupyter-Kqlmagic](https://github.com/Microsoft/jupyter-Kqlmagic) | Source code repository for Kqlmagic |

## **Connecting to Kusto Emulator**

| GUI Option                | Mac Support | How to Connect                                      |
|---------------------------|-------------|-----------------------------------------------------|
| Kusto Explorer            | No          | Windows only; VM required                           |
| Azure Data Explorer Web   | Yes         | Use http://localhost:8080 as cluster URI            |
| Python SDK + Jupyter/VSCode| Yes        | Use SDK to connect to emulator                      |
| VS Code Kusto Extension   | Yes         | Connect to emulator via extension settings          |
