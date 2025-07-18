import os
import dlt
from dlt.sources.sql_database import sql_database
from dlt.extract.incremental import Incremental
from dlt.sources.credentials import ConnectionStringCredentials
import requests
from msal import ConfidentialClientApplication

sql_conn_str = os.environ["MSSQL_CONN_STR"]
client_id = os.environ("FABRIC_CLIENT_ID")
client_secret = os.environ("FABRIC_CLIENT_SECRET")
tenant_id = os.environ("FABRIC_TENANT_ID")
workspace_id = os.environ("FABRIC_WORKSPACE_ID")
item_id = os.environ("FABRIC_ITEM_ID")


def run_dlt_pipeline():
    credentials = ConnectionStringCredentials(connection_string=sql_conn_str)
    # Initialize the source and resources
    source = sql_database(
        credentials=credentials,
        table_names=["Products", "Customers", "Orders"]
    )

    resources = [
        source.resources["Products"].apply_hints(
            write_disposition="merge",
            primary_key="product_id",
            incremental=Incremental("last_updated")
        ),
        source.resources["Customers"].apply_hints(
            write_disposition="merge",
            primary_key="customer_id",
            incremental=Incremental("last_updated")
        ),
        source.resources["Orders"].apply_hints(
            write_disposition="merge",
            primary_key="order_id",
            incremental=Incremental("last_updated")
        ),
    ]

    pipeline = dlt.pipeline(
        pipeline_name="mssql_tables",
        destination="filesystem",
        dataset_name="mssql_tables_dataset",
        progress="enlighten"
    )

    load_info = pipeline.run(
        resources,
        loader_file_format="parquet",
        table_format="delta"
    )

    print("DLT Load Complete:", load_info)
    return pipeline


def shortcut_exists(folder_name, workspace_id, item_id, headers):
    # Get base URL without query params for listing shortcuts
    list_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item_id}/shortcuts"
    resp = requests.get(list_url, headers=headers)
    resp.raise_for_status()
    shortcuts = resp.json().get("value", [])
    return any(s["name"].lower() == folder_name.lower() for s in shortcuts)

def create_shortcut_if_not_exists(folder_name, url, headers, workspace_id, item_id):
    if shortcut_exists(folder_name, workspace_id, item_id, headers):
        print(f"[Shortcut] Already exists: {folder_name}")
        return

    payload = {
        "path": "Tables/",
        "name": folder_name,
        "target": {
            "oneLake": {
                "workspaceId": workspace_id,
                "itemId": item_id,
                "path": f"Files/mssql_tables_dataset/{folder_name}"
            }
        }
    }

    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 201:
        print(f"[Shortcut] Created: {folder_name}")
    else:
        print(f"[Shortcut] Failed for {folder_name}: {response.status_code} - {response.text}")


def create_shortcuts():
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    scopes = ["https://analysis.windows.net/powerbi/api/.default"]

    app = ConfidentialClientApplication(client_id, authority=authority, client_credential=client_secret)
    token_response = app.acquire_token_for_client(scopes=scopes)
    access_token = token_response["access_token"]

    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item_id}/shortcuts?shortcutConflictPolicy=CreateOrOverwrite"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }


    dlt_table_folders = ["products", "customers", "orders"]

    for folder in dlt_table_folders:
        create_shortcut_if_not_exists(folder, url, headers, workspace_id, item_id)

def main():
    run_dlt_pipeline()
    create_shortcuts()

if __name__ == "__main__":
    main()
