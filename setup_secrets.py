"""
One-time setup: store Lakebase URL in a Databricks secret scope.

Usage (Databricks notebook / terminal with WorkspaceClient auth):
    python setup_secrets.py
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace
import getpass

w = WorkspaceClient()

existing = {s.name for s in w.secrets.list_scopes()}
if "database" not in existing:
    w.secrets.create_scope(scope="database")
    print("Created scope: database")
else:
    print("Scope already exists: database")

w.secrets.put_secret(
    scope="database",
    key="lakebase-url",
    string_value=getpass.getpass("Paste your Lakebase URL: "),
)
try:
    w.secrets.put_acl(
        scope="database",
        principal="users",
        permission=workspace.AclPermission.READ,
    )
    print("Granted READ ACL to principal 'users'")
except Exception as e:
    print(f"ACL note (safe to ignore if already set): {e}")

print("Done. Secret stored as database/lakebase-url")
