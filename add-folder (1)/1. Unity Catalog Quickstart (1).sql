-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Unity Catalog Quickstart (SQL)
-- MAGIC
-- MAGIC This notebook provides an example workflow for getting started with Unity Catalog by showing how to do the following:
-- MAGIC
-- MAGIC - Choose a catalog and creating a new schema.
-- MAGIC - Create a managed table and adding it to the schema.
-- MAGIC - Query the table using the the three-level namespace.
-- MAGIC - Manage data access permissions on the tables.
-- MAGIC
-- MAGIC ## Requirements
-- MAGIC
-- MAGIC - Unity Catalog is set up on the workspace. See https://docs.databricks.com/data-governance/unity-catalog/get-started.html.
-- MAGIC - Notebook is attached to a cluster that uses DBR 10.3 or higher and and uses the User Isolation cluster security mode.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Three-level namespace
-- MAGIC
-- MAGIC Unity Catalog provides a three-level namespace for organizing data: catalogs, schemas (also called databases), and tables and views. To refer to a table, use the following syntax
-- MAGIC
-- MAGIC >`<catalog>.<schema>.<table>`
-- MAGIC
-- MAGIC If you already have data in a workspace's local Hive metastore or an external Hive metastore, Unity Catalog is **additive**: the workspace’s Hive metastore becomes one catalog within the 3-layer namespace (called `hive_metastore`) and tables in the Hive metastore can be accessed using three-level namespace notation: `hive_metastore.<schema>.<table>`.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create a new catalog
-- MAGIC
-- MAGIC Each Unity Catalog metastore contains a default catalog named `main` with an empty schema called `default`.
-- MAGIC
-- MAGIC To create a new catalog, use the `CREATE CATALOG` command. You must be a metastore admin to create a new catalog.
-- MAGIC
-- MAGIC The following commands show how to:
-- MAGIC
-- MAGIC - Create a new catalog.
-- MAGIC - Select a catalog.
-- MAGIC - Show all catalogs.
-- MAGIC - Grant permissions on a catalog.
-- MAGIC - Show all grants on a catalog.

-- COMMAND ----------

--- create a new catalog
CREATE CATALOG IF NOT EXISTS quickstart_catalog;

-- COMMAND ----------

-- Set the current catalog
USE CATALOG quickstart_catalog;

-- COMMAND ----------

--- Show all catalogs in a metastore
SHOW CATALOGS;

-- COMMAND ----------

--- Grant create & usage permissions to all users on the account
--- This also works for other account-level groups and individual users
GRANT CREATE, USAGE
ON CATALOG quickstart_catalog
TO `account users`;

-- COMMAND ----------

--- Check grants on the quickstart catalog
SHOW GRANT ON CATALOG quickstart_catalog;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create and manage schemas
-- MAGIC Schemas, also referred to as databases, are the second layer of the Unity Catalog namespace. They logically organize tables and views.

-- COMMAND ----------

--- Create a new schema in the quick_start catalog
CREATE SCHEMA IF NOT EXISTS quickstart_schema
COMMENT "A new Unity Catalog schema called quickstart_schema";

-- COMMAND ----------

-- Show schemas in the selected catalog
SHOW SCHEMAS;

-- COMMAND ----------

thtrhr -- Describe a schemahgfhfg
DESCRIBE SCHEMA EXTENDED quickstart_schema;

-- COMMAND ----------

-- Drop a schema (uncomment the following line to try it. Be sure to re-create the schema before continuing with the rest of the notebook.)
-- DROP SCHEMA quickstart_schema CASCADE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create a managed table
-- MAGIC
-- MAGIC *Managed tables* are the default way to create table with Unity Catalog. If no `LOCATION` is included, a table is created in the managed storage location configured for the metastore.
-- MAGIC
-- MAGIC The following commands show how to:
-- MAGIC - Select a schema.
-- MAGIC - Create a managed table and insert records into it.
-- MAGIC - Show all tables in a schema.
-- MAGIC - Describe a table.

-- COMMAND ----------

-- Set the current schema
USE quickstart_schema;

-- COMMAND ----------

-- Create a managed Delta table and insert two records
CREATE TABLE IF NOT EXISTS quickstart_table
  (columnA Int, columnB String) PARTITIONED BY (columnA);

INSERT INTO TABLE quickstart_table
VALUES
  (1, "one"),
  (2, "two");

-- COMMAND ----------

-- View all tables in the schema
SHOW TABLES IN quickstart_schema;

-- COMMAND ----------

-- Describe this table
DESCRIBE TABLE EXTENDED quickstart_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC With the three level namespaces you can access tables in several different ways:
-- MAGIC - Access the table with a fully qualified name.
-- MAGIC - Select a default catalog and access the table using the schema and table name.
-- MAGIC - Select a default schema and use the table name.
-- MAGIC
-- MAGIC The following three commands are functionally equivalent.

-- COMMAND ----------

-- Query the table using the three-level namespace
SELECT
  *
FROM
  quickstart_catalog.quickstart_schema.quickstart_table;

-- COMMAND ----------

-- Set the default catalog and query the table using the schema and table name
USE CATALOG quickstart_catalog;
SELECT *
FROM quickstart_schema.quickstart_table;

-- COMMAND ----------

-- Set the default catalog and default schema and query the table using the table name
USE CATALOG quickstart_catalog;
USE quickstart_schema;
SELECT *
FROM quickstart_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Drop Table
-- MAGIC If a *managed table* is dropped with the `DROP TABLE` command, the underlying data files are removed as well. 
-- MAGIC
-- MAGIC If an *external table* is dropped with the `DROP TABLE` command, the metadata about the table is removed from the catalog but the underlying data files are not deleted. 

-- COMMAND ----------

-- Drop the managed table. Uncomment the following line to try it out. Be sure to re-create the table before continuing.
-- DROP TABLE quickstart_catalog.quickstart_database.quickstart_table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Manage permissions on data
-- MAGIC
-- MAGIC You use `GRANT` and `REVOKE` statements to manage access to your data. Unity Catalog is secure by default, and access to data is not automatically granted. Initially, all users have no access to data. Metastore admins and data owners can grant and revoke access to account-level users and groups. Grants are not recursive; you must explicitly grant permissions on the catalog, schema, and table or view.
-- MAGIC
-- MAGIC #### Ownership
-- MAGIC Each object in Unity Catalog has an owner. The owner can be any account-level user or group, called a *principals*. A principal becomes the owner of a securable object when they create it or when ownership is transferred by using an `ALTER` statement.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Manage privileges
-- MAGIC The following privileges can be granted on Unity Catalog objects:
-- MAGIC - `SELECT`: Allows the grantee to read data from a table or view.
-- MAGIC - `MODIFY`: Allows the grantee to insert, update and delete data to or from a table.
-- MAGIC - `CREATE`: Allows the grantee to create child securable objects within a catalog or schema.
-- MAGIC - `USAGE`: This privilege does not grant access to the securable objectitself, but allows the grantee to traverse the securable object in order to access its child objects. For example, to select data from a table, a user needs the `SELECT` privilege on that table and the `USAGE` privilege on its parent schema and parent catalog. You can use this privilege to restrict access to sections of your data namespace to specific groups.
-- MAGIC
-- MAGIC Three additional privileges are relevant only to external tables and external storage locations that contain data files. This notebook does not explore these privileges.
-- MAGIC
-- MAGIC - `CREATE TABLE`: Allows the grantee to create an external table at a given external location.
-- MAGIC - `READ FILES`: Allows the grantee to read data files from a given external location.
-- MAGIC - `WRITE FILES`: Allows the grantee to write data files to a given external location.
-- MAGIC
-- MAGIC Privileges are NOT inherited on child securable objects. Granting a privilege on a securable DOES NOT grant the privilege on its child securables.
-- MAGIC
-- MAGIC The following commands show how to:
-- MAGIC - Grant privileges.
-- MAGIC - Show grants on a securable object.
-- MAGIC - Revoke a privilege.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Grant a privilege**: Grants a privilege on securable(s) to a principal. Only Metastore Admin and owners of the securable can perform privilege granting.

-- COMMAND ----------

-- Grant USAGE on a schema
GRANT USAGE
ON SCHEMA quickstart_schema
TO `account users`;

-- COMMAND ----------

-- Grant SELECT privilege on a table to a principal
GRANT SELECT
ON TABLE quickstart_schema.quickstart_table
TO `account users`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Show grants**: Lists all privileges that are granted on a securable object.

-- COMMAND ----------

-- Show grants on quickstart_table
SHOW GRANTS
ON TABLE quickstart_catalog.quickstart_database.quickstart_table;

-- COMMAND ----------

-- Show grants on quickstart_schema
SHOW GRANTS
ON SCHEMA quickstart_catalog.quickstart_schema;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Revoke a privilege**: Revokes a previously granted privilege on a securable object from a principal.

-- COMMAND ----------

REVOKE SELECT
ON TABLE quickstart_database.quickstart_table
FROM `account users`;

-- COMMAND ----------

CREATE TABLE main.default.department
 (
    deptcode   INT,
    deptname  STRING,
    location  STRING
 );

INSERT INTO main.default.department VALUES
  (10, 'FINANCE', 'EDINBURGH'),
  (20, 'SOFTWARE', 'PADDINGTON'),
  (30, 'SALES', 'MAIDSTONE'),
  (40, 'MARKETING', 'DARLINGTON'),
  (50, 'ADMIN', 'BIRMINGHAM');

-- COMMAND ----------


