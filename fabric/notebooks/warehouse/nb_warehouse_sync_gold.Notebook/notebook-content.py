# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a8c50827-f4d9-41c2-bc56-e0ba1bb4c743",
# META       "default_lakehouse_name": "lh_health_analytics",
# META       "default_lakehouse_workspace_id": "b708d45d-9bec-4a98-858e-dbf20c51650c",
# META       "known_lakehouses": [
# META         {
# META           "id": "a8c50827-f4d9-41c2-bc56-e0ba1bb4c743"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 🏛️ [WAREHOUSE] `nb_warehouse_sync_gold` — Synchronisation T-SQL Atomique (Polaris)
# 
# > **HealthTek** · Plateforme analytique des ruptures et tensions d'approvisionnement de médicaments (données publiques BDPM / ANSM / HAS) — Microsoft Fabric
# 
# | Élément | Détail |
# |---|---|
# | **Couche** | Warehouse |
# | **Type** | Notebook exécutable |
# | **Orchestration** | Activité `run_warehouse` (si `p_skip_warehouse = false`) — pipeline `pl_master_health_pipeline` |
# | **Entrées** | Tables Gold via le point de terminaison SQL du Lakehouse |
# | **Sorties** | `wh_health_analytics.dbo.gold_*` |
# | **Idempotence** | Oui (TRUNCATE + INSERT transactionnel par table) |
# | **Documentation** | v3.1 — 10/11/2026 |
# 
# ## 🎯 Objectif
# 
# Génère dynamiquement les scripts DDL et DML T-SQL à partir des schémas réels des 17 tables Delta Gold, exécute chaque transaction `TRUNCATE + INSERT ... SELECT` depuis le point de terminaison SQL du Lakehouse vers `wh_health_analytics`, puis vérifie l'absence d'orphelins et de doublons.
# 
# ## 🔄 Flux de données
# 
# ```
# SQL Analytics Endpoint (lh_health_analytics) ──► T-SQL Transactionnel ──► Fabric Warehouse (wh_health_analytics)
# ```
# 
# ## ⚙️ Paramètres
# 
# La cellule taguée `parameters` contient les valeurs par défaut utilisées en exécution interactive ; le pipeline les remplace à chaque run.
# 
# | Paramètre | Défaut | Rôle |
# |---|---|---|
# | `run_id` | `"manual"` | Identifiant du run transmis par `@pipeline().RunId` ; tracé dans toutes les colonnes `_pipeline_run_id`. |
# | `run_date` | `""` | Date de référence ISO `AAAA-MM-JJ` ; vide = date du jour UTC (permet le rejeu historique). |
# | `stage` | `""` | Nom de l'étape d'orchestration (journalisation). |
# | `warehouse` | `"wh_health_analytics"` | Warehouse cible. |
# | `lakehouse` | `"lh_health_analytics"` | Lakehouse source. |
# 
# ## 🧭 Plan du notebook
# 
# 1. Chargement des utilitaires
# 2. Paramètres
# 3. Génération T-SQL, connexion et synchronisation
# 4. Exécution transactionnelle
# 5. Contrôles d'intégrité et sortie JSON
# 
# ## 📤 Contrat de sortie
# 
# `{"status","tables","verification":{...}}`

# MARKDOWN ********************

# ## 1 · Chargement des utilitaires partagés
# 
# Les notebooks `nb_util_config`, `nb_util_logging`, `nb_util_io` sont exécutés dans la même session Spark (`%run`) : configuration centrale, journalisation, accès Lakehouse. Chaque cellule `%run` doit rester seule dans sa cellule.

# CELL ********************

%run nb_util_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_util_logging

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_util_io

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2 · Paramètres d'exécution
# 
# Cellule taguée **`parameters`** : ne pas renommer les variables, elles sont injectées par l'activité notebook du pipeline (voir le tableau en tête de notebook).

# PARAMETERS CELL ********************

# Identifiant unique transmis par Fabric Data Factory (@pipeline().RunId)
run_id = "manual"

# Date de référence ISO AAAA-MM-JJ (laisser vide pour la date du jour UTC)
run_date = ""

# Nom de l'étape d'orchestration
stage = ""

# Artefacts Fabric cibles
warehouse = "wh_health_analytics"
lakehouse = "lh_health_analytics"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3 · Génération T-SQL, connexion et synchronisation du Warehouse

# CELL ********************

from __future__ import annotations

import time
from typing import Callable, Iterable

from pyspark.sql import types as T

wh_logger = get_logger("warehouse")

WAREHOUSE_VERIFICATION_QUERIES = {
    "orphans_fact_shortage": "SELECT COUNT(*) AS n FROM dbo.gold_fact_shortage f LEFT JOIN dbo.gold_dim_drug d ON d.drug_sk = f.drug_sk WHERE d.drug_sk IS NULL",
    "orphans_fact_composition": "SELECT COUNT(*) AS n FROM dbo.gold_fact_composition f LEFT JOIN dbo.gold_dim_drug d ON d.drug_sk = f.drug_sk WHERE d.drug_sk IS NULL",
    "orphans_fact_has_opinion": "SELECT COUNT(*) AS n FROM dbo.gold_fact_has_opinion f LEFT JOIN dbo.gold_dim_drug d ON d.drug_sk = f.drug_sk WHERE d.drug_sk IS NULL",
    "orphans_fact_price": "SELECT COUNT(*) AS n FROM dbo.gold_fact_presentation_price f LEFT JOIN dbo.gold_dim_presentation d ON d.presentation_sk = f.presentation_sk WHERE d.presentation_sk IS NULL",
    "duplicate_dim_drug": "SELECT COUNT(*) AS n FROM (SELECT drug_sk FROM dbo.gold_dim_drug GROUP BY drug_sk HAVING COUNT(*) > 1) x",
    "null_risk_score": "SELECT COUNT(*) AS n FROM dbo.gold_kpi_shortage_risk_score WHERE risk_score IS NULL",
}

GOLD_PRIMARY_KEYS = {
    "gold_dim_atc": "atc_sk",
    "gold_dim_drug": "drug_sk",
    "gold_dim_generic_group": "group_sk",
    "gold_dim_opinion_level": "level_sk",
    "gold_dim_presentation": "presentation_sk",
    "gold_dim_status": "status_sk",
    "gold_dim_substance": "substance_sk",
    "gold_dim_time": "time_sk",
}


def quote_tsql_identifier(name: str) -> str:
    """Valide et entoure un identifiant T-SQL de crochets (protection anti-injection)."""
    validate_sql_identifier(name, allow_qualified=False)
    return f"[{name}]"


def spark_to_tsql_type(spark_type: T.DataType) -> str:
    """Convertit un type Spark en type T-SQL supporté par Microsoft Fabric Warehouse."""
    if isinstance(spark_type, T.StringType):
        return "varchar(8000)"
    if isinstance(spark_type, T.BooleanType):
        return "bit"
    if isinstance(spark_type, (T.ByteType, T.ShortType)):
        return "smallint"
    if isinstance(spark_type, T.IntegerType):
        return "int"
    if isinstance(spark_type, T.LongType):
        return "bigint"
    if isinstance(spark_type, T.FloatType):
        return "real"
    if isinstance(spark_type, T.DoubleType):
        return "float"
    if isinstance(spark_type, T.DecimalType):
        return f"decimal({min(spark_type.precision, 38)},{spark_type.scale})"
    if isinstance(spark_type, T.DateType):
        return "date"
    if isinstance(spark_type, (T.TimestampType, T.TimestampNTZType)):
        return "datetime2(6)"
    if isinstance(spark_type, T.ArrayType):
        return "varchar(8000)"
    raise TypeError(f"Type Spark non pris en charge pour le Warehouse : {spark_type}")


def build_tsql_select_expr(field: T.StructField) -> str:
    quoted_col = quote_tsql_identifier(field.name)
    if isinstance(field.dataType, T.ArrayType):
        return f"CAST({quoted_col} AS varchar(8000)) AS {quoted_col}"
    if isinstance(field.dataType, T.TimestampType):
        return f"CAST({quoted_col} AS datetime2(6)) AS {quoted_col}"
    return quoted_col


def flatten_array_columns(schema: T.StructType) -> T.StructType:
    """Exclut les colonnes ArrayType non exposées par le point de terminaison SQL du Lakehouse."""
    return T.StructType([field for field in schema.fields if not isinstance(field.dataType, T.ArrayType)])


def build_create_table_sql(schema: T.StructType, table_name: str, wh_schema: str = "dbo") -> str:
    flat_schema = flatten_array_columns(schema)
    if not flat_schema.fields:
        raise ValueError(f"Schéma vide pour la table {table_name}")
    validate_sql_identifier(table_name)
    validate_sql_identifier(wh_schema)
    pk_col = GOLD_PRIMARY_KEYS.get(table_name)
    col_defs = ",\n    ".join(
        f"{quote_tsql_identifier(field.name)} {spark_to_tsql_type(field.dataType)} {'NOT NULL' if field.name == pk_col else 'NULL'}"
        for field in flat_schema.fields
    )
    return (
        f"IF OBJECT_ID(N'{wh_schema}.{table_name}', N'U') IS NULL\n"
        f"CREATE TABLE {quote_tsql_identifier(wh_schema)}.{quote_tsql_identifier(table_name)} (\n    {col_defs}\n);"
    )


def build_load_table_sql(
    schema: T.StructType,
    table_name: str,
    lakehouse_name: str,
    wh_schema: str = "dbo",
    lh_schema: str = "dbo",
    use_delete: bool | None = None,
) -> str:
    """Génère un bloc transactionnel atomique (DELETE pour dimensions PK, TRUNCATE sinon) + INSERT ... SELECT."""
    validate_sql_identifier(lakehouse_name)
    validate_sql_identifier(wh_schema)
    validate_sql_identifier(lh_schema)
    flat_schema = flatten_array_columns(schema)
    insert_cols = ", ".join(quote_tsql_identifier(field.name) for field in flat_schema.fields)
    select_cols = ",\n        ".join(build_tsql_select_expr(field) for field in flat_schema.fields)
    # Polaris lève l'erreur 4712 sur TRUNCATE TABLE pour les dimensions ciblées par une FK même NOT ENFORCED
    should_use_delete = (table_name in GOLD_PRIMARY_KEYS) if use_delete is None else use_delete
    clear_verb = "DELETE FROM" if should_use_delete else "TRUNCATE TABLE"
    return (
        f"BEGIN TRANSACTION;\n"
        f"    {clear_verb} {quote_tsql_identifier(wh_schema)}.{quote_tsql_identifier(table_name)};\n"
        f"    INSERT INTO {quote_tsql_identifier(wh_schema)}.{quote_tsql_identifier(table_name)} ({insert_cols})\n"
        f"    SELECT\n        {select_cols}\n"
        f"    FROM {quote_tsql_identifier(lakehouse_name)}.{quote_tsql_identifier(lh_schema)}.{quote_tsql_identifier(table_name)};\n"
        f"COMMIT TRANSACTION;"
    )


def build_merge_price_sql(
    schema: T.StructType,
    lakehouse_name: str,
    wh_schema: str = "dbo",
    keys: tuple[str, ...] = ("cip13", "snapshot_date"),
) -> str:
    """Génère un MERGE quotidien incrémental pour gold_fact_presentation_price."""
    validate_sql_identifier(lakehouse_name)
    validate_sql_identifier(wh_schema)
    field_names = [field.name for field in flatten_array_columns(schema).fields]
    if any(key_col not in field_names for key_col in keys):
        raise ValueError("Clé métier absente du schéma pour le MERGE")

    target_table = f"{quote_tsql_identifier(wh_schema)}.[gold_fact_presentation_price]"
    on_clause = " AND ".join(f"t.{quote_tsql_identifier(key_col)} = s.{quote_tsql_identifier(key_col)}" for key_col in keys)
    update_clause = ", ".join(
        f"t.{quote_tsql_identifier(col)} = s.{quote_tsql_identifier(col)}"
        for col in field_names
        if col not in keys
    )
    cols_list = ", ".join(quote_tsql_identifier(col) for col in field_names)
    vals_list = ", ".join(f"s.{quote_tsql_identifier(col)}" for col in field_names)
    return (
        f"MERGE {target_table} AS t\n"
        f"USING (SELECT {cols_list} FROM {quote_tsql_identifier(lakehouse_name)}.[dbo].[gold_fact_presentation_price]) AS s\n"
        f"  ON {on_clause}\n"
        f"WHEN MATCHED THEN UPDATE SET {update_clause}\n"
        f"WHEN NOT MATCHED BY TARGET THEN INSERT ({cols_list}) VALUES ({vals_list});"
    )


def ordered_warehouse_tables() -> list[str]:
    """Ordonne les tables Gold : Dimensions, puis Faits, puis Indicateurs KPI."""
    return sorted(
        GOLD_TABLES.values(),
        key=lambda table_name: (0 if "_dim_" in table_name else 1 if "_fact_" in table_name else 2, table_name),
    )


def build_warehouse_scripts(
    schemas: dict[str, T.StructType],
    lakehouse_name: str,
    wh_schema: str = "dbo",
    tables: Iterable[str] | None = None,
) -> dict:
    """Construit l'ensemble des requêtes DDL, LOAD (discrètes par table) et VERIFICATION."""
    target_tables = list(tables or ordered_warehouse_tables())
    missing_schemas = [name for name in target_tables if name not in schemas]
    if missing_schemas:
        raise KeyError(f"Schémas Gold manquants : {missing_schemas}")

    ddl_statements = [build_create_table_sql(schemas[name], name, wh_schema) for name in target_tables]
    return {
        "ddl_statements": ddl_statements,
        "ddl": "\n\n".join(ddl_statements),
        "load": {name: build_load_table_sql(schemas[name], name, lakehouse_name, wh_schema) for name in target_tables},
        "verification": WAREHOUSE_VERIFICATION_QUERIES,
    }


def execute_warehouse_sync(
    spark_session,
    table_io_client: TableIO,
    lakehouse_name: str,
    execute_fn: Callable[[str], object],
    scalar_query_fn: Callable[[str], int],
    current_run_id: str = "manual",
    wh_schema: str = "dbo",
) -> dict:
    """Synchronise les 17 tables Gold dans le Warehouse et valide l'intégrité référentielle."""
    del spark_session
    start_time = time.time()
    validate_sql_identifier(lakehouse_name)
    validate_sql_identifier(wh_schema)

    ordered_tables = ordered_warehouse_tables()
    schemas_by_table = {table_name: table_io_client.read(table_name).schema for table_name in ordered_tables}
    scripts = build_warehouse_scripts(schemas_by_table, lakehouse_name, wh_schema)

    for ddl_stmt in scripts["ddl_statements"]:
        execute_fn(ddl_stmt)

    step_results: list[StepResult] = []
    for table_name in ordered_tables:
        try:
            execute_fn(scripts["load"][table_name])
            loaded_rows = scalar_query_fn(f"SELECT COUNT(*) FROM {quote_tsql_identifier(wh_schema)}.{quote_tsql_identifier(table_name)}")
            step_results.append(StepResult(table_name, table_name, "SUCCESS", None, loaded_rows))
        except Exception as exc:  # noqa: BLE001
            wh_logger.error("Échec chargement Warehouse pour %s : %s", table_name, exc)
            step_results.append(StepResult(table_name, table_name, "FAILED", f"{type(exc).__name__}: {exc}"))

    verification_counts = {
        check_name: scalar_query_fn(check_sql)
        for check_name, check_sql in scripts["verification"].items()
    }
    failed_checks = {check_name: val for check_name, val in verification_counts.items() if val != 0}

    critical_tables = {name for name in ordered_tables if "_dim_drug" in name or "_fact_" in name}
    summary_payload = summarize_results(
        step_results,
        critical_tables,
        current_run_id,
        time.time() - start_time,
        extra={"verification": verification_counts, "results": [res.to_dict() for res in step_results]},
    )

    if failed_checks:
        raise RuntimeError(f"Vérifications référentielles du Warehouse en échec : {failed_checks}")

    return summary_payload


class WarehouseSqlConnection:
    """Connexion T-SQL compatible DataFrame via pyodbc et jeton Entra ID (PySpark Fabric)."""

    def __init__(self, conn) -> None:
        self._conn = conn

    def query(self, sql_statement: str):
        import pandas as pd

        cursor = self._conn.cursor()
        try:
            cursor.execute(sql_statement)
            columns = None
            rows = None
            if cursor.description is not None:
                columns = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
            while cursor.nextset():
                if cursor.description is not None and columns is None:
                    columns = [col[0] for col in cursor.description]
                    rows = cursor.fetchall()
            if columns is not None and rows is not None:
                return pd.DataFrame.from_records(rows, columns=columns)
            return pd.DataFrame()
        finally:
            cursor.close()

    def close(self) -> None:
        self._conn.close()


def connect_warehouse(nb_utils_runtime, warehouse_name: str):
    """Ouvre une connexion T-SQL vers le Warehouse Fabric (Python natif ou PySpark ODBC + Entra ID)."""
    data_api = getattr(nb_utils_runtime, "data", None)
    if data_api is not None and hasattr(data_api, "connect_to_artifact"):
        return data_api.connect_to_artifact(warehouse_name)

    import pyodbc
    import sempy.fabric as sempy_fabric
    import struct

    workspace_id = nb_utils_runtime.runtime.context["currentWorkspaceId"]
    wh_items = sempy_fabric.FabricRestClient().get(f"/v1/workspaces/{workspace_id}/warehouses").json().get("value", [])
    wh_match = next((item for item in wh_items if item.get("displayName") == warehouse_name), None)
    if wh_match is None:
        raise RuntimeError(f"Warehouse '{warehouse_name}' introuvable dans le workspace {workspace_id}")
    props = wh_match.get("properties", {})
    server_fqdn = props.get("connectionString") or props.get("connectionInfo")
    if not server_fqdn:
        raise RuntimeError(f"Chaîne de connexion T-SQL manquante pour '{warehouse_name}'")

    token_bytes = nb_utils_runtime.credentials.getToken("https://database.windows.net/").encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server={server_fqdn},1433;"
        f"Database={warehouse_name};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
    )
    return WarehouseSqlConnection(
        pyodbc.connect(conn_str, attrs_before={1256: token_struct}, autocommit=True)
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4 · Exécution
# 
# Résolution du contexte (format de table, landing, date de référence), bannière de traçabilité puis appel de la fonction d'orchestration.

# CELL ********************

table_io = create_table_io(spark)
reference_date = parse_run_date(run_date)
init_notebook_banner("nb_warehouse_sync_gold", run_id, reference_date, table_io.fmt)

validate_sql_identifier(warehouse)
validate_sql_identifier(lakehouse)

nb_utils_runtime = globals().get("notebookutils")
if nb_utils_runtime is None:
    import notebookutils as nb_utils_runtime  # type: ignore
wh_connection = connect_warehouse(nb_utils_runtime, warehouse)


def run_tsql_statement(sql_statement: str):
    return wh_connection.query(sql_statement)


def run_tsql_scalar(sql_query: str) -> int:
    return int(wh_connection.query(sql_query).iloc[0, 0])


try:
    payload = execute_warehouse_sync(
        spark_session=spark,
        table_io_client=table_io,
        lakehouse_name=lakehouse,
        execute_fn=run_tsql_statement,
        scalar_query_fn=run_tsql_scalar,
        current_run_id=run_id,
    )
finally:
    if hasattr(wh_connection, "close"):
        wh_connection.close()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5 · Rapport d'exécution et sortie JSON
# 
# Affiche la synthèse puis renvoie le résumé JSON au pipeline via `notebookutils.notebook.exit` (lisible dans la sortie de l'activité).

# CELL ********************

wh_logger = get_logger("warehouse")
wh_logger.info("🏛️ Statut Entrepôt : %s | Durée : %s s", payload.get("status"), payload.get("duration_seconds", 0))
if "verification" in payload:
    wh_logger.info("🔍 Contrôles d'intégrité post-chargement (attendu = 0) :")
    for check_name, check_value in payload["verification"].items():
        if check_value == 0:
            wh_logger.info("   ✅ %s : %s", check_name, check_value)
        else:
            wh_logger.error("   ❌ %s : %s", check_name, check_value)
exit_notebook_json(payload)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# 
# ## 🛠️ Exploitation et dépannage
# 
# - **Rejouer une date** : renseigner `run_date` (AAAA-MM-JJ) ; toutes les étapes sont idempotentes.
# - **Diagnostiquer** : la sortie JSON de l'activité contient `status`, les étapes en échec et leurs messages ; les contrôles qualité sont historisés dans `_dq_audit_log`.
# - **Verrou** : en cas d'interruption brutale, `_ops_run_lock` expire automatiquement après `ttl_minutes` ; le pipeline libère toujours le verrou en fin de run.
# 
# *HealthTek — nb_warehouse_sync_gold · documentation générée selon le standard du projet.*
