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

# # 🥉 [BRONZE - LOAD] `nb_bronze_load_bdpm` — Chargement Brut des Tables Delta
# 
# > **HealthTek** · Plateforme analytique des ruptures et tensions d'approvisionnement de médicaments (données publiques BDPM / ANSM / HAS) — Microsoft Fabric
# 
# | Élément | Détail |
# |---|---|
# | **Couche** | Bronze — chargement |
# | **Type** | Notebook exécutable |
# | **Orchestration** | Activité `run_bronze` — pipeline `pl_master_health_pipeline` |
# | **Entrées** | Dernier dépôt landing ≤ `run_date` par source |
# | **Sorties** | `bronze_bdpm_*` (11 tables Delta, texte brut + 5 colonnes d'audit) |
# | **Idempotence** | Oui (overwrite du dernier instantané) |
# | **Documentation** | v3.1 — 10/11/2026 |
# 
# ## 🎯 Objectif
# 
# Lit le dernier fichier déposé dans `Files/landing/bdpm/*` (<= `run_date`) pour chaque source sélectionnée, applique le schéma positionnel en texte brut (`inferSchema=false`) et ajoute les 5 colonnes techniques d'audit (`_ingested_at`, `_source_file`, `_source_system`, `_pipeline_run_id`, `_snapshot_date`).
# 
# ## 🔄 Flux de données
# 
# ```
# Files/landing/bdpm/*.txt ──► Lecture positionnelle TSV + Audit ──► Tables Delta Bronze (bronze_bdpm_*)
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
# | `source_filter` | `"all"` | Filtre des sources ; `auto` recharge toutes les sources (rechargement idempotent). |
# 
# ## 🧭 Plan du notebook
# 
# 1. Chargement des utilitaires
# 2. Paramètres
# 3. Lecture positionnelle et colonnes d'audit
# 4. Exécution
# 5. Rapport et sortie JSON
# 
# ## 📤 Contrat de sortie
# 
# `{"status","total_rows","results":[...]}`

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

# Filtre des sources Bronze à charger : 'all', 'auto', 'daily', 'biweekly', 'monthly'
source_filter = "all"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3 · Lecture positionnelle et colonnes d'audit Bronze

# CELL ********************

from __future__ import annotations

import os
import time
from datetime import date
from typing import Optional

from pyspark.sql import DataFrame, SparkSession, functions as F

bronze_logger = get_logger("bronze")


class SchemaContractError(Exception):
    """Exception levée si un fichier source contient moins de colonnes que son contrat."""


def append_bronze_audit_columns(
    df: DataFrame,
    source_system: str,
    current_run_id: str,
    source_file_path: str,
    snapshot_date: Optional[date],
) -> DataFrame:
    """Ajoute les 5 colonnes standardisées de traçabilité Bronze."""
    snapshot_col = F.lit(snapshot_date.isoformat()).cast("date") if snapshot_date else F.lit(None).cast("date")
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(os.path.basename(source_file_path)))
        .withColumn("_source_system", F.lit(source_system))
        .withColumn("_pipeline_run_id", F.lit(current_run_id))
        .withColumn("_snapshot_date", snapshot_col)
    )


def read_positional_tsv(
    spark_session: SparkSession,
    file_path: str,
    column_names: list[str],
    delimiter: str = "\t",
) -> tuple[DataFrame, dict]:
    """Lit un fichier sans en-tête tout en chaîne et valide le nombre minimum de colonnes."""
    df_raw = (
        spark_session.read.option("sep", delimiter)
        .option("header", "false")
        .option("quote", "")
        .option("inferSchema", "false")
        .option("encoding", "UTF-8")
        .option("mode", "PERMISSIVE")
        .csv(to_spark_file_path(file_path))
    )
    found_columns = len(df_raw.columns)
    contract_info = {"columns_found": found_columns, "columns_expected": len(column_names)}
    if found_columns < len(column_names):
        raise SchemaContractError(
            f"{os.path.basename(file_path)} : {found_columns} colonnes lues, {len(column_names)} attendues"
        )
    projected_df = df_raw.select(*[F.col(f"_c{idx}").alias(col_name) for idx, col_name in enumerate(column_names)])
    return projected_df, contract_info


def load_bronze_source(
    spark_session: SparkSession,
    table_io_client: TableIO,
    source_key: str,
    landing_root_path: str,
    current_run_id: str,
    target_date: Optional[date] = None,
) -> StepResult:
    """Charge de façon idempotente (overwrite) le dernier snapshot landing d'une source BDPM."""
    source_spec = BDPM_SOURCES[source_key]
    try:
        latest_path = latest_landing_file(landing_root_path, source_spec.subfolder, target_date)
        if latest_path is None and source_spec.optional:
            return StepResult(
                source=source_key,
                table=source_spec.table,
                status="SKIPPED",
                error=f"Source facultative absente dans {landing_root_path}/{source_spec.subfolder}",
            )
        if latest_path is None:
            return StepResult(
                source=source_key,
                table=source_spec.table,
                status="FAILED",
                error=f"Aucun fichier dans {landing_root_path}/{source_spec.subfolder} (exécuter nb_bronze_ingest_bdpm)",
            )

        snapshot_date = extract_snapshot_date(latest_path)
        df_source, contract_info = read_positional_tsv(spark_session, latest_path, list(source_spec.columns))
        df_audited = append_bronze_audit_columns(df_source, "BDPM", current_run_id, latest_path, snapshot_date)
        table_io_client.overwrite(df_audited, source_spec.table)
        df_written = table_io_client.read(source_spec.table)

        return StepResult(
            source=source_key,
            table=source_spec.table,
            status="SUCCESS",
            error=None,
            rows=df_written.count(),
            columns=len(df_written.columns),
            details={"file": os.path.basename(latest_path), **contract_info},
        )
    except Exception as exc:  # noqa: BLE001 - isoler chaque source pour le rapport summarize_results
        bronze_logger.exception("Erreur Bronze pour la source %s", source_key)
        return StepResult(source_key, source_spec.table, "FAILED", f"{type(exc).__name__}: {exc}")


def execute_bronze_pipeline(
    spark_session: SparkSession,
    table_io_client: TableIO,
    landing_root_path: str,
    current_run_id: str,
    target_date: Optional[date] = None,
    filter_mode: str = "all",
) -> dict:
    """Charge les sources sélectionnées en couche Bronze."""
    start_time = time.time()
    selected_keys = sources_for_filter(filter_mode)
    results = [
        load_bronze_source(spark_session, table_io_client, key, landing_root_path, current_run_id, target_date)
        for key in selected_keys
    ]
    return summarize_results(
        results,
        CRITICAL_SOURCES,
        current_run_id,
        time.time() - start_time,
        extra={"results": [res.to_dict() for res in results]},
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
landing_root = resolve_landing_root()
reference_date = parse_run_date(run_date)
init_notebook_banner("nb_bronze_load_bdpm", run_id, reference_date, table_io.fmt)

payload = execute_bronze_pipeline(
    spark_session=spark,
    table_io_client=table_io,
    landing_root_path=landing_root,
    current_run_id=run_id,
    target_date=reference_date,
    filter_mode=source_filter,
)

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

render_execution_report(
    payload,
    columns=["source", "table", "status", "rows", "columns"],
    summary_line=(
        f"🎯 Statut Bronze : {payload.get('status')} | "
        f"{payload.get('total_rows', 0):,} lignes chargées | "
        f"Durée : {payload.get('duration_seconds', 0)} s"
    ),
)
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
# *HealthTek — nb_bronze_load_bdpm · documentation générée selon le standard du projet.*
