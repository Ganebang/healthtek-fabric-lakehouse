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

# # 🧹 [OPS] `nb_ops_maintenance_delta` — Maintenance Delta Lake (OPTIMIZE & VACUUM)
# 
# > **HealthTek** · Plateforme analytique des ruptures et tensions d'approvisionnement de médicaments (données publiques BDPM / ANSM / HAS) — Microsoft Fabric
# 
# | Élément | Détail |
# |---|---|
# | **Couche** | Ops — performance |
# | **Type** | Notebook exécutable |
# | **Orchestration** | Activité `run_maintenance` (le dimanche ou si `p_run_maintenance = true`) — pipeline `pl_master_health_pipeline` |
# | **Entrées** | Tables Gold |
# | **Sorties** | Fichiers Delta compactés (OPTIMIZE / Z-ORDER) et purgés (VACUUM) |
# | **Idempotence** | Oui |
# | **Documentation** | v3.1 — 10/11/2026 |
# 
# ## 🎯 Objectif
# 
# Exécute la maintenance physique des tables de la couche Gold :
# - `OPTIMIZE` (+ `ZORDER BY` ciblé sur les faits volumineux) pour optimiser les lectures Direct Lake.
# - `VACUUM RETAIN <N> HOURS` via `table_io.vacuum()` (sans interpolation SQL brute dans le notebook).
# 
# ## 🔄 Flux de données
# 
# ```
# Tables Gold (GOLD_TABLES) ──► OPTIMIZE Z-Order + VACUUM sécurisé ──► Fichiers Parquet compactés & purgés
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
# | `retain_hours` | `168` | Rétention VACUUM en heures (≥ 168 h recommandé pour le time travel). |
# 
# ## 🧭 Plan du notebook
# 
# 1. Chargement des utilitaires
# 2. Paramètres
# 3. Fonction de maintenance
# 4. Exécution
# 5. Rapport et sortie JSON
# 
# ## 📤 Contrat de sortie
# 
# `{"status","tables_maintained","retain_hours","results":[...]}`

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

# Rétention VACUUM en heures (168 h = 7 jours recommandés en production)
retain_hours = 168

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3 · Maintenance Delta : OPTIMIZE Z-ORDER et VACUUM

# CELL ********************

import time


def execute_delta_maintenance(
    table_io_client: TableIO,
    retention_hours: int,
    current_run_id: str,
) -> dict:
    """Exécute OPTIMIZE Z-Order et VACUUM sécurisé sur les tables Gold existantes."""
    start_time = time.time()
    retention_hours_val = int(retention_hours)
    if retention_hours_val < 0:
        raise ValueError(f"retain_hours doit être positif ou nul, reçu {retention_hours!r}")

    maintenance_results: list[StepResult] = []
    for table_key, table_name in GOLD_TABLES.items():
        if not table_io_client.exists(table_name):
            maintenance_results.append(StepResult(table_key, table_name, "SKIPPED", "Table absente"))
            continue

        zorder_cols = GOLD_ZORDER_COLUMNS.get(table_name)
        table_io_client.optimize(table_name, zorder_cols)
        table_io_client.vacuum(table_name, retention_hours_val)
        maintenance_results.append(
            StepResult(
                source=table_key,
                table=table_name,
                status="SUCCESS",
                details={"zorder_columns": zorder_cols or [], "retain_hours": retention_hours_val},
            )
        )

    return summarize_results(
        maintenance_results,
        critical_sources=set(),
        run_id=current_run_id,
        elapsed_seconds=time.time() - start_time,
        extra={
            "tables_maintained": sum(res.status == "SUCCESS" for res in maintenance_results),
            "retain_hours": retention_hours_val,
            "results": [res.to_dict() for res in maintenance_results],
        },
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
init_notebook_banner("nb_ops_maintenance_delta", run_id, reference_date, table_io.fmt)

payload = execute_delta_maintenance(table_io, int(retain_hours), run_id)

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
    columns=["source", "table", "status"],
    summary_line=f"🧹 Maintenance terminée : {payload['tables_maintained']} tables maintenues (rétention VACUUM : {payload['retain_hours']} h).",
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
# *HealthTek — nb_ops_maintenance_delta · documentation générée selon le standard du projet.*
