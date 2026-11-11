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

# # 🔄 [OPS] `nb_ops_refresh_semantic_model` — Reframe Direct Lake du Modèle Sémantique
# 
# > **HealthTek** · Plateforme analytique des ruptures et tensions d'approvisionnement de médicaments (données publiques BDPM / ANSM / HAS) — Microsoft Fabric
# 
# | Élément | Détail |
# |---|---|
# | **Couche** | Ops — diffusion |
# | **Type** | Notebook exécutable |
# | **Orchestration** | Activité `run_refresh_model` — pipeline `pl_master_health_pipeline` (après la porte qualité Gold) |
# | **Entrées** | Tables Gold validées (Delta) |
# | **Sorties** | `sem_health_analytics` recadré sur la dernière version des tables |
# | **Idempotence** | Oui (un reframe ne copie aucune donnée) |
# | **Documentation** | v3.1 — 10/11/2026 |
# 
# ## 🎯 Objectif
# 
# Déclencher **un seul** rafraîchissement (*reframe*) du modèle Direct Lake `sem_health_analytics`, **après** la validation de la couche Gold, pour que le rapport passe directement d'un état validé à un autre. À combiner avec l'option *Keep your Direct Lake data up to date* = **Off** dans les paramètres du modèle.
# 
# ## ⚙️ Paramètres
# 
# | Paramètre | Défaut | Rôle |
# |---|---|---|
# | `run_id` | `"manual"` | Identifiant du run (`@pipeline().RunId`). |
# | `run_date` | `""` | Date de référence ISO (vide = aujourd'hui UTC). |
# | `stage` | `""` | Nom de l'étape d'orchestration. |
# | `semantic_model` | `"sem_health_analytics"` | Modèle sémantique à recadrer. |
# | `refresh_type` | `"full"` | Type de rafraîchissement (`full` = reframe complet Direct Lake). |
# | `timeout_seconds` | `900` | Délai maximal d'attente du statut final. |
# 
# ## 📤 Contrat de sortie
# 
# `{"status","semantic_model","request_id","duration_seconds","pipeline_run_id"}`

# MARKDOWN ********************

# ## 1 · Chargement des utilitaires partagés
# 
# Un seul `%run` par cellule (règle Fabric).

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
# Cellule taguée **`parameters`** : valeurs injectées par l'activité du pipeline.

# PARAMETERS CELL ********************

# Identifiant unique transmis par Fabric Data Factory (@pipeline().RunId)
run_id = "manual"

# Date de référence ISO AAAA-MM-JJ (laisser vide pour la date du jour UTC)
run_date = ""

# Nom de l'étape d'orchestration
stage = ""

# Modèle sémantique Direct Lake à recadrer
semantic_model = "sem_health_analytics"

# Type de rafraîchissement : 'full' = reframe complet Direct Lake
refresh_type = "full"

# Délai maximal d'attente de la fin du rafraîchissement (secondes)
timeout_seconds = 900

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3 · Rafraîchissement via Semantic Link (`sempy`)
# 
# `sempy` est préinstallé dans Fabric et s'authentifie avec l'identité de l'exécutant : aucun secret ni connexion à créer.

# CELL ********************

import time

import sempy.fabric as fabric

refresh_logger = get_logger("refresh")
FINAL_STATUSES = {"completed", "failed", "cancelled", "disabled"}


def refresh_semantic_model(model_name: str, kind: str, timeout_s: int, poll_s: int = 10) -> dict:
    """Déclenche un reframe du modèle Direct Lake et attend son statut final."""
    validate_sql_identifier(model_name)
    start = time.time()
    request_id = fabric.refresh_dataset(dataset=model_name, refresh_type=kind)
    refresh_logger.info("🔄 Rafraîchissement demandé pour %s (requête %s)", model_name, request_id)
    status = "unknown"
    while time.time() - start < int(timeout_s):
        details = fabric.get_refresh_execution_details(model_name, request_id)
        status = str(details.status).lower()
        if status in FINAL_STATUSES:
            break
        time.sleep(poll_s)
    elapsed = round(time.time() - start, 1)
    if status != "completed":
        raise RuntimeError(f"Rafraîchissement de {model_name} terminé avec le statut {status!r} après {elapsed} s")
    refresh_logger.info("✅ %s recadré en %s s", model_name, elapsed)
    return {
        "status": "SUCCESS",
        "semantic_model": model_name,
        "request_id": str(request_id),
        "duration_seconds": elapsed,
        "pipeline_run_id": run_id,
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4 · Exécution et sortie JSON

# CELL ********************

reference_date = parse_run_date(run_date)
init_notebook_banner("nb_ops_refresh_semantic_model", run_id, reference_date, resolve_table_format())

payload = refresh_semantic_model(semantic_model, refresh_type, int(timeout_seconds))
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
# - **`Unauthorized`** : l'identité qui exécute le pipeline doit être au moins `Contributor` du Workspace.
# - **Le rapport ne change pas** : vérifiez dans le `Monitoring hub` que le rafraîchissement de `sem_health_analytics` est `Completed`.
# - **Rafraîchir à la main** : `...` sur `sem_health_analytics` → `Refresh now`.
# 
# *HealthTek — nb_ops_refresh_semantic_model · documentation générée selon le standard du projet.*
