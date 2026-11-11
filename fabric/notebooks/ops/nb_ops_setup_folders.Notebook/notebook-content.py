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

# # 📁 [OPS] `nb_ops_setup_folders` — Initialisation de l'Arborescence Landing OneLake
# 
# > **HealthTek** · Plateforme analytique des ruptures et tensions d'approvisionnement de médicaments (données publiques BDPM / ANSM / HAS) — Microsoft Fabric
# 
# | Élément | Détail |
# |---|---|
# | **Couche** | Ops |
# | **Type** | Notebook exécutable |
# | **Orchestration** | Activité `run_setup_folders` — pipeline `pl_master_health_pipeline` |
# | **Entrées** | `BDPM_SOURCES` |
# | **Sorties** | `Files/landing/bdpm/<source>` (11 dossiers) |
# | **Idempotence** | Oui (`exist_ok=True`) |
# | **Documentation** | v3.1 — 10/11/2026 |
# 
# ## 🎯 Objectif
# 
# Prépare de manière idempotente les 11 sous-dossiers de réception dans `Files/landing/bdpm/*` pour les fichiers sources de la BDPM.
# 
# ## 🔄 Flux de données
# 
# ```
# OneLake Lakehouse (Files/landing) ──► os.makedirs(..., exist_ok=True) ──► 11 sous-dossiers bdpm/<source>
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
# 
# ## 🧭 Plan du notebook
# 
# 1. Chargement des utilitaires
# 2. Paramètres
# 3. Fonction de création des dossiers
# 4. Exécution
# 5. Rapport et sortie JSON
# 
# ## 📤 Contrat de sortie
# 
# `{"status","folders","landing_root","pipeline_run_id"}`

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3 · Création idempotente des dossiers landing

# CELL ********************

import os


def setup_landing_folders(landing_root_path: str, current_run_id: str) -> dict:
    """Crée de façon idempotente les sous-dossiers de réception landing OneLake."""
    folder_paths = [os.path.join(landing_root_path, source_spec.subfolder) for source_spec in BDPM_SOURCES.values()]
    for folder_path in folder_paths:
        os.makedirs(folder_path, exist_ok=True)
    return {
        "status": "SUCCESS",
        "folders": len(folder_paths),
        "landing_root": landing_root_path,
        "pipeline_run_id": current_run_id,
    }

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

landing_root = resolve_landing_root()
reference_date = parse_run_date(run_date)
table_format = resolve_table_format()
init_notebook_banner("nb_ops_setup_folders", run_id, reference_date, table_format)

payload = setup_landing_folders(landing_root, run_id)

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

setup_logger = get_logger("setup")
setup_logger.info("📁 %d sous-dossiers prêts dans %s", payload["folders"], landing_root)
for source_spec in BDPM_SOURCES.values():
    setup_logger.info("   - %s/%s", landing_root, source_spec.subfolder)
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
# *HealthTek — nb_ops_setup_folders · documentation générée selon le standard du projet.*
