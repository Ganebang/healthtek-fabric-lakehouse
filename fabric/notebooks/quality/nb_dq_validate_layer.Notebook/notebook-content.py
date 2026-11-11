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

# # 🛡️ [QUALITY] `nb_dq_validate_layer` — Porte Qualité Multi-Couches
# 
# > **HealthTek** · Plateforme analytique des ruptures et tensions d'approvisionnement de médicaments (données publiques BDPM / ANSM / HAS) — Microsoft Fabric
# 
# | Élément | Détail |
# |---|---|
# | **Couche** | Qualité |
# | **Type** | Notebook exécutable |
# | **Orchestration** | Activités `run_dq_bronze`, `run_dq_silver`, `run_dq_gold` — pipeline `pl_master_health_pipeline` |
# | **Entrées** | Tables de la couche ciblée |
# | **Sorties** | `_dq_audit_log`, décision de la porte qualité |
# | **Idempotence** | Oui (journal append-only avec `attempt_id`) |
# | **Documentation** | v3.1 — 10/11/2026 |
# 
# ## 🎯 Objectif
# 
# Évalue en une passe Spark vectorisée l'ensemble des règles de la couche cible (`bronze`, `silver`, ou `gold`), journalise les résultats dans `_dq_audit_log` et bloque l'exécution du pipeline si un contrôle critique échoue lorsque `fail_fast = "true"`.
# 
# ## 🔄 Flux de données
# 
# ```
# Tables Delta (bronze / silver / gold) ──► Évaluation vectorisée ──► _dq_audit_log + Décision Gate
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
# | `layer` | `"silver"` | Couche évaluée : `bronze`, `silver` ou `gold`. |
# | `fail_fast` | `"true"` | Bloque le pipeline si une règle critique échoue. |
# 
# ## 🧭 Plan du notebook
# 
# 1. Chargement du moteur qualité
# 2. Paramètres
# 3. Évaluation de la porte qualité
# 4. Synthèse et sortie JSON
# 
# ## 📤 Contrat de sortie
# 
# `{"layer","gate_passed","total_checks","passed","warnings","critical_failures","failed_rules"}`

# MARKDOWN ********************

# ## 1 · Chargement des utilitaires partagés
# 
# Les notebooks `nb_util_validation` sont exécutés dans la même session Spark (`%run`) : configuration centrale, journalisation, accès Lakehouse et moteur qualité. Chaque cellule `%run` doit rester seule dans sa cellule.

# CELL ********************

%run nb_util_validation

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

# Couche cible à évaluer : 'bronze', 'silver' ou 'gold'
layer = "silver"

# Arrêt du pipeline en cas d'échec critique ('true' / 'false')
fail_fast = "true"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3 · Exécution
# 
# Résolution du contexte (format de table, landing, date de référence), bannière de traçabilité puis appel de la fonction d'orchestration.

# CELL ********************

table_io = create_table_io(spark)
reference_date = parse_run_date(run_date)
init_notebook_banner("nb_dq_validate_layer", run_id, reference_date, table_io.fmt)

payload = run_quality_gate(
    spark_session=spark,
    table_io=table_io,
    layer=layer,
    fail_fast=fail_fast,
    run_id=run_id,
    as_of_date=reference_date,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4 · Rapport d'exécution et sortie JSON
# 
# Affiche la synthèse puis renvoie le résumé JSON au pipeline via `notebookutils.notebook.exit` (lisible dans la sortie de l'activité).

# CELL ********************

dq_logger = get_logger("dq_gate")
gate_icon = "✅ ACCEPTÉE" if payload.get("gate_passed") else "❌ BLOQUÉE"
dq_logger.info("🛡️ Porte Qualité Couche [%s] : %s", payload.get("layer", "").upper(), gate_icon)
dq_logger.info(
    "Total contrôles : %s | Réussis : %s | Avertissements : %s | Échecs critiques : %s",
    payload.get("total_checks"), payload.get("passed"), payload.get("warnings"), payload.get("critical_failures"),
)
if payload.get("failed_rules"):
    dq_logger.error("❌ Règles critiques en échec : %s", payload.get("failed_rules"))
if payload.get("warning_rules"):
    dq_logger.warning("⚠️ Avertissements tolérés : %s", payload.get("warning_rules"))

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
# *HealthTek — nb_dq_validate_layer · documentation générée selon le standard du projet.*
