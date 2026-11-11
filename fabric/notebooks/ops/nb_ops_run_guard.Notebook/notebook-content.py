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

# # 🔒 [OPS] `nb_ops_run_guard` — Verrou Coopératif Anti-Concurrence
# 
# > **HealthTek** · Plateforme analytique des ruptures et tensions d'approvisionnement de médicaments (données publiques BDPM / ANSM / HAS) — Microsoft Fabric
# 
# | Élément | Détail |
# |---|---|
# | **Couche** | Ops — concurrence |
# | **Type** | Notebook exécutable |
# | **Orchestration** | Activités `run_lock` (acquire) et `run_unlock` (release, exécutée même en cas d'échec) — pipeline `pl_master_health_pipeline` |
# | **Entrées** | `_ops_run_lock` |
# | **Sorties** | `_ops_run_lock` |
# | **Idempotence** | Oui (release ne supprime que le verrou du run courant) |
# | **Documentation** | v3.1 — 10/11/2026 |
# 
# ## 🎯 Objectif
# 
# Gère le verrou d'exécution coopératif dans la table Delta `_ops_run_lock` pour empêcher les écritures concurrentes entre déclenchements manuels et planifiés :
# - `action = "acquire"` : acquiert le verrou (ou reprend un verrou expiré après crash).
# - `action = "release"` : libère uniquement le verrou appartenant au `run_id` courant.
# 
# ## 🔄 Flux de données
# 
# ```
# Pipeline Run ID + Action ('acquire' / 'release') ──► Contrôle TTL ──► Table Delta _ops_run_lock
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
# | `action` | `"acquire"` | `acquire` en début de pipeline, `release` en fin. |
# | `ttl_minutes` | `180` | Durée de vie du verrou ; un verrou expiré (crash) est repris automatiquement. |
# 
# ## 🧭 Plan du notebook
# 
# 1. Chargement des utilitaires
# 2. Paramètres
# 3. Logique du verrou (acquisition / libération)
# 4. Exécution de l'action
# 5. Sortie JSON
# 
# ## 📤 Contrat de sortie
# 
# `{"status","action","lock_expires_at"|"released","pipeline_run_id"}`

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

# Action attendue : 'acquire' en début de pipeline, 'release' en fin de pipeline
action = "acquire"

# Durée de validité maximale du verrou en minutes (expiration automatique anti-blocage)
ttl_minutes = 180

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3 · Verrou coopératif : acquisition et libération

# CELL ********************

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

LOCK_TABLE_SCHEMA = "run_id string, acquired_at timestamp, expires_at timestamp"


@dataclass
class LockInfo:
    """Métadonnées d'un verrou d'exécution coopératif."""

    run_id: str
    acquired_at: datetime
    expires_at: datetime


class RunLockError(RuntimeError):
    """Erreur levée si un autre pipeline actif détient déjà le verrou."""


def acquire_run_lock(
    spark_session,
    table_io_client: TableIO,
    current_run_id: str,
    lock_ttl_minutes: int = 180,
    now_utc: Optional[datetime] = None,
) -> LockInfo:
    """Acquiert ou renouvelle le verrou coopératif ; refuse si un autre run actif le détient."""
    ttl_val = int(lock_ttl_minutes)
    if ttl_val <= 0:
        raise ValueError(f"ttl_minutes doit être strictement positif, reçu {lock_ttl_minutes!r}")

    current_time = now_utc or utc_now().replace(tzinfo=None)
    if table_io_client.exists(LOCK_TABLE):
        active_locks = table_io_client.read(LOCK_TABLE).collect()
        for lock_row in active_locks:
            if lock_row.run_id != current_run_id and lock_row.expires_at > current_time:
                raise RunLockError(
                    f"Un autre run ({lock_row.run_id}) détient déjà le verrou jusqu'à {lock_row.expires_at.isoformat()}"
                )

    lock_info = LockInfo(current_run_id, current_time, current_time + timedelta(minutes=ttl_val))
    df_lock = spark_session.createDataFrame(
        [(lock_info.run_id, lock_info.acquired_at, lock_info.expires_at)],
        LOCK_TABLE_SCHEMA,
    )
    table_io_client.overwrite(df_lock, LOCK_TABLE)
    return lock_info


def release_run_lock(
    spark_session,
    table_io_client: TableIO,
    current_run_id: str,
) -> bool:
    """Libère exclusivement le verrou appartenant à current_run_id."""
    if not table_io_client.exists(LOCK_TABLE):
        return False
    existing_locks = table_io_client.read(LOCK_TABLE).collect()
    if not any(lock_row.run_id == current_run_id for lock_row in existing_locks):
        return False

    remaining_locks = [
        (lock_row.run_id, lock_row.acquired_at, lock_row.expires_at)
        for lock_row in existing_locks
        if lock_row.run_id != current_run_id
    ]
    table_io_client.overwrite(spark_session.createDataFrame(remaining_locks, LOCK_TABLE_SCHEMA), LOCK_TABLE)
    return True

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
init_notebook_banner("nb_ops_run_guard", run_id, reference_date, table_io.fmt)

normalized_action = str(action).strip().lower()
if normalized_action not in ("acquire", "release"):
    raise ValueError(f"action invalide {action!r} ; valeurs permises : ('acquire', 'release')")

if normalized_action == "acquire":
    lock_info = acquire_run_lock(spark, table_io, run_id, int(ttl_minutes))
    payload = {
        "status": "SUCCESS",
        "action": "acquire",
        "lock_expires_at": lock_info.expires_at.isoformat(),
        "pipeline_run_id": run_id,
    }
    get_logger("run_guard").info("🔒 Verrou acquis pour run %s. Expire à : %s", run_id, payload["lock_expires_at"])
else:
    was_released = release_run_lock(spark, table_io, run_id)
    payload = {
        "status": "SUCCESS",
        "action": "release",
        "released": was_released,
        "pipeline_run_id": run_id,
    }
    get_logger("run_guard").info("🔓 Verrou libéré pour run %s : %s", run_id, was_released)

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
# *HealthTek — nb_ops_run_guard · documentation générée selon le standard du projet.*
