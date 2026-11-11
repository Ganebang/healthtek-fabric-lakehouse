# Fabric notebook source

# METADATA ********************

# META {
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "lh_health_analytics",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 💾 [UTILITY] `nb_util_io` — Accès Sécurisé Lakehouse (Delta/Parquet) & Landing OneLake
# 
# > **HealthTek** · Plateforme analytique des ruptures et tensions d'approvisionnement de médicaments (données publiques BDPM / ANSM / HAS) — Microsoft Fabric
# 
# | Élément | Détail |
# |---|---|
# | **Couche** | Socle — entrées / sorties |
# | **Type** | Bibliothèque sans état (`%run`) |
# | **Orchestration** | Inclus par les notebooks de traitement — pipeline `pl_master_health_pipeline` |
# | **Entrées** | Tables Delta, dossier `Files/landing` |
# | **Sorties** | Classe `TableIO` et utilitaires landing |
# | **Idempotence** | Oui (MERGE, replaceWhere) |
# | **Documentation** | v3.1 — 10/11/2026 |
# 
# ## 🎯 Objectif
# 
# Encapsule toutes les opérations d'entrées/sorties sur le Lakehouse et le stockage Landing OneLake :
# - Validation stricte des identifiants SQL (`validate_sql_identifier`) et échappement des littéraux (`format_sql_literal`) contre l'injection SQL.
# - Classe `TableIO` supportant **Delta Lake** (Fabric) et **Parquet** (tests locaux) : `overwrite`, `append`, `replace_where`, `upsert` (MERGE idempotent), `optimize`, `vacuum` et `set_properties`.
# - Résolution de l'environnement (`resolve_landing_root`, `resolve_table_format`, `create_table_io`).
# - Découverte et sélection des fichiers datés dans OneLake (`list_landing_files`, `latest_landing_file`).
# 
# ## 🔗 Utilisation
# 
# Exécuter au préalable `nb_util_config` puis ce notebook :
# ```python
# %run nb_util_config
# ```
# ```python
# %run nb_util_io
# ```
# 
# ## 🧭 Plan du notebook
# 
# 1. Sécurité SQL : validation des identifiants et échappement des littéraux
# 2. `TableIO` : overwrite, append, replace_where, upsert, optimize, vacuum, propriétés
# 3. Résolution des fichiers datés du landing OneLake
# 
# ## 📤 Contrat de sortie
# 
# Aucune sortie : expose `TableIO`, `create_table_io()` et les helpers landing.

# MARKDOWN ********************

# ## 1 · Sécurité SQL : identifiants et littéraux

# CELL ********************

from __future__ import annotations

import hashlib
import os
import re
from datetime import date
from typing import Any, Optional

SQL_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SQL_QUALIFIED_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")
LANDING_DATE_PATTERN = re.compile(r"_(\d{4}-\d{2}-\d{2})(?:\.[A-Za-z0-9]+)?$")


def validate_sql_identifier(identifier: str, allow_qualified: bool = False) -> str:
    """Valide qu'un nom de table, colonne ou schéma ne contient aucun caractère d'injection SQL."""
    pattern = SQL_QUALIFIED_NAME_PATTERN if allow_qualified else SQL_IDENTIFIER_PATTERN
    if not isinstance(identifier, str) or not pattern.match(identifier):
        raise ValueError(f"Identifiant SQL invalide : {identifier!r}")
    return identifier


def escape_sql_string(value: str) -> str:
    """Échappe les apostrophes pour une interpolation sûre dans un littéral SQL Spark."""
    return value.replace("'", "''")


def format_sql_literal(value: Any) -> str:
    """Formate un scalaire Python en littéral Spark SQL sécurisé."""
    if isinstance(value, str):
        return f"'{escape_sql_string(value)}'"
    if isinstance(value, date):
        return f"DATE'{value.isoformat()}'"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    raise TypeError(f"Type de littéral SQL non géré : {type(value).__name__}")

# MARKDOWN ********************

# ## 2 · Couche d'accès TableIO (Delta / Parquet)

# CELL ********************

class TableIO:
    """Couche d'accès unifiée aux tables du Lakehouse (Delta sous Fabric, Parquet en test local)."""

    def __init__(self, spark_session, fmt: str = "delta", schema: str | None = None):
        if fmt not in ("delta", "parquet"):
            raise ValueError("fmt doit valoir 'delta' ou 'parquet'")
        if schema is not None:
            validate_sql_identifier(schema, allow_qualified=False)
        self.spark = spark_session
        self.fmt = fmt
        self.schema = schema

    def qualify_name(self, name: str) -> str:
        """Valide le nom de table et applique le schéma configuré si non qualifié."""
        validate_sql_identifier(name, allow_qualified=True)
        return f"{self.schema}.{name}" if self.schema and "." not in name else name

    def exists(self, name: str) -> bool:
        return self.spark.catalog.tableExists(self.qualify_name(name))

    def read(self, name: str):
        return self.spark.table(self.qualify_name(name))

    def _writer(self, df, mode: str):
        return df.write.format(self.fmt).mode(mode)

    def overwrite(self, df, name: str) -> None:
        writer = self._writer(df, "overwrite")
        if self.fmt == "delta":
            writer = writer.option("overwriteSchema", "true")
        writer.saveAsTable(self.qualify_name(name))

    def append(self, df, name: str, merge_schema: bool = False) -> None:
        writer = self._writer(df, "append")
        if merge_schema and self.fmt == "delta":
            writer = writer.option("mergeSchema", "true")
        writer.saveAsTable(self.qualify_name(name))

    def _materialize_table(self, name: str):
        return self.read(name).localCheckpoint(eager=True)

    def replace_where(self, df, name: str, column: str, values: list[Any]) -> str:
        """Remplace atomiquement les partitions/lignes correspondant aux valeurs fournies."""
        validate_sql_identifier(column, allow_qualified=False)
        values_list = list(values)
        if not self.exists(name):
            self.overwrite(df, name)
            return "created"
        if not values_list:
            return "unchanged"

        if self.fmt == "delta":
            literals_sql = ",".join(format_sql_literal(val) for val in values_list)
            (
                df.write.format("delta")
                .mode("overwrite")
                .option("replaceWhere", f"{column} IN ({literals_sql})")
                .saveAsTable(self.qualify_name(name))
            )
        else:
            from pyspark.sql import functions as F

            df_existing_kept = self._materialize_table(name).filter(~F.col(column).isin(values_list))
            self.overwrite(df_existing_kept.unionByName(df, allowMissingColumns=True), name)
        return "replaced"

    def upsert(self, df, name: str, keys: list[str]) -> str:
        """Exécute un MERGE idempotent sur clé métier (mise à jour + insertion sans suppression)."""
        if not keys:
            raise ValueError("upsert requiert au moins une clé métier")
        for key_col in keys:
            validate_sql_identifier(key_col, allow_qualified=False)

        if not self.exists(name):
            self.overwrite(df, name)
            return "created"

        if self.fmt == "delta":
            name_digest = hashlib.sha256(name.encode("utf-8")).hexdigest()[:12]
            temp_view = f"_upsert_src_{name_digest}"
            df.createOrReplaceTempView(temp_view)
            join_condition = " AND ".join(f"t.`{key}` <=> s.`{key}`" for key in keys)
            self.spark.sql(
                f"MERGE INTO {self.qualify_name(name)} t USING {temp_view} s ON {join_condition} "
                f"WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
            )
        else:
            df_existing = self._materialize_table(name)
            df_preserved = df_existing.join(df.select(*keys).distinct(), keys, "left_anti")
            self.overwrite(df_preserved.unionByName(df, allowMissingColumns=True), name)
        return "merged"

    def optimize(self, name: str, zorder_columns: list[str] | None = None) -> bool:
        """Compacte une table Delta avec Z-Order optionnel (sans effet en mode Parquet local)."""
        if self.fmt != "delta":
            return False
        zorder_clause = ""
        if zorder_columns:
            validated_cols = [validate_sql_identifier(col_name, allow_qualified=False) for col_name in zorder_columns]
            zorder_clause = f" ZORDER BY ({', '.join(validated_cols)})"
        self.spark.sql(f"OPTIMIZE {self.qualify_name(name)}{zorder_clause}")
        return True

    def vacuum(self, name: str, retain_hours: int = 168) -> bool:
        """Purge les fichiers Delta obsolètes au-delà de la période de rétention."""
        hours = int(retain_hours)
        if hours < 0:
            raise ValueError(f"retain_hours doit être >= 0, reçu {retain_hours!r}")
        if self.fmt != "delta":
            return False
        self.spark.sql(f"VACUUM {self.qualify_name(name)} RETAIN {hours} HOURS")
        return True

    def set_properties(self, name: str, properties: dict[str, Any]) -> bool:
        """Applique des métadonnées de gouvernance Delta TBLPROPERTIES sur la table."""
        if self.fmt != "delta":
            return False
        assignments = ", ".join(
            f"'{escape_sql_string(str(prop_key))}' = '{escape_sql_string(str(prop_val))}'"
            for prop_key, prop_val in properties.items()
        )
        self.spark.sql(f"ALTER TABLE {self.qualify_name(name)} SET TBLPROPERTIES ({assignments})")
        return True


def resolve_table_format() -> str:
    """Retourne le format de stockage actif ('delta' par défaut, ou HEALTHTEK_TABLE_FORMAT)."""
    return os.environ.get("HEALTHTEK_TABLE_FORMAT", "delta")


def resolve_landing_root() -> str:
    """Retourne la racine du dossier landing (OneLake POSIX ou variable HEALTHTEK_LANDING)."""
    fallback_root = globals().get("DEFAULT_LANDING_ROOT", "/lakehouse/default/Files/landing")
    return os.environ.get("HEALTHTEK_LANDING", fallback_root)


def create_table_io(spark_session) -> TableIO:
    """Instancie TableIO d'après la configuration d'exécution active."""
    return TableIO(spark_session, resolve_table_format())

# MARKDOWN ********************

# ## 3 · Utilitaires de résolution des fichiers landing OneLake

# CELL ********************

# ==============================================================================
# UTILITAIRES DE RÉSOLUTION DES FICHIERS LANDING ONELAKE
# ==============================================================================
def landing_filename(file_name: str, run_date: date) -> str:
    """Construit le nom horodaté d'un fichier déposé (ex: CIS_bdpm_2026-11-06.txt)."""
    base_name, extension = os.path.splitext(file_name)
    return f"{base_name}_{run_date.isoformat()}{extension}"


def extract_snapshot_date(filename: str) -> Optional[date]:
    """Extrait la date ISO présente dans un nom de fichier landing, ou None si absente."""
    match = LANDING_DATE_PATTERN.search(os.path.basename(filename))
    return date.fromisoformat(match.group(1)) if match else None


def list_landing_files(landing_root: str, subfolder: str) -> list[tuple[date, str]]:
    """Liste les dépôts valides d'un sous-dossier par date croissante (ignore .manifest.json et .part)."""
    folder_path = os.path.join(landing_root, subfolder)
    if not os.path.isdir(folder_path):
        return []
    discovered_files = []
    for entry_name in os.listdir(folder_path):
        if entry_name.endswith((".manifest.json", ".part")):
            continue
        match = LANDING_DATE_PATTERN.search(entry_name)
        if match:
            discovered_files.append((date.fromisoformat(match.group(1)), os.path.join(folder_path, entry_name)))
    return sorted(discovered_files)


def latest_landing_file(
    landing_root: str,
    subfolder: str,
    on_or_before: Optional[date] = None,
) -> Optional[str]:
    """Retourne le chemin du dépôt le plus récent (<= on_or_before si fourni)."""
    candidates = [
        (file_date, file_path)
        for file_date, file_path in list_landing_files(landing_root, subfolder)
        if on_or_before is None or file_date <= on_or_before
    ]
    return candidates[-1][1] if candidates else None


def to_spark_file_path(path: str) -> str:
    """Convertit un chemin local POSIX en URI compatible Spark file:// si nécessaire."""
    return path if "://" in path or path.startswith("file:") else f"file://{os.path.abspath(path)}"

# MARKDOWN ********************

# ---
# 
# ## 🛠️ Exploitation et dépannage
# 
# - **Rejouer une date** : renseigner `run_date` (AAAA-MM-JJ) ; toutes les étapes sont idempotentes.
# - **Diagnostiquer** : la sortie JSON de l'activité contient `status`, les étapes en échec et leurs messages ; les contrôles qualité sont historisés dans `_dq_audit_log`.
# - **Verrou** : en cas d'interruption brutale, `_ops_run_lock` expire automatiquement après `ttl_minutes` ; le pipeline libère toujours le verrou en fin de run.
# 
# *HealthTek — nb_util_io · documentation générée selon le standard du projet.*
