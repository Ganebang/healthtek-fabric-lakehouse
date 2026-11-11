# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "lh_health_analytics",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 🛡️ [UTILITY] `nb_util_validation` — Moteur Qualité (DQ) & Quarantaine Étanche
# 
# > **HealthTek** · Plateforme analytique des ruptures et tensions d'approvisionnement de médicaments (données publiques BDPM / ANSM / HAS) — Microsoft Fabric
# 
# | Élément | Détail |
# |---|---|
# | **Couche** | Socle — qualité des données |
# | **Type** | Bibliothèque (`%run`, charge config/logging/io) |
# | **Orchestration** | Inclus par `nb_dq_validate_layer` et `nb_silver_clean_bdpm` — pipeline `pl_master_health_pipeline` |
# | **Entrées** | Tables Bronze / Silver / Gold |
# | **Sorties** | `_dq_audit_log`, `_dq_quarantine` |
# | **Idempotence** | Oui (audit append-only avec `attempt_id`, quarantaine dédoublonnée par `row_hash`) |
# | **Documentation** | v3.1 — 10/11/2026 |
# 
# ## 🎯 Objectif
# 
# Implémente le cadre de gouvernance et de contrôle qualité des données sur les 3 couches médaillon :
# - Validation structurelle, d'unicité, d'intégrité référentielle, métier et de fraîcheur en **une seule passe Spark par table**.
# - Catalogue complet des règles Bronze, Silver et Gold (`get_quality_catalogue`).
# - Journalisation systématique append-only dans `_dq_audit_log` avec `attempt_id` unique par reprise.
# - Quarantaine JSON à schéma immuable et idempotente (`quarantine_rejected_rows` avec libération mémoire `finally: unpersist()`).
# 
# ## 🔗 Utilisation
# 
# Ce notebook utilitaire est **autonome** : il charge automatiquement `nb_util_config`, `nb_util_logging` et `nb_util_io` en tête de session.
# Un notebook appelant peut simplement exécuter :
# ```python
# %run nb_util_validation
# ```
# 
# ## 🧭 Plan du notebook
# 
# 1. Chargement des utilitaires
# 2. Schémas d'audit et de quarantaine, modèle `QualityRule`
# 3. Traduction des règles en SQL vectorisé
# 4. Moteur d'évaluation en une passe par table
# 5. Catalogue des règles Bronze / Silver / Gold
# 6. Persistance, quarantaine et porte qualité
# 
# ## 📤 Contrat de sortie
# 
# Aucune sortie directe : `run_quality_gate()` renvoie le résumé consommé par `nb_dq_validate_layer`.

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

# ## 2 · Schémas d'audit, de quarantaine et modèle QualityRule

# CELL ********************

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql import types as T

dq_logger = get_logger("dq")

DQ_LAYERS = ("bronze", "silver", "gold")
DQ_SEVERITIES = ("critical", "warning")
DQ_RULE_KINDS = (
    "not_null", "unique", "range", "regex", "accepted", "fk", "row_count",
    "predicate", "freshness", "data_freshness", "schema", "reconcile",
    "contiguous_dates", "exists",
)

AUDIT_SCHEMA = T.StructType([
    T.StructField("rule", T.StringType(), False),
    T.StructField("description", T.StringType()),
    T.StructField("table_name", T.StringType(), False),
    T.StructField("layer", T.StringType(), False),
    T.StructField("severity", T.StringType(), False),
    T.StructField("rule_type", T.StringType(), False),
    T.StructField("column_name", T.StringType()),
    T.StructField("total_rows", T.LongType(), False),
    T.StructField("failures", T.LongType(), False),
    T.StructField("failure_pct", T.DoubleType(), False),
    T.StructField("pass_rate", T.DoubleType(), False),
    T.StructField("passed", T.BooleanType(), False),
    T.StructField("skipped", T.BooleanType(), False),
    T.StructField("checked_at", T.TimestampType(), False),
    T.StructField("pipeline_run_id", T.StringType(), False),
    T.StructField("attempt_id", T.StringType(), False),
    T.StructField("as_of_date", T.DateType(), False),
    T.StructField("error", T.StringType()),
])

QUARANTINE_SCHEMA = T.StructType([
    T.StructField("source_table", T.StringType()),
    T.StructField("rule", T.StringType()),
    T.StructField("layer", T.StringType()),
    T.StructField("pipeline_run_id", T.StringType()),
    T.StructField("row_hash", T.StringType()),
    T.StructField("row_json", T.StringType()),
    T.StructField("quarantined_at", T.TimestampType()),
])


class GateError(RuntimeError):
    """Exception levée lorsqu'une porte qualité bloque le pipeline (fail_fast=True)."""


@dataclass(frozen=True)
class QualityRule:
    """Définition déclarative et auto-validée d'une règle de qualité de données."""

    name: str
    layer: str
    table: str
    kind: str
    severity: str = "critical"
    column: Optional[str] = None
    params: dict = field(default_factory=dict)
    description: str = ""
    optional: bool = False
    max_failure_pct: float = 0.0

    def __post_init__(self):
        if self.layer not in DQ_LAYERS:
            raise ValueError(f"{self.name}: layer {self.layer!r} hors de {DQ_LAYERS}")
        if self.severity not in DQ_SEVERITIES:
            raise ValueError(f"{self.name}: severity {self.severity!r} hors de {DQ_SEVERITIES}")
        if self.kind not in DQ_RULE_KINDS:
            raise ValueError(f"{self.name}: kind {self.kind!r} hors de {DQ_RULE_KINDS}")
        requires_column = ("not_null", "unique", "range", "regex", "accepted", "fk", "data_freshness", "contiguous_dates")
        if self.kind in requires_column and not (self.column or self.params.get("columns")):
            raise ValueError(f"{self.name}: colonne requise pour la règle '{self.kind}'")
        if self.kind in ("predicate", "exists") and "expr" not in self.params:
            raise ValueError(f"{self.name}: params.expr requis pour la règle '{self.kind}'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3 · Traduction des règles en expressions SQL

# CELL ********************

def _format_rule_result(
    rule: QualityRule,
    total_rows: int = 0,
    failures: int = 0,
    passed: bool = True,
    skipped: bool = False,
    error: Optional[str] = None,
    evaluated_rows: Optional[int] = None,
) -> dict:
    evaluated = total_rows if evaluated_rows is None else evaluated_rows
    failure_pct = 0.0 if not evaluated or failures < 0 else round(failures * 100.0 / evaluated, 4)
    return {
        "rule": rule.name,
        "description": rule.description,
        "table_name": rule.table,
        "layer": rule.layer,
        "severity": rule.severity,
        "rule_type": rule.kind,
        "column_name": rule.column,
        "total_rows": int(total_rows),
        "failures": int(failures),
        "failure_pct": failure_pct,
        "pass_rate": round(1 - failure_pct / 100, 6) if failures >= 0 else 0.0,
        "passed": bool(passed),
        "skipped": skipped,
        "error": error,
    }


def _substitute_as_of_date(sql_expr: str, as_of_date: date) -> str:
    return sql_expr.replace(":as_of", f"DATE'{as_of_date.isoformat()}'")


def build_row_violation_sql(rule: QualityRule, as_of_date: date) -> Optional[str]:
    """Construit l'expression SQL Spark identifiant les lignes en infraction."""
    quoted_col = f"`{validate_sql_identifier(rule.column)}`" if rule.column else None
    rule_params = rule.params

    if rule.kind == "not_null":
        return f"{quoted_col} IS NULL"

    if rule.kind == "range":
        bounds = []
        if "min" in rule_params:
            bounds.append(f"{quoted_col} < {rule_params['min']}")
        if "max" in rule_params:
            bounds.append(f"{quoted_col} > {rule_params['max']}")
        if not bounds:
            raise ValueError(f"{rule.name}: règle 'range' sans borne min ou max")
        out_of_range = "(" + " OR ".join(bounds) + ")"
        return out_of_range + (f" OR {quoted_col} IS NULL" if rule_params.get("required") else "")

    if rule.kind == "regex":
        escaped_pattern = escape_sql_string(str(rule_params["pattern"]))
        return f"{quoted_col} IS NOT NULL AND NOT ({quoted_col} RLIKE '{escaped_pattern}')"

    if rule.kind == "accepted":
        escaped_values = ", ".join(format_sql_literal(val) for val in rule_params["values"])
        return f"{quoted_col} IS NOT NULL AND {quoted_col} NOT IN ({escaped_values})"

    if rule.kind == "predicate":
        return _substitute_as_of_date(rule_params["expr"], as_of_date)

    return None


def _finalize_threshold(rule: QualityRule, total_rows: int, failures: int) -> dict:
    if total_rows == 0:
        if rule.params.get("allow_empty"):
            return _format_rule_result(rule, 0, 0, True)
        return _format_rule_result(rule, 0, 1, False, error="table vide")
    passed = failures * 100.0 / total_rows <= rule.max_failure_pct
    return _format_rule_result(rule, total_rows, failures, passed)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4 · Moteur d'évaluation vectorisé (une passe par table)

# CELL ********************

def _evaluate_complex_rule(
    table_io: TableIO,
    df: DataFrame,
    rule: QualityRule,
    as_of_date: date,
    current_utc: datetime,
    scale: float,
) -> dict:
    """Évalue les contrôles multi-lignes ou inter-tables (unicité, FK, fraîcheur, schéma)."""
    rule_params = rule.params
    if rule.kind == "row_count":
        total_rows = df.count()
        min_required = int(rule_params["min_rows"] * scale)
        return _format_rule_result(
            rule, total_rows, max(min_required - total_rows, 0), total_rows >= max(min_required, 1)
        )

    total_rows = df.count()
    if total_rows == 0 and not rule_params.get("allow_empty"):
        return _format_rule_result(rule, 0, 1, False, error="table vide")

    if rule.kind == "schema":
        missing_cols = [col for col in rule_params["columns"] if col not in df.columns]
        error_msg = f"colonnes absentes : {missing_cols}" if missing_cols else None
        return _format_rule_result(rule, total_rows, len(missing_cols), not missing_cols, error=error_msg)

    if rule.kind == "unique":
        key_cols = rule_params.get("columns") or [rule.column]
        duplicate_rows = (
            df.groupBy(*key_cols)
            .count()
            .filter("count > 1")
            .agg(F.sum(F.col("count") - 1))
            .first()[0]
            or 0
        )
        return _finalize_threshold(rule, total_rows, int(duplicate_rows))

    if rule.kind == "fk":
        df_ref = table_io.read(rule_params["ref_table"]).select(
            F.col(rule_params["ref_column"]).alias("_ref_key")
        ).distinct()
        df_non_null = df.filter(F.col(rule.column).isNotNull())
        non_null_count = df_non_null.count()
        orphan_count = df_non_null.join(df_ref, df_non_null[rule.column] == df_ref["_ref_key"], "left_anti").count()
        return _finalize_threshold(rule, non_null_count, orphan_count)

    if rule.kind == "exists":
        matching_rows = df.filter(F.expr(_substitute_as_of_date(rule_params["expr"], as_of_date))).count()
        return _format_rule_result(
            rule,
            total_rows,
            0 if matching_rows >= 1 else 1,
            matching_rows >= 1,
            error=None if matching_rows >= 1 else "ligne attendue absente",
        )

    if rule.kind == "reconcile":
        ref_rows = table_io.read(rule_params["ref_table"]).count()
        loss_pct = 0.0 if ref_rows == 0 else max(ref_rows - total_rows, 0) * 100.0 / ref_rows
        return _format_rule_result(
            rule,
            total_rows,
            max(ref_rows - total_rows, 0),
            loss_pct <= rule_params.get("max_loss_pct", 5.0),
            evaluated_rows=ref_rows,
        )

    if rule.kind == "freshness":
        max_epoch = df.agg(F.max(F.unix_timestamp(F.col(rule.column or "_ingested_at")))).first()[0]
        if max_epoch is None:
            return _format_rule_result(rule, 1, 1, False, error="aucune date")
        age_hours = (current_utc.timestamp() - max_epoch) / 3600.0
        is_fresh = age_hours <= rule_params["max_age_hours"]
        error_msg = None if is_fresh else f"âge {age_hours:.1f} h > {rule_params['max_age_hours']} h"
        return _format_rule_result(rule, 1, 0 if is_fresh else 1, is_fresh, error=error_msg)

    if rule.kind == "data_freshness":
        max_data_date = df.agg(F.max(F.col(rule.column))).first()[0]
        if max_data_date is None:
            return _format_rule_result(rule, 1, 1, False, error="aucune date")
        age_days = (as_of_date - max_data_date).days
        is_fresh = age_days <= rule_params["max_age_days"]
        error_msg = None if is_fresh else f"donnée la plus récente : {age_days} j > {rule_params['max_age_days']} j"
        return _format_rule_result(rule, 1, 0 if is_fresh else 1, is_fresh, error=error_msg)

    if rule.kind == "contiguous_dates":
        date_stats = df.agg(
            F.count("*").alias("row_count"),
            F.min(rule.column).alias("min_date"),
            F.max(rule.column).alias("max_date"),
            F.countDistinct(rule.column).alias("distinct_dates"),
        ).first()
        expected_days = (date_stats["max_date"] - date_stats["min_date"]).days + 1
        gap_count = max(expected_days - date_stats["distinct_dates"], 0) + (
            date_stats["row_count"] - date_stats["distinct_dates"]
        )
        return _format_rule_result(rule, total_rows, gap_count, gap_count == 0)

    raise ValueError(f"Type de règle non géré : {rule.kind}")


def evaluate_quality_rules(
    table_io: TableIO,
    rules: list[QualityRule],
    as_of_date: date,
    current_utc: Optional[datetime] = None,
    scale: float = 1.0,
) -> list[dict]:
    """Exécute les règles regroupées par table (une seule passe Spark par table)."""
    now_utc = current_utc or utc_now()
    all_results: list[dict] = []
    rules_by_table: dict[str, list[QualityRule]] = {}
    for rule in rules:
        rules_by_table.setdefault(rule.table, []).append(rule)

    row_rule_kinds = ("not_null", "range", "regex", "accepted", "predicate")

    for table_name, table_rules in rules_by_table.items():
        if not table_io.exists(table_name):
            for rule in table_rules:
                if rule.optional:
                    all_results.append(_format_rule_result(rule, 0, 0, True, skipped=True, error="table absente (facultative)"))
                else:
                    all_results.append(_format_rule_result(rule, 0, 1, False, skipped=False, error="table absente"))
            continue

        df_table = table_io.read(table_name)
        row_rules = [rule for rule in table_rules if rule.kind in row_rule_kinds]
        agg_exprs = [F.count(F.lit(1)).alias("_total")]
        indexed_row_rules: dict[int, QualityRule] = {}

        for idx, rule in enumerate(row_rules):
            try:
                violation_sql = build_row_violation_sql(rule, as_of_date)
                agg_exprs.append(F.sum(F.when(F.expr(violation_sql), 1).otherwise(0)).alias(f"v{idx}"))
                indexed_row_rules[idx] = rule
            except Exception as exc:  # noqa: BLE001
                all_results.append(_format_rule_result(rule, -1, -1, False, error=f"{type(exc).__name__}: {exc}"))

        if indexed_row_rules:
            try:
                agg_row = df_table.agg(*agg_exprs).first()
                total_rows = int(agg_row["_total"])
                for idx, rule in indexed_row_rules.items():
                    all_results.append(_finalize_threshold(rule, total_rows, int(agg_row[f"v{idx}"] or 0)))
            except Exception:  # noqa: BLE001 - isoler la règle fautive en rejouant séquentiellement
                fallback_total = int(df_table.count())
                for rule in indexed_row_rules.values():
                    try:
                        violation_sql = build_row_violation_sql(rule, as_of_date)
                        violation_count = df_table.agg(F.sum(F.when(F.expr(violation_sql), 1).otherwise(0))).first()[0]
                        all_results.append(_finalize_threshold(rule, fallback_total, int(violation_count or 0)))
                    except Exception as single_exc:  # noqa: BLE001
                        all_results.append(
                            _format_rule_result(rule, -1, -1, False, error=f"{type(single_exc).__name__}: {single_exc}")
                        )

        complex_rules = [rule for rule in table_rules if rule.kind not in row_rule_kinds]
        for rule in complex_rules:
            try:
                all_results.append(_evaluate_complex_rule(table_io, df_table, rule, as_of_date, now_utc, scale))
            except Exception as exc:  # noqa: BLE001
                all_results.append(_format_rule_result(rule, -1, -1, False, error=f"{type(exc).__name__}: {exc}"))

    return all_results

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5 · Catalogue des règles Bronze, Silver et Gold

# CELL ********************

def get_quality_catalogue() -> list[QualityRule]:
    """Catalogue complet des règles Bronze, Silver et Gold du projet HealthTek."""
    rules: list[QualityRule] = []

    # ---------------- COUCHE BRONZE
    min_rows_by_source = {
        "specialties": 10_000,
        "presentations": 15_000,
        "compositions": 20_000,
        "generics": 8_000,
        "conditions": 15_000,
        "ruptures": 300,
        "smr": 10_000,
        "asmr": 5_000,
        "mitm": 5_000,
        "important_info": 5_000,
        "ct_links": 1,
    }
    for source_key, source_spec in BDPM_SOURCES.items():
        rules.append(
            QualityRule(
                name=f"brz_{source_key}_min_rows",
                layer="bronze",
                table=source_spec.table,
                kind="row_count",
                severity="critical" if source_spec.critical else "warning",
                params={"min_rows": min_rows_by_source[source_key]},
                optional=source_spec.optional,
                description=f"{source_spec.table} >= {min_rows_by_source[source_key]} lignes",
            )
        )
        rules.append(
            QualityRule(
                name=f"brz_{source_key}_schema",
                layer="bronze",
                table=source_spec.table,
                kind="schema",
                severity="critical" if not source_spec.optional else "warning",
                params={"columns": list(source_spec.columns) + ["_ingested_at", "_source_file", "_snapshot_date"]},
                optional=source_spec.optional,
                description="Colonnes du contrat et d'audit présentes",
            )
        )

    rules += [
        QualityRule("brz_spec_cis_nn", "bronze", "bronze_bdpm_specialties", "not_null", "critical", "cis_code"),
        QualityRule("brz_cip_cis_nn", "bronze", "bronze_bdpm_cip", "not_null", "critical", "cis_code"),
        QualityRule("brz_cip_cip13_nn", "bronze", "bronze_bdpm_cip", "not_null", "critical", "cip13"),
        QualityRule("brz_rupt_cis_nn", "bronze", "bronze_bdpm_ruptures", "not_null", "critical", "cis_code"),
        QualityRule("brz_comp_cis_nn", "bronze", "bronze_bdpm_compositions", "not_null", "critical", "cis_code"),
        QualityRule("brz_gen_group_nn", "bronze", "bronze_bdpm_generics", "not_null", "critical", "generic_group_id"),
        QualityRule("brz_mitm_cis_nn", "bronze", "bronze_bdpm_mitm", "not_null", "critical", "cis_code"),
        QualityRule("brz_info_cis_nn", "bronze", "bronze_bdpm_important_info", "not_null", "critical", "cis_code"),
        QualityRule(
            "brz_spec_encoding", "bronze", "bronze_bdpm_specialties", "predicate", "warning",
            params={"expr": "denomination LIKE '%\ufffd%' OR etat_commercialisation LIKE '%\ufffd%'"},
            description="Caractères U+FFFD : accents perdus en amont",
        ),
        QualityRule(
            "brz_mitm_encoding", "bronze", "bronze_bdpm_mitm", "predicate", "warning",
            params={"expr": "denomination LIKE '%\ufffd%'"},
            max_failure_pct=100.0,
            description="U+FFFD dans MITM (informatif)",
        ),
        QualityRule("brz_cip_fresh_load", "bronze", "bronze_bdpm_cip", "freshness", "critical", "_ingested_at", {"max_age_hours": 36}, "Présentations chargées depuis < 36 h"),
        QualityRule("brz_rupt_fresh_load", "bronze", "bronze_bdpm_ruptures", "freshness", "critical", "_ingested_at", {"max_age_hours": 360}, "Ruptures chargées depuis < 15 jours"),
        QualityRule("brz_spec_fresh_load", "bronze", "bronze_bdpm_specialties", "freshness", "warning", "_ingested_at", {"max_age_hours": 768}),
    ]

    # ---------------- COUCHE SILVER (incluant fraîcheur donnée ruptures corrigée)
    rules += [
        QualityRule("slv_spec_cis_uniq", "silver", "silver_bdpm_specialties", "unique", "critical", "cis_code"),
        QualityRule("slv_spec_denom_nn", "silver", "silver_bdpm_specialties", "not_null", "critical", "denomination"),
        QualityRule("slv_spec_amm_known", "silver", "silver_bdpm_specialties", "predicate", "warning", params={"expr": "statut_amm IS NOT NULL AND lower(statut_amm) NOT LIKE 'autorisation %'"}),
        QualityRule("slv_spec_rows", "silver", "silver_bdpm_specialties", "row_count", "critical", params={"min_rows": 10_000}),
        QualityRule("slv_cip_rows", "silver", "silver_bdpm_cip", "row_count", "critical", params={"min_rows": 15_000}),
        QualityRule("slv_cip_cip13_uniq", "silver", "silver_bdpm_cip", "unique", "critical", "cip13"),
        QualityRule("slv_cip_cip13_fmt", "silver", "silver_bdpm_cip", "regex", "critical", "cip13", {"pattern": "^[0-9]{13}$"}),
        QualityRule("slv_cip_cip13_ean", "silver", "silver_bdpm_cip", "predicate", "warning", params={"expr": "NOT cip13_valid"}, max_failure_pct=1.0),
        QualityRule("slv_cip_price_range", "silver", "silver_bdpm_cip", "range", "critical", "prix_hors_honoraires", {"min": 0, "max": 100_000}),
        QualityRule("slv_cip_taux_known", "silver", "silver_bdpm_cip", "accepted", "warning", "taux_remboursement", {"values": [0.0, 15.0, 30.0, 65.0, 100.0]}),
        QualityRule("slv_cip_fk_spec", "silver", "silver_bdpm_cip", "fk", "warning", "cis_code", {"ref_table": "silver_bdpm_specialties", "ref_column": "cis_code"}, max_failure_pct=5.0),
        QualityRule("slv_cip_hist_rows", "silver", "silver_bdpm_cip_history", "row_count", "critical", params={"min_rows": 15_000}),
        QualityRule("slv_cip_hist_uniq", "silver", "silver_bdpm_cip_history", "unique", "critical", params={"columns": ["cip13", "snapshot_date"]}),
        QualityRule("slv_rupt_rows", "silver", "silver_bdpm_ruptures", "row_count", "critical", params={"min_rows": 300}),
        QualityRule("slv_rupt_key_uniq", "silver", "silver_bdpm_ruptures", "unique", "critical", params={"columns": ["cis_code", "cip13_key"]}),
        QualityRule("slv_rupt_dates", "silver", "silver_bdpm_ruptures", "predicate", "warning", params={"expr": "date_fin IS NOT NULL AND date_fin < date_debut"}),
        QualityRule("slv_rupt_duration", "silver", "silver_bdpm_ruptures", "range", "warning", "duration_days", {"min": 0, "max": LONG_SHORTAGE_DAYS}, max_failure_pct=5.0),
        QualityRule("slv_rupt_active_consistent", "silver", "silver_bdpm_ruptures", "predicate", "critical", params={"expr": "is_active AND code_statut NOT IN (1, 2)"}),
        QualityRule("slv_rupt_fk_spec", "silver", "silver_bdpm_ruptures", "fk", "warning", "cis_code", {"ref_table": "silver_bdpm_specialties", "ref_column": "cis_code"}, max_failure_pct=10.0),
        QualityRule(
            "slv_rupt_fresh_data", "silver", "silver_bdpm_ruptures", "data_freshness", "warning",
            "date_mise_a_jour", {"max_age_days": 30},
            description="Dernière mise à jour ANSM de moins de 30 jours", optional=True,
        ),
        QualityRule("slv_comp_rows", "silver", "silver_bdpm_compositions", "row_count", "warning", params={"min_rows": 20_000}),
        QualityRule("slv_comp_nature", "silver", "silver_bdpm_compositions", "accepted", "warning", "nature_composant", {"values": ["SA", "FT"]}),
        QualityRule("slv_gen_rows", "silver", "silver_bdpm_generics", "row_count", "warning", params={"min_rows": 8_000}),
        QualityRule("slv_gen_type", "silver", "silver_bdpm_generics", "accepted", "warning", "type_generique", {"values": ["0", "1", "2", "3", "4"]}),
        QualityRule("slv_cond_rows", "silver", "silver_bdpm_conditions", "row_count", "warning", params={"min_rows": 15_000}),
        QualityRule("slv_smr_dates", "silver", "silver_bdpm_smr", "not_null", "warning", "date_avis", max_failure_pct=1.0),
        QualityRule("slv_asmr_dates", "silver", "silver_bdpm_asmr", "not_null", "warning", "date_avis", max_failure_pct=1.0),
        QualityRule("slv_mitm_cis_uniq", "silver", "silver_bdpm_mitm", "unique", "critical", "cis_code"),
        QualityRule("slv_mitm_atc_valid", "silver", "silver_bdpm_mitm", "predicate", "warning", params={"expr": "NOT atc_valid"}, max_failure_pct=1.0),
        QualityRule("slv_mitm_fk_spec", "silver", "silver_bdpm_mitm", "fk", "warning", "cis_code", {"ref_table": "silver_bdpm_specialties", "ref_column": "cis_code"}, max_failure_pct=5.0),
        QualityRule("slv_info_rows", "silver", "silver_bdpm_important_info", "row_count", "warning", params={"min_rows": 5_000}),
        QualityRule("slv_info_grain", "silver", "silver_bdpm_important_info", "unique", "critical", params={"columns": ["cis_code", "date_debut", "date_fin", "info_url"]}),
        QualityRule("slv_info_dates", "silver", "silver_bdpm_important_info", "not_null", "critical", "date_debut"),
        QualityRule("slv_reconcile_spec", "silver", "silver_bdpm_specialties", "reconcile", "critical", params={"ref_table": "bronze_bdpm_specialties", "max_loss_pct": SILVER_LOSS_WARN_PCT}),
        QualityRule("slv_reconcile_cip", "silver", "silver_bdpm_cip", "reconcile", "critical", params={"ref_table": "bronze_bdpm_cip", "max_loss_pct": SILVER_LOSS_WARN_PCT}),
        QualityRule("slv_reconcile_info", "silver", "silver_bdpm_important_info", "reconcile", "warning", params={"ref_table": "bronze_bdpm_important_info", "max_loss_pct": SILVER_LOSS_WARN_PCT}),
    ]

    # ---------------- COUCHE GOLD
    gold_dimensions = (
        ("dim_drug", "drug_sk", "cis_code"),
        ("dim_presentation", "presentation_sk", "cip13"),
        ("dim_generic_group", "group_sk", "generic_group_id"),
        ("dim_substance", "substance_sk", "code_substance"),
        ("dim_atc", "atc_sk", "code_atc"),
        ("dim_status", "status_sk", "status_code"),
    )
    for dim_key, surrogate_col, natural_col in gold_dimensions:
        table_name = f"gold_{dim_key}"
        rules += [
            QualityRule(f"gld_{dim_key}_sk_uniq", "gold", table_name, "unique", "critical", surrogate_col),
            QualityRule(f"gld_{dim_key}_nat_uniq", "gold", table_name, "unique", "critical", natural_col),
            QualityRule(
                f"gld_{dim_key}_unknown", "gold", table_name, "exists", "critical",
                params={"expr": f"{surrogate_col} = {UNKNOWN_SURROGATE_KEY}"},
                description="Membre « Inconnu » (-1) présent",
            ),
        ]

    rules += [
        QualityRule("gld_dim_opinion_sk_uniq", "gold", "gold_dim_opinion_level", "unique", "critical", "level_sk"),
        QualityRule("gld_dim_drug_rows", "gold", "gold_dim_drug", "row_count", "critical", params={"min_rows": 10_000}),
        QualityRule("gld_dim_drug_inferred", "gold", "gold_dim_drug", "predicate", "warning", params={"expr": "is_inferred"}, max_failure_pct=30.0),
        QualityRule("gld_dim_time_sk_uniq", "gold", "gold_dim_time", "unique", "critical", "time_sk"),
        QualityRule("gld_dim_time_contiguous", "gold", "gold_dim_time", "contiguous_dates", "critical", "date_value"),
        QualityRule("gld_price_rows", "gold", "gold_fact_presentation_price", "row_count", "critical", params={"min_rows": 15_000}),
        QualityRule("gld_price_grain", "gold", "gold_fact_presentation_price", "unique", "critical", params={"columns": ["cip13", "snapshot_date"]}),
        QualityRule("gld_price_drug_fk", "gold", "gold_fact_presentation_price", "fk", "critical", "drug_sk", {"ref_table": "gold_dim_drug", "ref_column": "drug_sk"}),
        QualityRule("gld_price_pres_fk", "gold", "gold_fact_presentation_price", "fk", "critical", "presentation_sk", {"ref_table": "gold_dim_presentation", "ref_column": "presentation_sk"}),
        QualityRule("gld_price_time_fk", "gold", "gold_fact_presentation_price", "fk", "critical", "time_sk", {"ref_table": "gold_dim_time", "ref_column": "time_sk"}),
        QualityRule("gld_price_unknown_drug", "gold", "gold_fact_presentation_price", "predicate", "warning", params={"expr": f"drug_sk = {UNKNOWN_SURROGATE_KEY}"}, max_failure_pct=1.0),
        QualityRule("gld_short_rows", "gold", "gold_fact_shortage", "row_count", "critical", params={"min_rows": 300}),
        QualityRule("gld_short_grain", "gold", "gold_fact_shortage", "unique", "critical", "shortage_key"),
        QualityRule("gld_short_drug_fk", "gold", "gold_fact_shortage", "fk", "critical", "drug_sk", {"ref_table": "gold_dim_drug", "ref_column": "drug_sk"}),
        QualityRule("gld_short_pres_fk", "gold", "gold_fact_shortage", "fk", "critical", "presentation_sk", {"ref_table": "gold_dim_presentation", "ref_column": "presentation_sk"}),
        QualityRule("gld_short_status_fk", "gold", "gold_fact_shortage", "fk", "critical", "status_sk", {"ref_table": "gold_dim_status", "ref_column": "status_sk"}),
        QualityRule("gld_short_time_fk", "gold", "gold_fact_shortage", "fk", "critical", "time_sk", {"ref_table": "gold_dim_time", "ref_column": "time_sk"}),
        QualityRule("gld_short_unknown_drug", "gold", "gold_fact_shortage", "predicate", "warning", params={"expr": f"drug_sk = {UNKNOWN_SURROGATE_KEY}"}, max_failure_pct=1.0),
        QualityRule("gld_opinion_rows", "gold", "gold_fact_has_opinion", "row_count", "warning", params={"min_rows": 10_000}, optional=True),
        QualityRule("gld_opinion_grain", "gold", "gold_fact_has_opinion", "unique", "critical", "opinion_key", optional=True),
        QualityRule("gld_opinion_drug_fk", "gold", "gold_fact_has_opinion", "fk", "critical", "drug_sk", {"ref_table": "gold_dim_drug", "ref_column": "drug_sk"}, optional=True),
        QualityRule("gld_opinion_level_fk", "gold", "gold_fact_has_opinion", "fk", "critical", "level_sk", {"ref_table": "gold_dim_opinion_level", "ref_column": "level_sk"}, optional=True),
        QualityRule("gld_opinion_time_fk", "gold", "gold_fact_has_opinion", "fk", "critical", "time_sk", {"ref_table": "gold_dim_time", "ref_column": "time_sk"}, optional=True),
        QualityRule("gld_info_grain", "gold", "gold_fact_important_info", "unique", "critical", "info_key", optional=True),
        QualityRule("gld_info_drug_fk", "gold", "gold_fact_important_info", "fk", "critical", "drug_sk", {"ref_table": "gold_dim_drug", "ref_column": "drug_sk"}, optional=True),
        QualityRule("gld_info_time_fk", "gold", "gold_fact_important_info", "fk", "critical", "time_sk", {"ref_table": "gold_dim_time", "ref_column": "time_sk"}, optional=True),
        QualityRule("gld_comp_grain", "gold", "gold_fact_composition", "unique", "critical", "composition_key"),
        QualityRule("gld_comp_drug_fk", "gold", "gold_fact_composition", "fk", "critical", "drug_sk", {"ref_table": "gold_dim_drug", "ref_column": "drug_sk"}),
        QualityRule("gld_comp_substance_fk", "gold", "gold_fact_composition", "fk", "critical", "substance_sk", {"ref_table": "gold_dim_substance", "ref_column": "substance_sk"}),
        QualityRule("gld_kpi_risk_score_nn", "gold", "gold_kpi_shortage_risk_score", "not_null", "critical", "risk_score"),
        QualityRule("gld_kpi_risk_cat", "gold", "gold_kpi_shortage_risk_score", "accepted", "critical", "risk_category", {"values": ["Critique", "Élevé", "Modéré", "Faible"]}),
        QualityRule("gld_kpi_risk_uniq", "gold", "gold_kpi_shortage_risk_score", "unique", "critical", "cis_code"),
        QualityRule("gld_kpi_gap_range", "gold", "gold_kpi_generic_price_gap", "range", "warning", "gap_pct", {"min": -100, "max": 100}, max_failure_pct=5.0),
        QualityRule("gld_kpi_gap_uniq", "gold", "gold_kpi_generic_price_gap", "unique", "critical", "generic_group_id"),
        QualityRule("gld_kpi_move_uniq", "gold", "gold_kpi_price_movement", "unique", "critical", "cip13", params={"allow_empty": True}, optional=True),
        QualityRule("gld_kpi_month_grain", "gold", "gold_kpi_shortage_monthly", "unique", "critical", params={"columns": ["year_month", "code_statut"]}),
    ]

    rule_names = [rule.name for rule in rules]
    assert len(rule_names) == len(set(rule_names)), "Noms de règles Data Quality dupliqués"
    return rules

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6 · Persistance de l'audit, quarantaine et porte qualité

# CELL ********************

def persist_audit_results(
    table_io: TableIO,
    results: list[dict],
    run_id: str,
    attempt_id: str,
    as_of_date: date,
) -> int:
    """Enregistre les résultats DQ dans _dq_audit_log en mode append-only."""
    if not results:
        return 0
    checked_at = utc_now().replace(tzinfo=None)
    audit_rows = [
        (
            res["rule"], res.get("description"), res["table_name"], res["layer"],
            res["severity"], res["rule_type"], res.get("column_name"),
            int(res["total_rows"]), int(res["failures"]), float(res["failure_pct"]),
            float(res["pass_rate"]), bool(res["passed"]), bool(res.get("skipped", False)),
            checked_at, run_id, attempt_id, as_of_date, res.get("error"),
        )
        for res in results
    ]
    df_audit = table_io.spark.createDataFrame(audit_rows, AUDIT_SCHEMA)
    table_io.append(df_audit, AUDIT_TABLE)
    return len(audit_rows)


def quarantine_rejected_rows(
    table_io: TableIO,
    df_rejected: DataFrame,
    source_table: str,
    rule_name: Optional[str],
    layer: str,
    run_id: str,
    max_rows: int = 20_000,
) -> int:
    """Route les lignes rejetées vers _dq_quarantine avec un schéma JSON immuable et idempotent."""
    source_cols = [col for col in df_rejected.columns if col != "_reject_reason"]
    rule_col = F.col("_reject_reason") if rule_name is None else F.lit(rule_name)
    json_payload_col = F.to_json(F.struct(*[F.col(col).cast("string").alias(col) for col in source_cols]))

    df_quarantine = df_rejected.limit(max_rows).select(
        F.lit(source_table).alias("source_table"),
        rule_col.alias("rule"),
        F.lit(layer).alias("layer"),
        F.lit(run_id).alias("pipeline_run_id"),
        F.sha2(json_payload_col, 256).alias("row_hash"),
        json_payload_col.alias("row_json"),
        F.current_timestamp().alias("quarantined_at"),
    )

    if table_io.exists(QUARANTINE_TABLE):
        df_existing = (
            table_io.read(QUARANTINE_TABLE)
            .filter(F.col("pipeline_run_id") == run_id)
            .select("rule", "row_hash")
        )
        df_quarantine = df_quarantine.join(df_existing, ["rule", "row_hash"], "left_anti")

    df_quarantine_cached = df_quarantine.dropDuplicates(["rule", "row_hash"]).cache()
    try:
        inserted_count = df_quarantine_cached.count()
        if inserted_count:
            table_io.append(df_quarantine_cached, QUARANTINE_TABLE)
        return inserted_count
    finally:
        df_quarantine_cached.unpersist()


def evaluate_quality_gate(
    results: list[dict],
    layer: str,
    fail_fast: bool,
    run_id: str,
    elapsed_seconds: float = 0.0,
    extra: dict | None = None,
) -> dict:
    """Décide de l'ouverture de la porte qualité et lève GateError si échec critique."""
    critical_failures = [res for res in results if not res["passed"] and res["severity"] == "critical"]
    warning_failures = [res for res in results if not res["passed"] and res["severity"] == "warning"]

    payload = {
        "gate_passed": not critical_failures,
        "layer": layer,
        "total_checks": len(results),
        "passed": sum(res["passed"] for res in results),
        "warnings": len(warning_failures),
        "critical_failures": len(critical_failures),
        "failed_rules": [res["rule"] for res in critical_failures],
        "warning_rules": [res["rule"] for res in warning_failures],
        "duration_seconds": round(elapsed_seconds, 1),
        "pipeline_run_id": run_id,
        **(extra or {}),
    }

    if critical_failures and fail_fast:
        details_msg = ", ".join(
            f"{res['rule']} ({res.get('error') or str(res['failures']) + ' lignes'})"
            for res in critical_failures
        )
        raise GateError(f"Porte qualité {layer} : {len(critical_failures)} contrôle(s) critique(s) en échec : {details_msg}")
    return payload


def run_quality_gate(
    spark_session: SparkSession,
    table_io: TableIO,
    layer: str,
    fail_fast: Any,
    run_id: str,
    as_of_date: date,
    rules: list[QualityRule] | None = None,
    scale: float = 1.0,
) -> dict:
    """Point d'entrée complet d'une porte qualité : évalue, audite puis tranche."""
    del spark_session
    normalized_layer = str(layer).strip().lower()
    if normalized_layer not in DQ_LAYERS:
        raise ValueError(f"layer invalide {layer!r} ; valeurs permises {DQ_LAYERS}")
    strict_fail_fast = parse_bool(fail_fast, "fail_fast")

    started_utc = utc_now()
    rule_pool = rules if rules is not None else get_quality_catalogue()
    layer_rules = [rule for rule in rule_pool if rule.layer == normalized_layer]
    if not layer_rules:
        raise ValueError(f"Aucune règle définie pour la couche {normalized_layer}")

    results = evaluate_quality_rules(table_io, layer_rules, as_of_date, started_utc, scale)
    attempt_id = uuid.uuid4().hex[:12]
    audit_error = None
    try:
        persist_audit_results(table_io, results, run_id, attempt_id, as_of_date)
    except Exception as exc:  # noqa: BLE001 - ne pas masquer la décision métier si le log échoue
        audit_error = f"{type(exc).__name__}: {exc}"
        dq_logger.error("Audit DQ non écrit : %s", audit_error)

    elapsed_seconds = (utc_now() - started_utc).total_seconds()
    return evaluate_quality_gate(
        results,
        normalized_layer,
        strict_fail_fast,
        run_id,
        elapsed_seconds,
        {"attempt_id": attempt_id, "audit_error": audit_error},
    )

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
# *HealthTek — nb_util_validation · documentation générée selon le standard du projet.*
