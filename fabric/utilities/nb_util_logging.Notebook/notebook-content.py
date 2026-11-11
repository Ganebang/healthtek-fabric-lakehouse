# Fabric notebook source


# MARKDOWN ********************

# # 📋 [UTILITY] `nb_util_logging` — Journalisation, Paramètres, Rapports & Sortie JSON
# 
# > **HealthTek** · Plateforme analytique des ruptures et tensions d'approvisionnement de médicaments (données publiques BDPM / ANSM / HAS) — Microsoft Fabric
# 
# | Élément | Détail |
# |---|---|
# | **Couche** | Socle — observabilité |
# | **Type** | Bibliothèque sans état (`%run`) |
# | **Orchestration** | Inclus par tous les notebooks exécutables — pipeline `pl_master_health_pipeline` |
# | **Entrées** | — |
# | **Sorties** | Logger, `StepResult`, sortie JSON |
# | **Idempotence** | Oui |
# | **Documentation** | v3.1 — 10/11/2026 |
# 
# ## 🎯 Objectif
# 
# Centralise les briques transverses de pilotage et d'observabilité des notebooks Fabric :
# - Journalisation idempotente (`get_logger`).
# - Parsing strict des paramètres transmis par Data Factory (`parse_bool`, `parse_run_date`).
# - Contrat de résultat normalisé (`StepResult` & `summarize_results`).
# - Rendu visuel factorisé (`render_execution_report`, remplaçant les blocs `display()` dupliqués).
# - Retour JSON standardisé vers le pipeline Fabric (`exit_notebook_json`).
# 
# ## 🔗 Utilisation
# 
# ```python
# %run nb_util_logging
# ```
# 
# ## 🧭 Plan du notebook
# 
# 1. Logger structuré idempotent
# 2. Parsing strict des paramètres pipeline
# 3. Contrat `StepResult`, agrégation, rapport et `exit_notebook_json`
# 
# ## 📤 Contrat de sortie
# 
# Aucune sortie : fournit `exit_notebook_json(payload)` utilisé par chaque étape.

# MARKDOWN ********************

# ## 1 · Logger structuré et horodatage UTC

# CELL ********************

from __future__ import annotations

import json
import logging
import os
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Optional

TRUE_VALUES = {"true", "1", "yes", "oui"}
FALSE_VALUES = {"false", "0", "no", "non"}


def utc_now() -> datetime:
    """Retourne l'horodatage courant UTC."""
    return datetime.now(timezone.utc)


LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
LOG_LEVEL = os.environ.get("HEALTHTEK_LOG_LEVEL", "INFO").upper()


def get_logger(name: str) -> logging.Logger:
    """Retourne un logger structuré idempotent pour l'espace de noms HealthTek."""
    logger = logging.getLogger(f"health_analytics.{name}")
    if not logger.handlers:
        stream_handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT)
        formatter.converter = time.gmtime  # horodatage UTC (cohérent avec spark.sql.session.timeZone)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
        logger.propagate = False
    logger.setLevel(LOG_LEVEL)
    return logger

# MARKDOWN ********************

# ## 2 · Parsing strict des paramètres et bannière de traçabilité

# CELL ********************

def parse_bool(value: Any, param_name: str = "paramètre") -> bool:
    """Convertit strictement un paramètre chaîne ou booléen (ex: 'false' -> False).

    Lève ValueError sur toute valeur non reconnue afin d'éviter qu'une faute de frappe
    ne bascule silencieusement le comportement du pipeline.
    """
    if isinstance(value, bool):
        return value
    normalized_value = str(value).strip().lower()
    if normalized_value in TRUE_VALUES:
        return True
    if normalized_value in FALSE_VALUES:
        return False
    allowed = sorted(TRUE_VALUES | FALSE_VALUES)
    raise ValueError(f"{param_name} doit être l'une de {allowed}, reçu {value!r}")


def parse_run_date(value: Optional[str]) -> date:
    """Parse une date ISO AAAA-MM-JJ ; vide ou 'today' renvoie la date du jour UTC."""
    if value in (None, "", "today"):
        return utc_now().date()
    return date.fromisoformat(str(value).strip())


def init_notebook_banner(
    notebook_name: str,
    run_id: str,
    reference_date: date,
    table_format: str,
) -> None:
    """Affiche une bannière d'initialisation uniforme au démarrage de chaque notebook."""
    get_logger(notebook_name).info(
        "🚀 Initialisation | Run ID: %s | Date de référence: %s | Format: %s",
        run_id, reference_date.isoformat(), table_format,
    )

# MARKDOWN ********************

# ## 3 · Contrat StepResult, rapport et sortie JSON

# CELL ********************

@dataclass
class StepResult:
    """Contrat uniforme de compte-rendu par étape ou table traitée."""

    source: str
    table: str = ""
    status: str = "SUCCESS"          # SUCCESS | FAILED | SKIPPED
    error: Optional[str] = None
    rows: int = 0
    columns: int = 0
    details: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)

    @property
    def ok(self) -> bool:
        return self.status in ("SUCCESS", "SKIPPED")


def summarize_results(
    results: list[StepResult],
    critical_sources: set[str] | None = None,
    run_id: str = "manual",
    elapsed_seconds: float = 0.0,
    extra: dict | None = None,
    fail_on_critical: bool = True,
) -> dict:
    """Agrège les résultats d'une phase et lève RuntimeError si une source critique a échoué."""
    critical_set = critical_sources or set()
    failed_steps = [res for res in results if res.status == "FAILED"]
    failed_critical = [res.source for res in results if res.source in critical_set and res.status != "SUCCESS"]

    payload = {
        "status": "FAILED" if failed_critical else ("PARTIAL" if failed_steps else "SUCCESS"),
        "total": len(results),
        "succeeded": sum(res.status == "SUCCESS" for res in results),
        "failed": len(failed_steps),
        "skipped": sum(res.status == "SKIPPED" for res in results),
        "total_rows": sum(res.rows for res in results),
        "duration_seconds": round(elapsed_seconds, 1),
        "pipeline_run_id": run_id,
        "failed_sources": [res.source for res in failed_steps],
        "tables": [res.table for res in results if res.status == "SUCCESS" and res.table],
        **(extra or {}),
    }

    if failed_critical and fail_on_critical:
        failure_details = [res.to_dict() for res in results if res.source in failed_critical]
        raise RuntimeError(f"Source(s) critique(s) en échec ou absente(s) : {failed_critical} ; détails : {failure_details}")
    return payload


def render_execution_report(
    payload: dict,
    columns: list[str] | None = None,
    summary_line: str = "",
    logger_name: str = "report",
) -> None:
    """Affiche un tableau synthétique d'exécution via display() sous Fabric ou stdout localement."""
    results_list = payload.get("results") or []
    if results_list:
        import pandas as pd

        df_report = pd.DataFrame(results_list)
        if columns:
            existing_cols = [col for col in columns if col in df_report.columns]
            if existing_cols:
                df_report = df_report[existing_cols]
        try:
            display(df_report)  # noqa: F821 - fonction native injectée par le runtime Fabric
        except NameError:
            get_logger(logger_name).info("\n%s", df_report.to_string(index=False))
    report_logger = get_logger(logger_name)
    for failed_step in [res for res in results_list if res.get("status") == "FAILED"]:
        report_logger.error("Étape en échec : %s -> %s", failed_step.get("source"), failed_step.get("error"))
    if summary_line:
        level = logging.WARNING if payload.get("status") == "PARTIAL" else logging.INFO
        report_logger.log(level, summary_line)


def exit_notebook_json(payload: dict) -> str:
    """Retourne le résumé JSON valide au pipeline Microsoft Fabric via notebookutils."""
    json_text = json.dumps(payload, ensure_ascii=False, default=str)
    nb_utils = globals().get("notebookutils")
    if nb_utils is None:
        try:  # pragma: no cover - disponible nativement dans Fabric
            import notebookutils as nb_utils  # type: ignore
        except ImportError:
            nb_utils = None
    if nb_utils is not None:
        nb_utils.notebook.exit(json_text)
    return json_text


# Message de chargement de la configuration (anciennement un print dans nb_util_config)
if "PROJECT_NAME" in globals():
    get_logger("config").info(
        "✅ Configuration chargée : %s v%s (%d sources BDPM, %d tables Gold)",
        PROJECT_NAME, PROJECT_VERSION, len(BDPM_SOURCES), len(GOLD_TABLES),
    )


# MARKDOWN ********************

# ---
# 
# ## 🛠️ Exploitation et dépannage
# 
# - **Rejouer une date** : renseigner `run_date` (AAAA-MM-JJ) ; toutes les étapes sont idempotentes.
# - **Diagnostiquer** : la sortie JSON de l'activité contient `status`, les étapes en échec et leurs messages ; les contrôles qualité sont historisés dans `_dq_audit_log`.
# - **Verrou** : en cas d'interruption brutale, `_ops_run_lock` expire automatiquement après `ttl_minutes` ; le pipeline libère toujours le verrou en fin de run.
# 
# *HealthTek — nb_util_logging · documentation générée selon le standard du projet.*
