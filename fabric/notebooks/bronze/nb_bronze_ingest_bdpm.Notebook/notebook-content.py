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

# # 🌐 [BRONZE - INGESTION] `nb_bronze_ingest_bdpm` — Téléchargement & Dépôt Atomique BDPM
# 
# > **HealthTek** · Plateforme analytique des ruptures et tensions d'approvisionnement de médicaments (données publiques BDPM / ANSM / HAS) — Microsoft Fabric
# 
# | Élément | Détail |
# |---|---|
# | **Couche** | Bronze — ingestion |
# | **Type** | Notebook exécutable |
# | **Orchestration** | Activité `run_ingest_bdpm` (2 reprises) — pipeline `pl_master_health_pipeline` |
# | **Entrées** | Portail BDPM (HTTPS) |
# | **Sorties** | `Files/landing/bdpm/<source>/<fichier>_<date>.txt` + `.manifest.json` |
# | **Idempotence** | Oui (écriture atomique `.part` → `os.replace`, un fichier par date) |
# | **Documentation** | v3.1 — 10/11/2026 |
# 
# ## 🎯 Objectif
# 
# Télécharge les fichiers officiels de la Base de Données Publique des Médicaments (BDPM) selon leur cadence (`all`, `auto`, `daily`, `biweekly`, `monthly`), valide l'intégrité HTTP/ZIP/HTML/colonnes, normalise en UTF-8 et dépose chaque fichier daté avec un manifeste `.manifest.json` atomique dans OneLake.
# 
# ## 🔄 Flux de données
# 
# ```
# Portail BDPM (HTTP) ──► Reprises exponentielles + Validation UTF-8/Colonnes ──► Files/landing/bdpm/*.txt + .manifest.json
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
# | `source_filter` | `"auto"` | `all`, `auto` (sources échues selon leur cadence), `daily`, `biweekly`, `monthly`. |
# | `fail_fast` | `"true"` | Lève une erreur si une source critique (spécialités, présentations, ruptures) échoue. |
# 
# ## 🧭 Plan du notebook
# 
# 1. Chargement des utilitaires
# 2. Paramètres
# 3. Téléchargement résilient, validation et dépôt atomique
# 4. Exécution
# 5. Rapport et sortie JSON
# 
# ## 📤 Contrat de sortie
# 
# `{"status","total","succeeded","failed","selected","results":[...]}`

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

# Filtre des sources à télécharger : 'all', 'auto', 'daily', 'biweekly', 'monthly'
source_filter = "auto"

# Arrêt immédiat si une source critique échoue ('true' / 'false')
fail_fast = "true"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3 · Téléchargement résilient, validation et dépôt atomique

# CELL ********************

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import time
import urllib.error
import urllib.request
import zipfile
from collections import Counter
from dataclasses import dataclass
from datetime import date
from typing import Callable, Optional

ingest_logger = get_logger("ingest")
USER_AGENT = f"{PROJECT_NAME}-Pipeline/{PROJECT_VERSION} (+data-engineering)"


class DownloadError(Exception):
    """Erreur irrécupérable lors du téléchargement d'une source BDPM."""


@dataclass
class DownloadedPayload:
    """Contenu brut téléchargé et métadonnées réseau associées."""

    content: bytes
    url: str
    attempts: int
    sha256: str


def fetch_http_bytes(url: str, timeout_seconds: float) -> bytes:
    """Effectue une requête HTTP GET et vérifie la complétude via Content-Length."""
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/plain, text/csv, application/zip, application/octet-stream",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310 - URL issues du registre BDPM_SOURCES
        http_status = getattr(response, "status", 200)
        if http_status != 200:
            raise urllib.error.HTTPError(url, http_status, f"HTTP {http_status}", response.headers, None)
        body_bytes = response.read()
        announced_length = response.headers.get("Content-Length")
        if announced_length and announced_length.isdigit() and len(body_bytes) != int(announced_length):
            raise OSError(
                f"Réponse tronquée : {len(body_bytes)} octets reçus sur {announced_length} annoncés"
            )
        return body_bytes


def download_with_retry(
    url: str,
    min_bytes: int = 100,
    retries: int = DEFAULT_HTTP_CONFIG["retries"],
    backoff_seconds: float = DEFAULT_HTTP_CONFIG["backoff_seconds"],
    timeout_seconds: float = DEFAULT_HTTP_CONFIG["timeout_seconds"],
    fetch_fn: Callable[[str, float], bytes] = fetch_http_bytes,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> DownloadedPayload:
    """Télécharge un fichier avec backoff exponentiel sur erreurs transitoires (hors 4xx permanents)."""
    last_error = "inconnu"
    for attempt in range(1, retries + 1):
        try:
            body_bytes = fetch_fn(url, timeout_seconds)
            if len(body_bytes) < min_bytes:
                last_error = f"trop petit : {len(body_bytes)} octets (minimum {min_bytes})"
            else:
                return DownloadedPayload(
                    content=body_bytes,
                    url=url,
                    attempts=attempt,
                    sha256=hashlib.sha256(body_bytes).hexdigest(),
                )
        except urllib.error.HTTPError as exc:
            last_error = f"HTTP {exc.code}"
            if 400 <= exc.code < 500 and exc.code not in (408, 429):
                raise DownloadError(f"{url} : {last_error} (non rejoué)") from exc
        except (urllib.error.URLError, OSError, TimeoutError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"

        ingest_logger.warning("Téléchargement %s : tentative %d/%d échouée (%s)", url, attempt, retries, last_error)
        if attempt < retries:
            sleep_fn(backoff_seconds ** attempt)

    raise DownloadError(f"{url} : échec après {retries} tentatives ({last_error})")


def decode_bdpm_text(raw_bytes: bytes) -> tuple[str, str]:
    """Décode un fichier brut selon l'ordre UTF-8 (avec BOM), Windows-1252, puis Latin-1."""
    for encoding in ("utf-8-sig", "cp1252"):
        try:
            detected = "utf-8" if encoding == "utf-8-sig" else encoding
            return raw_bytes.decode(encoding), detected
        except UnicodeDecodeError:
            continue
    return raw_bytes.decode("latin-1"), "latin-1"


def sniff_csv_delimiter(text: str, candidates: str = ";\t,") -> str:
    """Détecte le délimiteur dominant sur la première ligne d'un fichier avec en-tête."""
    header_line = text.split("\n", 1)[0]
    counts = {candidate: header_line.count(candidate) for candidate in candidates}
    best_delimiter = max(counts, key=counts.get)
    if counts[best_delimiter] == 0:
        raise ValueError("Aucun délimiteur reconnu en première ligne")
    return best_delimiter


def extract_zip_member(raw_bytes: bytes, pattern: str = r"\.(csv|txt)$") -> tuple[bytes, str]:
    """Extrait le fichier CSV/TXT le plus volumineux contenu dans une archive ZIP."""
    if not zipfile.is_zipfile(io.BytesIO(raw_bytes)):
        raise ValueError("Contenu non ZIP")
    with zipfile.ZipFile(io.BytesIO(raw_bytes)) as archive:
        matching_members = [
            member for member in archive.infolist()
            if re.search(pattern, member.filename, re.I) and not member.is_dir()
        ]
        if not matching_members:
            archived_names = [member.filename for member in archive.infolist()]
            raise ValueError(f"Aucun fichier correspondant à {pattern} dans l'archive : {archived_names}")
        largest_member = max(matching_members, key=lambda member: member.file_size)
        return archive.read(largest_member), largest_member.filename


def is_html_payload(text: str) -> bool:
    """Détecte une page d'erreur HTML renvoyée à la place d'un fichier de données."""
    prefix = text[:500].lstrip().lower()
    return prefix.startswith("<!doctype html") or prefix.startswith("<html")


def count_column_distribution(text: str, delimiter: str, sample_lines: int = 200) -> dict[str, int]:
    """Calcule min, max et mode du nombre de colonnes sur un échantillon de lignes."""
    non_empty_lines = [line for line in text.splitlines()[:sample_lines] if line.strip()]
    distribution = Counter(len(line.split(delimiter)) for line in non_empty_lines)
    if not distribution:
        return {"min": 0, "max": 0, "mode": 0}
    return {
        "min": min(distribution),
        "max": max(distribution),
        "mode": distribution.most_common(1)[0][0],
    }


def atomic_write_bytes(destination_path: str, payload_bytes: bytes) -> None:
    """Écrit atomiquement via un fichier .part temporaire suivi d'un os.replace."""
    parent_dir = os.path.dirname(destination_path)
    if parent_dir:
        os.makedirs(parent_dir, exist_ok=True)
    temp_path = f"{destination_path}.part"
    with open(temp_path, "wb") as file_handle:
        file_handle.write(payload_bytes)
        file_handle.flush()
        os.fsync(file_handle.fileno())
    os.replace(temp_path, destination_path)


def deposit_landing_file(
    landing_root: str,
    subfolder: str,
    file_name: str,
    raw_bytes: bytes,
    target_date: date,
    *,
    min_bytes: int = 100,
    expected_columns: int | None = None,
    delimiter: str = "\t",
    has_header: bool = False,
    source_key: str = "",
) -> dict:
    """Valide, normalise en UTF-8/LF et dépose le fichier daté avec son manifeste JSON."""
    zip_member_name = None
    if zipfile.is_zipfile(io.BytesIO(raw_bytes)):
        raw_bytes, zip_member_name = extract_zip_member(raw_bytes)

    if len(raw_bytes) < min_bytes:
        raise ValueError(f"{file_name} : {len(raw_bytes)} octets < minimum {min_bytes}")

    decoded_text, detected_encoding = decode_bdpm_text(raw_bytes)
    if is_html_payload(decoded_text):
        raise ValueError(f"{file_name} : contenu HTML (page d'erreur ?) au lieu de données")

    normalized_text = decoded_text.replace("\r\n", "\n").replace("\r", "\n")
    cleaned_lines = [line for line in normalized_text.split("\n") if line.strip(" \t") != ""]
    if not cleaned_lines:
        raise ValueError(f"{file_name} : fichier vide après suppression des lignes blanches")
    normalized_text = "\n".join(cleaned_lines) + "\n"

    resolved_delimiter = delimiter if not has_header else sniff_csv_delimiter(normalized_text, ";\t,")
    col_stats = count_column_distribution(normalized_text, resolved_delimiter)
    if expected_columns is not None and col_stats["mode"] != expected_columns and not has_header:
        raise ValueError(
            f"{file_name} : {col_stats['mode']} colonnes détectées, {expected_columns} attendues (dérive de schéma ?)"
        )

    utf8_bytes = normalized_text.encode("utf-8")
    destination_path = os.path.join(landing_root, subfolder, landing_filename(file_name, target_date))
    atomic_write_bytes(destination_path, utf8_bytes)

    manifest = {
        "source": source_key,
        "file": os.path.basename(destination_path),
        "bytes": len(utf8_bytes),
        "lines": normalized_text.count("\n"),
        "sha256": hashlib.sha256(utf8_bytes).hexdigest(),
        "encoding_detected": detected_encoding,
        "replacement_chars": normalized_text.count("\ufffd"),
        "delimiter": "\\t" if resolved_delimiter == "\t" else resolved_delimiter,
        "columns": col_stats,
        "run_date": target_date.isoformat(),
        "zip_member": zip_member_name,
    }
    atomic_write_bytes(destination_path + ".manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"))
    return {**manifest, "path": destination_path}


def ingest_bdpm_source(
    url: str,
    landing_root: str,
    subfolder: str,
    file_name: str,
    target_date: date,
    *,
    retries: int = DEFAULT_HTTP_CONFIG["retries"],
    backoff_seconds: float = DEFAULT_HTTP_CONFIG["backoff_seconds"],
    timeout_seconds: float = DEFAULT_HTTP_CONFIG["timeout_seconds"],
    fetch_fn: Callable[[str, float], bytes] = fetch_http_bytes,
    sleep_fn: Callable[[float], None] = time.sleep,
    **deposit_kwargs,
) -> dict:
    """Télécharge puis dépose une source et enrichit le manifeste de la télémétrie réseau."""
    downloaded = download_with_retry(
        url=url,
        min_bytes=deposit_kwargs.get("min_bytes", 100),
        retries=retries,
        backoff_seconds=backoff_seconds,
        timeout_seconds=timeout_seconds,
        fetch_fn=fetch_fn,
        sleep_fn=sleep_fn,
    )
    manifest = deposit_landing_file(landing_root, subfolder, file_name, downloaded.content, target_date, **deposit_kwargs)
    manifest["attempts"] = downloaded.attempts
    manifest["url"] = url
    return manifest


def find_last_landing_dates(landing_root: str) -> dict[str, date]:
    """Retourne la date du dernier dépôt présent dans landing pour chaque source."""
    last_dates: dict[str, date] = {}
    for source_key, source_spec in BDPM_SOURCES.items():
        existing_files = list_landing_files(landing_root, source_spec.subfolder)
        if existing_files:
            last_dates[source_key] = existing_files[-1][0]
    return last_dates


def execute_ingestion_pipeline(
    landing_root: str,
    filter_mode: str,
    target_date: date,
    current_run_id: str = "manual",
    fail_fast_enabled: bool = True,
    base_url: str = BDPM_BASE_URL,
    bytes_scale: float = 1.0,
    fetch_fn: Callable[[str, float], bytes] = fetch_http_bytes,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> dict:
    """Orchestre le téléchargement et le dépôt de toutes les sources BDPM éligibles."""
    start_time = time.time()
    last_dates = find_last_landing_dates(landing_root)
    selected_sources = sources_for_filter(filter_mode, last_dates, target_date)
    step_results: list[StepResult] = []

    for source_key in selected_sources:
        source_spec = BDPM_SOURCES[source_key]
        try:
            target_url = source_url(source_spec, base_url)
            min_size_bytes = int(source_spec.min_bytes * bytes_scale)
            manifest = ingest_bdpm_source(
                url=target_url,
                landing_root=landing_root,
                subfolder=source_spec.subfolder,
                file_name=source_spec.file_name,
                target_date=target_date,
                min_bytes=min_size_bytes,
                expected_columns=len(source_spec.columns),
                source_key=source_key,
                fetch_fn=fetch_fn,
                sleep_fn=sleep_fn,
            )
            step_results.append(
                StepResult(
                    source=source_key,
                    table=source_spec.table,
                    status="SUCCESS",
                    error=None,
                    rows=manifest["lines"],
                    columns=manifest["columns"]["mode"],
                    details={
                        "file": manifest["file"],
                        "bytes": manifest["bytes"],
                        "encoding": manifest["encoding_detected"],
                        "replacement_chars": manifest["replacement_chars"],
                        "attempts": manifest["attempts"],
                    },
                )
            )
        except Exception as exc:  # noqa: BLE001
            ingest_logger.error("Échec d'ingestion pour %s : %s", source_key, exc)
            step_status = "SKIPPED" if source_spec.optional else "FAILED"
            step_results.append(StepResult(source_key, source_spec.table, step_status, f"{type(exc).__name__}: {exc}"))

    return summarize_results(
        step_results,
        CRITICAL_SOURCES,
        current_run_id,
        time.time() - start_time,
        extra={"source_filter": filter_mode, "selected": selected_sources, "results": [res.to_dict() for res in step_results]},
        fail_on_critical=fail_fast_enabled,
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

landing_root = resolve_landing_root()
reference_date = parse_run_date(run_date)
fail_fast_flag = parse_bool(fail_fast, "fail_fast")
init_notebook_banner("nb_bronze_ingest_bdpm", run_id, reference_date, resolve_table_format())

payload = execute_ingestion_pipeline(
    landing_root=landing_root,
    filter_mode=source_filter,
    target_date=reference_date,
    current_run_id=run_id,
    fail_fast_enabled=fail_fast_flag,
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
    columns=["source", "status", "rows", "error"],
    summary_line=(
        f"🎯 Statut Ingestion : {payload.get('status')} | "
        f"{payload.get('succeeded', 0)}/{payload.get('total', 0)} sources traitées | "
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
# *HealthTek — nb_bronze_ingest_bdpm · documentation générée selon le standard du projet.*
