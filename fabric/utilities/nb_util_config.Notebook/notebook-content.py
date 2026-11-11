# Fabric notebook source


# MARKDOWN ********************

# # ⚙️ [UTILITY] `nb_util_config` — Configuration Centrale & Contrats BDPM
# 
# > **HealthTek** · Plateforme analytique des ruptures et tensions d'approvisionnement de médicaments (données publiques BDPM / ANSM / HAS) — Microsoft Fabric
# 
# | Élément | Détail |
# |---|---|
# | **Couche** | Socle — configuration |
# | **Type** | Bibliothèque sans état (`%run`) |
# | **Orchestration** | Inclus par tous les notebooks — pipeline `pl_master_health_pipeline` |
# | **Entrées** | — |
# | **Sorties** | Constantes, contrats `SourceSpec`, registres de tables |
# | **Idempotence** | Oui (aucune écriture) |
# | **Documentation** | v3.1 — 10/11/2026 |
# 
# ## 🎯 Objectif
# 
# Source unique de vérité (**Single Source of Truth**) pour l'ensemble du projet HealthTek :
# - Identifiants des artefacts Microsoft Fabric (`lh_health_analytics`, `wh_health_analytics`, `sem_health_analytics`).
# - Contrats des **11 sources officielles de la BDPM** (noms de colonnes positionnels, cadences, tailles minimales, criticité).
# - Registre unifié des tables **Bronze**, **Silver**, **Gold** et techniques (`_ops_run_lock`, `_dq_audit_log`, `_dq_quarantine`).
# - Référentiels métier ANSM / HAS / ATC et constantes techniques nommées (suppression des *magic numbers*).
# 
# ## 🔗 Utilisation
# 
# Inclure ce notebook sans état dans un notebook appelant via une cellule dédiée :
# ```python
# %run nb_util_config
# ```
# 
# ## 🧭 Plan du notebook
# 
# 1. Artefacts Fabric & constantes nommées
# 2. Contrat des 11 sources BDPM (`SourceSpec`) et sélection par cadence
# 3. Registres Bronze / Silver / Gold et référentiels métier (ANSM, HAS, ATC)
# 
# ## 📤 Contrat de sortie
# 
# Aucune sortie : expose des variables globales au notebook appelant.

# MARKDOWN ********************

# ## 1 · Imports

# CELL ********************

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

# MARKDOWN ********************

# ## 2 · Artefacts Microsoft Fabric et constantes globales

# CELL ********************

from __future__ import annotations

# ==============================================================================
# 1. ARTEFACTS MICROSOFT FABRIC & CONSTANTES GLOBALES
# ==============================================================================
PROJECT_NAME = "HealthTek"
PROJECT_VERSION = "3.0.0"
SCHEMA_VERSION = "3.0.0"
WORKSPACE_NAME = "FR-Health-Analytics-Dev"
DEFAULT_LAKEHOUSE = "lh_health_analytics"
DEFAULT_WAREHOUSE = "wh_health_analytics"
DEFAULT_SEMANTIC_MODEL = "sem_health_analytics"
DEFAULT_LANDING_ROOT = "/lakehouse/default/Files/landing"

# Tables techniques d'exploitation et de gouvernance
LOCK_TABLE = "_ops_run_lock"
AUDIT_TABLE = "_dq_audit_log"
QUARANTINE_TABLE = "_dq_quarantine"

# Constantes métier et techniques nommées (aucun magic number dans le code métier)
UNKNOWN_SURROGATE_KEY = -1
UNKNOWN_CIS_CODE = "00000000"
UNKNOWN_CIP13_CODE = "0000000000000"
NON_BREAKING_SPACE = "\u00a0"
UNICODE_REPLACEMENT_CHAR = "\ufffd"
PRICE_FEE_COHERENCE_TOLERANCE = 0.011

CALENDAR_START_DATE = "2000-01-01"
CALENDAR_END_DATE = "2032-12-31"
DAYS_FR = ["lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi", "dimanche"]
MONTHS_FR = [
    "janvier", "février", "mars", "avril", "mai", "juin",
    "juillet", "août", "septembre", "octobre", "novembre", "décembre",
]

# MARKDOWN ********************

# ## 3 · Contrat des 11 sources BDPM (TSV sans en-tête)

# CELL ********************

# ==============================================================================
# 2. CONTRAT DES 11 SOURCES BDPM (TSV SANS EN-TÊTE)
# ==============================================================================
BDPM_BASE_URL = "https://base-donnees-publique.medicaments.gouv.fr/"
VALID_SOURCE_FILTERS = ("all", "auto", "daily", "biweekly", "monthly")
CADENCES = ("daily", "biweekly", "monthly", "annual")

BDPM_COLUMNS: dict[str, list[str]] = {
    "specialties": [
        "cis_code", "denomination", "forme_pharmaceutique", "voie_administration",
        "statut_amm", "type_procedure_amm", "etat_commercialisation", "date_amm",
        "statut_bdm", "numero_autorisation_europeenne", "titulaire", "surveillance_renforcee",
    ],
    "presentations": [
        "cis_code", "cip7", "libelle_presentation", "statut_administratif",
        "etat_commercialisation", "date_declaration_commercialisation", "cip13",
        "agrement_collectivites", "taux_remboursement", "prix_hors_honoraires",
        "prix_avec_honoraires", "honoraires_dispensation", "indications_remboursement",
    ],
    "compositions": [
        "cis_code", "designation_element", "code_substance", "denomination_substance",
        "dosage_substance", "reference_dosage", "nature_composant", "numero_liaison_sa_ft",
    ],
    "smr": [
        "cis_code", "code_dossier_has", "motif_evaluation",
        "date_avis", "valeur_smr", "libelle_smr",
    ],
    "asmr": [
        "cis_code", "code_dossier_has", "motif_evaluation",
        "date_avis", "valeur_asmr", "libelle_asmr",
    ],
    "ct_links": ["code_dossier_has", "lien_page_ct"],
    "generics": [
        "generic_group_id", "libelle_groupe", "cis_code", "type_generique", "numero_tri",
    ],
    "conditions": ["cis_code", "condition_prescription"],
    "ruptures": [
        "cis_code", "cip13", "code_statut", "libelle_statut",
        "date_debut", "date_mise_a_jour", "date_remise", "lien_ansm",
    ],
    "mitm": [
        "cis_code", "code_atc", "denomination", "titulaire",
        "exploitant", "date_mise_a_jour", "lien_fiche",
    ],
    "important_info": ["cis_code", "date_debut", "date_fin", "html_info"],
}


@dataclass(frozen=True)
class SourceSpec:
    """Description contractuelle d'une source officielle BDPM."""

    key: str
    file_name: str
    subfolder: str
    table: str
    cadence: str
    cadence_days: int
    min_bytes: int
    critical: bool
    columns: tuple[str, ...]
    role: str = ""
    url_path: str = "download/file/"
    optional: bool = False


BDPM_SOURCES: dict[str, SourceSpec] = {
    spec.key: spec
    for spec in [
        SourceSpec(
            key="specialties",
            file_name="CIS_bdpm.txt",
            subfolder="bdpm/specialties",
            table="bronze_bdpm_specialties",
            cadence="monthly",
            cadence_days=30,
            min_bytes=500_000,
            critical=True,
            columns=tuple(BDPM_COLUMNS["specialties"]),
            role="dimension médicament",
        ),
        SourceSpec(
            key="presentations",
            file_name="CIS_CIP_bdpm.txt",
            subfolder="bdpm/presentations",
            table="bronze_bdpm_cip",
            cadence="daily",
            cadence_days=1,
            min_bytes=500_000,
            critical=True,
            columns=tuple(BDPM_COLUMNS["presentations"]),
            role="prix et remboursement par boîte (CIP13)",
        ),
        SourceSpec(
            key="compositions",
            file_name="CIS_COMPO_bdpm.txt",
            subfolder="bdpm/compositions",
            table="bronze_bdpm_compositions",
            cadence="monthly",
            cadence_days=30,
            min_bytes=400_000,
            critical=False,
            columns=tuple(BDPM_COLUMNS["compositions"]),
            role="substances actives",
        ),
        SourceSpec(
            key="smr",
            file_name="CIS_HAS_SMR_bdpm.txt",
            subfolder="bdpm/smr",
            table="bronze_bdpm_smr",
            cadence="monthly",
            cadence_days=30,
            min_bytes=500_000,
            critical=False,
            columns=tuple(BDPM_COLUMNS["smr"]),
            role="service médical rendu (HAS)",
        ),
        SourceSpec(
            key="asmr",
            file_name="CIS_HAS_ASMR_bdpm.txt",
            subfolder="bdpm/asmr",
            table="bronze_bdpm_asmr",
            cadence="monthly",
            cadence_days=30,
            min_bytes=500_000,
            critical=False,
            columns=tuple(BDPM_COLUMNS["asmr"]),
            role="amélioration du service médical rendu (HAS)",
        ),
        SourceSpec(
            key="ct_links",
            file_name="HAS_LiensPageCT_bdpm.txt",
            subfolder="bdpm/ct_links",
            table="bronze_bdpm_ct_links",
            cadence="monthly",
            cadence_days=30,
            min_bytes=50_000,
            critical=False,
            columns=tuple(BDPM_COLUMNS["ct_links"]),
            role="liens vers les avis de la commission de la transparence",
            optional=True,
        ),
        SourceSpec(
            key="generics",
            file_name="CIS_GENER_bdpm.txt",
            subfolder="bdpm/generics",
            table="bronze_bdpm_generics",
            cadence="monthly",
            cadence_days=30,
            min_bytes=100_000,
            critical=False,
            columns=tuple(BDPM_COLUMNS["generics"]),
            role="groupes génériques",
        ),
        SourceSpec(
            key="conditions",
            file_name="CIS_CPD_bdpm.txt",
            subfolder="bdpm/conditions",
            table="bronze_bdpm_conditions",
            cadence="monthly",
            cadence_days=30,
            min_bytes=100_000,
            critical=False,
            columns=tuple(BDPM_COLUMNS["conditions"]),
            role="conditions de prescription et de délivrance",
        ),
        SourceSpec(
            key="ruptures",
            file_name="CIS_CIP_Dispo_Spec.txt",
            subfolder="bdpm/ruptures",
            table="bronze_bdpm_ruptures",
            cadence="biweekly",
            cadence_days=14,
            min_bytes=10_000,
            critical=True,
            columns=tuple(BDPM_COLUMNS["ruptures"]),
            role="ruptures et tensions d'approvisionnement",
        ),
        SourceSpec(
            key="mitm",
            file_name="CIS_MITM.txt",
            subfolder="bdpm/mitm",
            table="bronze_bdpm_mitm",
            cadence="monthly",
            cadence_days=30,
            min_bytes=100_000,
            critical=False,
            columns=tuple(BDPM_COLUMNS["mitm"]),
            role="médicaments d'intérêt thérapeutique majeur",
        ),
        SourceSpec(
            key="important_info",
            file_name="CIS_InfoImportantes.txt",
            subfolder="bdpm/important_info",
            table="bronze_bdpm_important_info",
            cadence="daily",
            cadence_days=1,
            min_bytes=100_000,
            critical=False,
            columns=tuple(BDPM_COLUMNS["important_info"]),
            role="informations importantes de sécurité (ANSM)",
            url_path="download/",
        ),
    ]
}

CRITICAL_SOURCES = {source_key for source_key, spec in BDPM_SOURCES.items() if spec.critical}


def source_url(spec: SourceSpec, base_url: str = BDPM_BASE_URL) -> str:
    """Construit l'URL officielle complète de téléchargement pour une source."""
    return base_url.rstrip("/") + "/" + spec.url_path + spec.file_name


def sources_for_filter(
    source_filter: str,
    last_success_dates: dict[str, date] | None = None,
    reference_date: date | None = None,
) -> list[str]:
    """Sélectionne les sources BDPM à traiter selon le filtre demandé.

    - ``all`` : toutes les sources ;
    - ``daily`` / ``biweekly`` / ``monthly`` : filtrage exact par cadence (sensible à la casse) ;
    - ``auto`` : sélectionne uniquement les sources jamais chargées ou dont l'intervalle est échu.
    """
    if source_filter not in VALID_SOURCE_FILTERS:
        raise ValueError(f"source_filter invalide {source_filter!r} ; valeurs permises : {VALID_SOURCE_FILTERS}")
    if source_filter == "all":
        return list(BDPM_SOURCES)
    if source_filter == "auto":
        today_date = reference_date or date.today()
        last_dates = last_success_dates or {}
        due_sources = []
        for source_key, spec in BDPM_SOURCES.items():
            last_date = last_dates.get(source_key)
            if last_date is None or (today_date - last_date).days >= spec.cadence_days:
                due_sources.append(source_key)
        return due_sources
    return [source_key for source_key, spec in BDPM_SOURCES.items() if spec.cadence == source_filter]

# MARKDOWN ********************

# ## 4 · Registres des tables et référentiels métier

# CELL ********************

# ==============================================================================
# 3. REGISTRES DES TABLES & RÉFÉRENTIELS MÉTIER
# ==============================================================================
BRONZE_TABLES = {source_key: spec.table for source_key, spec in BDPM_SOURCES.items()}

SILVER_TABLES = {
    "specialties": "silver_bdpm_specialties",
    "presentations": "silver_bdpm_cip",
    "ruptures": "silver_bdpm_ruptures",
    "compositions": "silver_bdpm_compositions",
    "generics": "silver_bdpm_generics",
    "conditions": "silver_bdpm_conditions",
    "smr": "silver_bdpm_smr",
    "asmr": "silver_bdpm_asmr",
    "mitm": "silver_bdpm_mitm",
    "important_info": "silver_bdpm_important_info",
    "ct_links": "silver_bdpm_ct_links",
}
SILVER_PRICE_HISTORY_TABLE = "silver_bdpm_cip_history"

GOLD_TABLES = {
    "dim_drug": "gold_dim_drug",
    "dim_presentation": "gold_dim_presentation",
    "dim_generic_group": "gold_dim_generic_group",
    "dim_substance": "gold_dim_substance",
    "dim_atc": "gold_dim_atc",
    "dim_status": "gold_dim_status",
    "dim_opinion_level": "gold_dim_opinion_level",
    "dim_time": "gold_dim_time",
    "fact_presentation_price": "gold_fact_presentation_price",
    "fact_shortage": "gold_fact_shortage",
    "fact_has_opinion": "gold_fact_has_opinion",
    "fact_important_info": "gold_fact_important_info",
    "fact_composition": "gold_fact_composition",
    "kpi_shortage_risk_score": "gold_kpi_shortage_risk_score",
    "kpi_generic_price_gap": "gold_kpi_generic_price_gap",
    "kpi_price_movement": "gold_kpi_price_movement",
    "kpi_shortage_monthly": "gold_kpi_shortage_monthly",
}

GOLD_ZORDER_COLUMNS = {
    "gold_fact_presentation_price": ["drug_sk", "snapshot_date"],
    "gold_fact_shortage": ["drug_sk", "date_debut"],
    "gold_fact_has_opinion": ["drug_sk", "date_avis"],
    "gold_fact_composition": ["drug_sk"],
}

GOLD_TABLE_DESCRIPTIONS = {
    "gold_dim_drug": "Dimension médicament - une ligne par code CIS (+ membres inférés + membre inconnu -1)",
    "gold_dim_presentation": "Dimension présentation - une ligne par CIP13",
    "gold_dim_generic_group": "Dimension groupe générique - une ligne par groupe",
    "gold_dim_substance": "Dimension substance - une ligne par code substance",
    "gold_dim_atc": "Dimension classe ATC (médicaments MITM uniquement)",
    "gold_dim_status": "Dimension statut de disponibilité ANSM (rupture, tension, arrêt, remise)",
    "gold_dim_opinion_level": "Dimension niveau d'avis HAS (SMR et ASMR) avec rang ordinal",
    "gold_dim_time": "Dimension calendrier - une ligne par jour",
    "gold_fact_presentation_price": "Prix et remboursement par présentation et par jour de photo",
    "gold_fact_shortage": "Ruptures et tensions d'approvisionnement (ANSM)",
    "gold_fact_has_opinion": "Avis SMR et ASMR de la HAS",
    "gold_fact_important_info": "Informations importantes de sécurité (ANSM)",
    "gold_fact_composition": "Composition : une substance dans un médicament",
    "gold_kpi_shortage_risk_score": "Score de risque de rupture par médicament",
    "gold_kpi_generic_price_gap": "Écart de prix princeps / générique par groupe",
    "gold_kpi_price_movement": "Variation de prix entre les deux dernières photos",
    "gold_kpi_shortage_monthly": "Signalements de rupture débutés par mois et par statut",
}

STATUT_LABELS = {
    1: "Rupture de stock",
    2: "Tension d'approvisionnement",
    3: "Arrêt de commercialisation",
    4: "Remise à disposition",
}
ACTIVE_STATUS_CODES = (1, 2)
STATUT_SEVERITY = {1: 3, 2: 2, 3: 1, 4: 0}

GENERIC_LABELS = {
    0: "Princeps",
    1: "Générique",
    2: "Générique complémentarité",
    3: "Générique substituable (code 3)",
    4: "Générique substituable",
}

ATC_LEVEL1 = {
    "A": "Voies digestives et métabolisme",
    "B": "Sang et organes hématopoïétiques",
    "C": "Système cardiovasculaire",
    "D": "Dermatologie",
    "G": "Système génito-urinaire et hormones sexuelles",
    "H": "Hormones systémiques (hors hormones sexuelles)",
    "J": "Anti-infectieux généraux à usage systémique",
    "L": "Antinéoplasiques et immunomodulateurs",
    "M": "Système musculo-squelettique",
    "N": "Système nerveux",
    "P": "Antiparasitaires, insecticides et répulsifs",
    "R": "Système respiratoire",
    "S": "Organes sensoriels",
    "V": "Divers",
}

SMR_LEVELS = {"IMPORTANT": 4, "MODERE": 3, "FAIBLE": 2, "INSUFFISANT": 1}

DEFAULT_HTTP_CONFIG = {
    "retries": 3,
    "backoff_seconds": 2.0,
    "timeout_seconds": 60.0,
}

DEFAULT_RISK_WEIGHTS = {
    "per_shortage": 20,
    "per_day": 0.5,
    "days_cap": 365,
    "per_active": 50,
    "recent_180": 30,
    "recent_365": 15,
    "mitm": 40,
    "no_alternative": 25,
}
DEFAULT_RISK_THRESHOLDS = {"Critique": 300, "Élevé": 200, "Modéré": 100}
LONG_SHORTAGE_DAYS = 1825
SILVER_LOSS_WARN_PCT = 5.0

# Le message « Configuration chargée » est journalisé par nb_util_logging (module logging).


# MARKDOWN ********************

# ---
# 
# ## 🛠️ Exploitation et dépannage
# 
# - **Rejouer une date** : renseigner `run_date` (AAAA-MM-JJ) ; toutes les étapes sont idempotentes.
# - **Diagnostiquer** : la sortie JSON de l'activité contient `status`, les étapes en échec et leurs messages ; les contrôles qualité sont historisés dans `_dq_audit_log`.
# - **Verrou** : en cas d'interruption brutale, `_ops_run_lock` expire automatiquement après `ttl_minutes` ; le pipeline libère toujours le verrou en fin de run.
# 
# *HealthTek — nb_util_config · documentation générée selon le standard du projet.*
