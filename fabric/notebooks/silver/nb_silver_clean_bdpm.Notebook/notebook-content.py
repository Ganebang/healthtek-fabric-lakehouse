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

# # 🥈 [SILVER] `nb_silver_clean_bdpm` — Nettoyage, Typage & Dédoublonnage BDPM
# 
# > **HealthTek** · Plateforme analytique des ruptures et tensions d'approvisionnement de médicaments (données publiques BDPM / ANSM / HAS) — Microsoft Fabric
# 
# | Élément | Détail |
# |---|---|
# | **Couche** | Silver |
# | **Type** | Notebook exécutable |
# | **Orchestration** | Activité `run_silver` — pipeline `pl_master_health_pipeline` |
# | **Entrées** | `bronze_bdpm_*` |
# | **Sorties** | `silver_bdpm_*`, `silver_bdpm_cip_history`, `_dq_quarantine` |
# | **Idempotence** | Oui (overwrite + replaceWhere par instantané) |
# | **Documentation** | v3.1 — 10/11/2026 |
# 
# ## 🎯 Objectif
# 
# Transforme de façon déterministe chacune des 11 tables Bronze (tout texte) en tables Silver typées et dédoublonnées, sans aucune suppression silencieuse :
# - Toute ligne invalide ou doublon écarté est conservé avec son `_reject_reason` et routé vers `_dq_quarantine`.
# - Construit et met à jour l'historique cumulatif idempotent des prix (`silver_bdpm_cip_history`) via `replace_where`.
# - Réutilise les constantes nommées et utilitaires transverses de `nb_util_config`, `nb_util_logging`, `nb_util_io` et `nb_util_validation`.
# 
# ## 🔄 Flux de données
# 
# ```
# bronze_bdpm_* (11 tables) ──► Nettoyage / Typage / Dédoublonnage ──► silver_bdpm_* + silver_bdpm_cip_history + _dq_quarantine
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
# 1. Chargement des utilitaires (via `nb_util_validation`)
# 2. Paramètres
# 3. Constantes de normalisation
# 4. Helpers PySpark purs
# 5. Constructeurs Silver par source
# 6. Orchestration : écriture, quarantaine, historique des prix
# 7. Exécution
# 8. Rapport et sortie JSON
# 
# ## 📤 Contrat de sortie
# 
# `{"status","as_of","total_rows","results":[...]}`


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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3 · Imports et constantes de normalisation

# CELL ********************

from __future__ import annotations

import time
from datetime import date
from typing import Callable, Iterable

from pyspark.sql import Column, DataFrame, Window, functions as F

silver_logger = get_logger("silver")

ACCENTS_FROM = "àâäáãéèêëíìîïóòôöõúùûüçñœ" + UNICODE_REPLACEMENT_CHAR
ACCENTS_TO = "aaaaaeeeeiiiiooooouuuucno"
CIS_CODE_REGEX = "^[0-9]{8}$"
ATC_CODE_REGEX = "^[A-Z][0-9]{2}([A-Z]([A-Z]([0-9]{2})?)?)?$"

CONDITION_CATEGORIES = [
    (r"liste ii\b", "Liste II"),
    (r"liste i\b", "Liste I"),
    (r"stup.?fiant", "Stupéfiant"),
    (r"usage hospitalier|prescription hospitali.?re", "Réservé hospitalier"),
    (r"r.?serv.?e? aux (sp.?cialistes|m.?decins)|prescription initiale|prescription restreinte|usage professionnel", "Prescription spécialiste"),
    (r"ordonnance s.?curis.?e|toutes lettres", "Ordonnance sécurisée"),
    (r"m.?dicament d.exception", "Médicament d'exception"),
    (r"surveillance particuli.?re", "Surveillance particulière"),
]
REQUIRED_CONDITION_CATEGORIES = (
    "Liste I",
    "Liste II",
    "Stupéfiant",
    "Réservé hospitalier",
    "Prescription spécialiste",
    "Ordonnance sécurisée",
    "Médicament d'exception",
)
HTML_ENTITY_REPLACEMENTS = [
    ("&amp;", "&"),
    ("&quot;", '"'),
    ("&#39;", "'"),
    ("&rsquo;", "\u2019"),
    ("&nbsp;", " "),
    ("&eacute;", "é"),
    ("&egrave;", "è"),
    ("&ecirc;", "ê"),
    ("&agrave;", "à"),
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4 · Helpers purs PySpark de normalisation et lineage

# CELL ********************

from __future__ import annotations

# ==============================================================================
# HELPERS PURS PYSPARK DE NORMALISATION & LINEAGE
# ==============================================================================
def clean_text_column(column_name: str) -> Column:
    """Supprime CR/LF finaux et espaces (classiques + insécables), convertit vide -> NULL."""
    cleaned = F.trim(
        F.regexp_replace(
            F.regexp_replace(F.col(column_name), r"[\r\n]+$", ""),
            NON_BREAKING_SPACE,
            " ",
        )
    )
    return F.when(cleaned == "", F.lit(None).cast("string")).otherwise(cleaned)


def clean_string_columns(df: DataFrame, column_names: Iterable[str]) -> DataFrame:
    """Applique clean_text_column sur un ensemble explicite de colonnes texte."""
    result_df = df
    for col_name in column_names:
        result_df = result_df.withColumn(col_name, clean_text_column(col_name))
    return result_df


def normalize_lowercase_ascii(column: Column) -> Column:
    """Met en minuscules, supprime les accents et retire le caractère U+FFFD."""
    return F.trim(F.translate(F.lower(column), ACCENTS_FROM, ACCENTS_TO))


def parse_flexible_date(column_name: str, formats: tuple[str, ...] = ("dd/MM/yyyy", "yyyyMMdd", "yyyy-MM-dd")) -> Column:
    """Parse tolérant (try_to_date sans exception en mode ANSI strict)."""
    trimmed = F.trim(F.col(column_name))
    return F.coalesce(*[F.try_to_date(trimmed, date_fmt) for date_fmt in formats])


def parse_french_decimal(column_name: str, precision: int = 14, scale: int = 2) -> Column:
    """Convertit un nombre français ('1 466,29' ou '1,466,29') en decimal(precision, scale)."""
    without_spaces = F.regexp_replace(F.col(column_name), f"[\\s{NON_BREAKING_SPACE}]", "")
    normalized = F.regexp_replace(F.regexp_replace(without_spaces, r"[.,](?=.*[.,])", ""), ",", ".")
    return F.when(normalized == "", F.lit(None)).otherwise(normalized).try_cast(f"decimal({precision},{scale})")


def is_valid_ean13(column_name: str) -> Column:
    """Valide le format 13 chiffres et la clé de contrôle EAN-13 (poids 1, 3, 1, 3...)."""
    col = F.col(column_name)
    digits = [col.substr(pos, 1).try_cast("int") for pos in range(1, 14)]
    weighted_sum = sum(digits[idx] * (3 if idx % 2 else 1) for idx in range(12))
    checksum_matches = ((F.lit(10) - (weighted_sum % 10)) % 10) == digits[12]
    return F.coalesce(col.rlike("^[0-9]{13}$") & checksum_matches, F.lit(False))


def compute_duration_bucket(days_column: Column) -> Column:
    """Regroupe les durées en tranches ordinales lisibles."""
    return (
        F.when(days_column.isNull(), "Inconnue")
        .when(days_column < 0, "Incohérente")
        .when(days_column < 30, "< 30 jours")
        .when(days_column < 90, "30-90 jours")
        .when(days_column <= 365, "90-365 jours")
        .otherwise("> 365 jours")
    )


def deduplicate_latest(df: DataFrame, partition_keys: list[str], order_cols: list[Column]) -> tuple[DataFrame, DataFrame]:
    """Dédoublonne de façon déterministe et retourne (conservés, doublons_remplacés)."""
    dedup_window = Window.partitionBy(*partition_keys).orderBy(*order_cols)
    df_ranked = df.withColumn("_rn", F.row_number().over(dedup_window))
    df_kept = df_ranked.filter(F.col("_rn") == 1).drop("_rn")
    df_duplicates = (
        df_ranked.filter(F.col("_rn") > 1)
        .drop("_rn")
        .withColumn("_reject_reason", F.lit("doublon_remplace"))
    )
    return df_kept, df_duplicates


def prepare_bronze_lineage(df: DataFrame) -> DataFrame:
    """Préserve le lignage Bronze et retire les colonnes techniques redondantes."""
    return (
        df.withColumnRenamed("_ingested_at", "_bronze_ingested_at")
        .withColumnRenamed("_pipeline_run_id", "_bronze_run_id")
        .drop("_source_system")
    )


def append_silver_audit(df: DataFrame, current_run_id: str) -> DataFrame:
    """Ajoute les métadonnées d'audit Silver."""
    return df.withColumn("_cleaned_at", F.current_timestamp()).withColumn("_pipeline_run_id", F.lit(current_run_id))


def split_valid_and_rejected(df: DataFrame, reject_reason_col: Column) -> tuple[DataFrame, DataFrame]:
    """Sépare lignes valides et lignes rejetées sans perte silencieuse."""
    df_tagged = df.withColumn("_reject_reason", reject_reason_col)
    df_valid = df_tagged.filter("_reject_reason IS NULL").drop("_reject_reason")
    df_rejected = df_tagged.filter("_reject_reason IS NOT NULL")
    return df_valid, df_rejected


def combine_rejects(df_invalid: DataFrame, df_duplicates: DataFrame) -> DataFrame:
    """Unifie les rejets fonctionnels et les doublons écartés."""
    return df_invalid.unionByName(df_duplicates, allowMissingColumns=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5 · Constructeurs métier Silver par source BDPM

# CELL ********************

# ==============================================================================
# CONSTRUCTEURS MÉTIER SILVER PAR SOURCE BDPM
# ==============================================================================
def build_silver_specialties(df_bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Nettoie et type les spécialités pharmaceutiques (grain CIS)."""
    df = prepare_bronze_lineage(df_bronze)
    df = clean_string_columns(
        df,
        [
            "cis_code", "denomination", "forme_pharmaceutique", "voie_administration", "statut_amm",
            "type_procedure_amm", "etat_commercialisation", "statut_bdm", "numero_autorisation_europeenne", "titulaire",
        ],
    )

    surveillance_norm = normalize_lowercase_ascii(clean_text_column("surveillance_renforcee"))
    routes_array = F.filter(F.transform(F.split(F.col("voie_administration"), ";"), lambda x: F.trim(x)), lambda x: x != "")

    df = (
        df.withColumn("date_amm", parse_flexible_date("date_amm"))
        .withColumn(
            "surveillance_renforcee",
            F.when(surveillance_norm == "oui", True).when(surveillance_norm == "non", False),
        )
        .withColumn(
            "voies_administration_array",
            F.when(F.col("voie_administration").isNull(), F.array().cast("array<string>")).otherwise(routes_array),
        )
        .withColumn("nombre_voies", F.size("voies_administration_array"))
        .withColumn(
            "is_commercialisee",
            F.coalesce(normalize_lowercase_ascii(F.col("etat_commercialisation")).startswith("commercialis"), F.lit(False)),
        )
    )

    df_valid, df_rejected = split_valid_and_rejected(
        df,
        F.when(~F.coalesce(F.col("cis_code").rlike(CIS_CODE_REGEX), F.lit(False)), "cis_code_invalide"),
    )
    df_kept, df_duplicates = deduplicate_latest(
        df_valid,
        ["cis_code"],
        [F.col("_snapshot_date").desc_nulls_last(), F.col("denomination").asc_nulls_last()],
    )
    return df_kept, combine_rejects(df_rejected, df_duplicates)


def build_silver_presentations(df_bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Nettoie et type les présentations CIP13 (prix, honoraires, remboursement)."""
    df = prepare_bronze_lineage(df_bronze)
    df = clean_string_columns(
        df,
        [
            "cis_code", "cip7", "libelle_presentation", "statut_administratif", "etat_commercialisation",
            "cip13", "agrement_collectivites", "taux_remboursement", "indications_remboursement",
        ],
    )

    declared_date = parse_flexible_date("date_declaration_commercialisation", ("dd/MM/yyyy",))
    is_aberrant_date = (
        declared_date.isNotNull()
        & ((F.year(declared_date) > F.year(F.col("_snapshot_date"))) | (F.year(declared_date) < 1900))
    )
    reimbursement_rates_array = F.expr("regexp_extract_all(coalesce(taux_remboursement, ''), '([0-9]+)', 1)")

    df = (
        df.withColumn("_tmp_price_excl_fee", parse_french_decimal("prix_hors_honoraires", 10, 2))
        .withColumn("_tmp_price_incl_fee", parse_french_decimal("prix_avec_honoraires", 10, 2))
        .withColumn("_tmp_dispensing_fee", parse_french_decimal("honoraires_dispensation", 10, 2))
    )
    col_price_excl = F.col("_tmp_price_excl_fee")
    col_price_incl = F.col("_tmp_price_incl_fee")
    col_fee = F.col("_tmp_dispensing_fee")

    df = (
        df.withColumn("prix_hors_honoraires", col_price_excl)
        .withColumn("prix_avec_honoraires", col_price_incl)
        .withColumn("honoraires_dispensation", F.coalesce(col_fee, (col_price_incl - col_price_excl).cast("decimal(10,2)")))
        .withColumn(
            "honoraires_incoherents",
            col_fee.isNotNull()
            & col_price_excl.isNotNull()
            & col_price_incl.isNotNull()
            & (F.abs(col_price_excl + col_fee - col_price_incl) > F.lit(PRICE_FEE_COHERENCE_TOLERANCE)),
        )
        .drop("_tmp_price_excl_fee", "_tmp_price_incl_fee", "_tmp_dispensing_fee")
        .withColumn("taux_remboursement_liste", F.col("taux_remboursement"))
        .withColumn(
            "taux_remboursement",
            F.array_max(F.transform(reimbursement_rates_array, lambda rate: rate.try_cast("int"))).cast("double"),
        )
        .withColumn("date_declaration_aberrante", is_aberrant_date)
        .withColumn("date_declaration_commercialisation", F.when(~is_aberrant_date, declared_date))
        .withColumn("cip13_format_ok", F.coalesce(F.col("cip13").rlike("^[0-9]{13}$"), F.lit(False)))
        .withColumn("cip13_valid", is_valid_ean13("cip13"))
        .withColumn("has_price", F.coalesce(F.col("prix_hors_honoraires") > 0, F.lit(False)))
        .withColumn("is_reimbursed", F.coalesce(F.col("taux_remboursement") > 0, F.lit(False)))
        .withColumn(
            "is_commercialisee",
            F.coalesce(
                normalize_lowercase_ascii(F.col("etat_commercialisation")).rlike(r"^d.?claration de commercialisation"),
                F.lit(False),
            ),
        )
        .withColumn("snapshot_date", F.col("_snapshot_date"))
    )

    df_valid, df_rejected = split_valid_and_rejected(
        df,
        F.when(F.col("cip13").isNull(), "cip13_absent")
        .when(~F.coalesce(F.col("cis_code").rlike(CIS_CODE_REGEX), F.lit(False)), "cis_code_invalide"),
    )
    df_kept, df_duplicates = deduplicate_latest(
        df_valid,
        ["cip13"],
        [F.col("_snapshot_date").desc_nulls_last(), F.col("date_declaration_commercialisation").desc_nulls_last(), F.col("cis_code").asc()],
    )
    return df_kept, combine_rejects(df_rejected, df_duplicates)


def build_silver_ruptures(df_bronze: DataFrame, as_of_date: date) -> tuple[DataFrame, DataFrame]:
    """Nettoie les signalements de ruptures et tensions d'approvisionnement (ANSM)."""
    df = clean_string_columns(prepare_bronze_lineage(df_bronze), ["cis_code", "cip13", "code_statut", "lien_ansm"])

    status_code_int = F.col("code_statut").try_cast("int")
    status_label_map = F.create_map(
        *[item for code, label in STATUT_LABELS.items() for item in (F.lit(code), F.lit(label))]
    )[status_code_int]
    as_of_lit = F.lit(as_of_date.isoformat()).cast("date")

    df = (
        df.withColumn("code_statut", status_code_int)
        .withColumn("libelle_statut", status_label_map)
        .withColumn("classification", F.upper(status_label_map))
        .withColumn("cip13_invalide", F.col("cip13").isNotNull() & ~is_valid_ean13("cip13"))
        .withColumn("cip13", F.when(is_valid_ean13("cip13"), F.col("cip13")))
        .withColumn("date_debut", parse_flexible_date("date_debut"))
        .withColumn("date_mise_a_jour", parse_flexible_date("date_mise_a_jour"))
        .withColumn("date_fin", parse_flexible_date("date_remise"))
        .drop("date_remise")
        .withColumn("is_active", F.col("code_statut").isin(*ACTIVE_STATUS_CODES))
        .withColumn(
            "_tmp_end_reference",
            F.when(F.col("is_active"), as_of_lit).otherwise(F.coalesce(F.col("date_fin"), F.col("date_mise_a_jour"))),
        )
        .withColumn("duration_days", F.datediff("_tmp_end_reference", "date_debut"))
        .drop("_tmp_end_reference")
        .withColumn("duration_bucket", compute_duration_bucket(F.col("duration_days")))
        .withColumn("days_since_update", F.datediff(as_of_lit, F.col("date_mise_a_jour")))
        .withColumn("remise_depassee", F.col("is_active") & F.col("date_fin").isNotNull() & (F.col("date_fin") < as_of_lit))
        .withColumn("cip13_key", F.coalesce(F.col("cip13"), F.lit("TOUS")))
        .withColumn("as_of_date", as_of_lit)
    )

    df_valid, df_rejected = split_valid_and_rejected(
        df,
        F.when(~F.coalesce(F.col("cis_code").rlike(CIS_CODE_REGEX), F.lit(False)), "cis_code_invalide")
        .when(F.col("code_statut").isNull() | ~F.col("code_statut").isin(*STATUT_LABELS), "code_statut_invalide")
        .when(F.col("date_debut").isNull(), "date_debut_illisible"),
    )
    df_kept, df_duplicates = deduplicate_latest(
        df_valid,
        ["cis_code", "cip13_key"],
        [
            F.col("date_mise_a_jour").desc_nulls_last(),
            F.col("date_debut").desc_nulls_last(),
            F.col("code_statut").desc(),
            F.col("date_fin").desc_nulls_last(),
        ],
    )
    return df_kept, combine_rejects(df_rejected, df_duplicates)


def build_silver_compositions(df_bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Nettoie et extrait les dosages des substances actives (SA) et fractions thérapeutiques (FT)."""
    df = clean_string_columns(
        prepare_bronze_lineage(df_bronze),
        [
            "cis_code", "designation_element", "code_substance", "denomination_substance",
            "dosage_substance", "reference_dosage", "nature_composant", "numero_liaison_sa_ft",
        ],
    )

    col_dosage = F.col("dosage_substance")
    numeric_token = F.regexp_extract(col_dosage, f"^\\s*([0-9]+(?:[ {NON_BREAKING_SPACE}][0-9]{{3}})*(?:[.,][0-9]+)?)", 1)
    unit_token = F.trim(F.regexp_extract(col_dosage, f"^\\s*[0-9][0-9\\s{NON_BREAKING_SPACE}.,]*?\\s*([^0-9\\s.,].*)$", 1))

    df = (
        df.withColumn("denomination_substance", F.upper(F.col("denomination_substance")))
        .withColumn("nature_composant", F.upper(F.col("nature_composant")))
        .withColumn(
            "dosage_numeric",
            F.when(
                numeric_token != "",
                F.regexp_replace(F.regexp_replace(numeric_token, f"[\\s{NON_BREAKING_SPACE}]", ""), ",", ".").try_cast("double"),
            ),
        )
        .withColumn("dosage_unit", F.when(unit_token != "", unit_token))
        .withColumn(
            "is_active_substance",
            F.when(F.col("nature_composant").isNull(), F.lit(None).cast("boolean")).otherwise(F.col("nature_composant") == "SA"),
        )
    )

    df_valid, df_rejected = split_valid_and_rejected(
        df,
        F.when(~F.coalesce(F.col("cis_code").rlike(CIS_CODE_REGEX), F.lit(False)), "cis_code_invalide"),
    )
    df_kept, df_duplicates = deduplicate_latest(
        df_valid,
        ["cis_code", "designation_element", "code_substance", "nature_composant"],
        [F.col("_snapshot_date").desc_nulls_last(), F.col("numero_liaison_sa_ft").asc_nulls_last()],
    )
    return df_kept, combine_rejects(df_rejected, df_duplicates)


def build_silver_generics(df_bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Nettoie les groupes génériques et qualifie princeps vs génériques."""
    df = clean_string_columns(
        prepare_bronze_lineage(df_bronze),
        ["generic_group_id", "libelle_groupe", "cis_code", "type_generique"],
    )

    generic_type_int = F.col("type_generique").try_cast("int")
    generic_label_map = F.create_map(
        *[item for code, label in GENERIC_LABELS.items() for item in (F.lit(code), F.lit(label))]
    )[generic_type_int]

    df = (
        df.withColumn("numero_tri", F.col("numero_tri").try_cast("int"))
        .withColumn("type_generique", generic_type_int.cast("string"))
        .withColumn("type_generique_label", F.coalesce(generic_label_map, F.lit("Inconnu")))
        .withColumn(
            "is_princeps",
            F.when(generic_type_int.isNull(), F.lit(None).cast("boolean")).otherwise(generic_type_int == 0),
        )
        .withColumn(
            "is_generic",
            F.when(generic_type_int.isNull(), F.lit(None).cast("boolean")).otherwise(generic_type_int.isin(1, 2, 3, 4)),
        )
    )

    df_valid, df_rejected = split_valid_and_rejected(
        df,
        F.when(~F.coalesce(F.col("cis_code").rlike(CIS_CODE_REGEX), F.lit(False)), "cis_code_invalide")
        .when(F.col("generic_group_id").isNull(), "groupe_absent"),
    )
    df_kept, df_duplicates = deduplicate_latest(
        df_valid,
        ["generic_group_id", "cis_code"],
        [F.col("_snapshot_date").desc_nulls_last(), F.col("numero_tri").asc_nulls_last()],
    )
    return df_kept, combine_rejects(df_rejected, df_duplicates)


def build_silver_conditions(df_bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Catégorise les conditions de prescription et de délivrance."""
    df = clean_string_columns(prepare_bronze_lineage(df_bronze), ["cis_code", "condition_prescription"])

    condition_norm = normalize_lowercase_ascii(F.col("condition_prescription"))
    category_expr = F.lit("Autre")
    for regex_pattern, category_label in reversed(CONDITION_CATEGORIES):
        category_expr = F.when(condition_norm.rlike(regex_pattern), category_label).otherwise(category_expr)
    category_expr = F.when(condition_norm.isNull(), F.lit("Autre")).otherwise(category_expr)

    df = df.withColumn("prescription_category", category_expr).withColumn(
        "is_prescription_required",
        F.col("prescription_category").isin(*REQUIRED_CONDITION_CATEGORIES)
        & ~F.coalesce(condition_norm.contains("sans ordonnance"), F.lit(False)),
    )

    df_valid, df_rejected = split_valid_and_rejected(
        df,
        F.when(~F.coalesce(F.col("cis_code").rlike(CIS_CODE_REGEX), F.lit(False)), "cis_code_invalide")
        .when(F.col("condition_prescription").isNull(), "condition_vide"),
    )
    df_kept, df_duplicates = deduplicate_latest(
        df_valid,
        ["cis_code", "condition_prescription"],
        [F.col("_snapshot_date").desc_nulls_last()],
    )
    return df_kept, combine_rejects(df_rejected, df_duplicates)


def compute_smr_rank(value_col: Column) -> Column:
    """Rang ordinal SMR : Important=4, Modéré=3, Faible=2, Insuffisant=1."""
    text_norm = normalize_lowercase_ascii(value_col)
    return (
        F.when(text_norm.rlike("^important"), 4)
        .when(text_norm.rlike("^mod"), 3)
        .when(text_norm.rlike("^faible"), 2)
        .when(text_norm.rlike("^insuffisant"), 1)
    )


def compute_asmr_rank(value_col: Column) -> Column:
    """Rang ordinal ASMR : I=1 ... V=5."""
    roman_token = F.regexp_extract(normalize_lowercase_ascii(value_col), r"^(iv|v|iii|ii|i)(\b|$)", 1)
    return (
        F.when(roman_token == "i", 1)
        .when(roman_token == "ii", 2)
        .when(roman_token == "iii", 3)
        .when(roman_token == "iv", 4)
        .when(roman_token == "v", 5)
    )


def build_silver_has_opinions(df_bronze: DataFrame, opinion_kind: str) -> tuple[DataFrame, DataFrame]:
    """Factorise le nettoyage des avis HAS SMR et ASMR."""
    value_col_name = f"valeur_{opinion_kind}"
    label_col_name = f"libelle_{opinion_kind}"
    df = clean_string_columns(
        prepare_bronze_lineage(df_bronze),
        ["cis_code", "code_dossier_has", "motif_evaluation", value_col_name, label_col_name],
    )

    rank_expr = compute_smr_rank(F.col(value_col_name)) if opinion_kind == "smr" else compute_asmr_rank(F.col(value_col_name))
    df = (
        df.withColumn("date_avis", parse_flexible_date("date_avis"))
        .withColumn(value_col_name, F.upper(F.col(value_col_name)))
        .withColumn(f"{opinion_kind}_rank", rank_expr.cast("int"))
    )

    df_valid, df_rejected = split_valid_and_rejected(
        df,
        F.when(~F.coalesce(F.col("cis_code").rlike(CIS_CODE_REGEX), F.lit(False)), "cis_code_invalide"),
    )
    latest_opinion_window = Window.partitionBy("cis_code").orderBy(
        F.col("date_avis").desc_nulls_last(),
        F.col("code_dossier_has").desc_nulls_last(),
    )
    df_valid = df_valid.withColumn("is_latest_avis", F.row_number().over(latest_opinion_window) == 1)
    df_kept, df_duplicates = deduplicate_latest(
        df_valid,
        ["cis_code", "code_dossier_has", value_col_name],
        [F.col("_snapshot_date").desc_nulls_last()],
    )
    return df_kept, combine_rejects(df_rejected, df_duplicates)


def build_silver_smr(df_bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    return build_silver_has_opinions(df_bronze, "smr")


def build_silver_asmr(df_bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    return build_silver_has_opinions(df_bronze, "asmr")


def build_silver_mitm(df_bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Médicaments d'intérêt thérapeutique majeur et hiérarchie ATC."""
    df = clean_string_columns(
        prepare_bronze_lineage(df_bronze),
        ["cis_code", "code_atc", "denomination", "titulaire", "exploitant", "lien_fiche"],
    )

    atc_upper = F.upper(F.col("code_atc"))
    df = (
        df.withColumn("code_atc", atc_upper)
        .withColumn("date_mise_a_jour", parse_flexible_date("date_mise_a_jour", ("dd/MM/yyyy",)))
        .withColumn("atc_valid", F.coalesce(atc_upper.rlike(ATC_CODE_REGEX), F.lit(False)))
        .withColumn("atc_level1", F.when(F.col("atc_valid"), F.substring(atc_upper, 1, 1)))
        .withColumn("atc_level2", F.when(F.col("atc_valid"), F.substring(atc_upper, 1, 3)))
        .withColumn("atc_level3", F.when(F.col("atc_valid") & (F.length(atc_upper) >= 4), F.substring(atc_upper, 1, 4)))
        .withColumn("atc_level4", F.when(F.col("atc_valid") & (F.length(atc_upper) >= 5), F.substring(atc_upper, 1, 5)))
        .withColumn("atc_level5", F.when(F.col("atc_valid") & (F.length(atc_upper) == 7), atc_upper))
        .withColumn("is_mitm", F.lit(True))
    )

    df_valid, df_rejected = split_valid_and_rejected(
        df,
        F.when(~F.coalesce(F.col("cis_code").rlike(CIS_CODE_REGEX), F.lit(False)), "cis_code_invalide")
        .when(F.col("code_atc").isNull(), "atc_absent"),
    )
    df_kept, df_duplicates = deduplicate_latest(
        df_valid,
        ["cis_code"],
        [F.col("_snapshot_date").desc_nulls_last(), F.col("date_mise_a_jour").desc_nulls_last(), F.col("code_atc").asc()],
    )
    return df_kept, combine_rejects(df_rejected, df_duplicates)


def build_silver_important_info(df_bronze: DataFrame, as_of_date: date) -> tuple[DataFrame, DataFrame]:
    """Informations importantes de sécurité ANSM : extraction titre + URL depuis HTML."""
    df = clean_string_columns(prepare_bronze_lineage(df_bronze), ["cis_code", "html_info"])

    as_of_lit = F.lit(as_of_date.isoformat()).cast("date")
    extracted_title = F.regexp_extract(F.col("html_info"), ">([^<]*)</a>", 1)
    for html_entity, plain_char in HTML_ENTITY_REPLACEMENTS:
        extracted_title = F.regexp_replace(extracted_title, html_entity, plain_char)
    extracted_url = F.regexp_extract(F.col("html_info"), """href=['"]([^'"]+)['"]""", 1)

    df = (
        df.withColumn("date_debut", parse_flexible_date("date_debut", ("yyyy-MM-dd",)))
        .withColumn("date_fin", parse_flexible_date("date_fin", ("yyyy-MM-dd",)))
        .withColumn("info_title", F.when(extracted_title != "", F.trim(extracted_title)))
        .withColumn("info_url", F.when(extracted_url != "", extracted_url))
        .withColumn("info_type", F.when(extracted_url != "", F.regexp_extract(extracted_url, r"ansm\.sante\.fr/([^/]+)/", 1)))
        .withColumn("info_type", F.when(F.col("info_type") != "", F.col("info_type")))
        .withColumn(
            "is_active",
            F.coalesce(
                (F.col("date_debut") <= as_of_lit) & (F.col("date_fin").isNull() | (F.col("date_fin") >= as_of_lit)),
                F.lit(False),
            ),
        )
        .withColumn("duration_days", F.datediff("date_fin", "date_debut"))
        .withColumn("days_remaining", F.when(F.col("is_active"), F.datediff("date_fin", as_of_lit)))
        .withColumn("as_of_date", as_of_lit)
    )

    df_valid, df_rejected = split_valid_and_rejected(
        df,
        F.when(~F.coalesce(F.col("cis_code").rlike(CIS_CODE_REGEX), F.lit(False)), "cis_code_invalide")
        .when(F.col("date_debut").isNull(), "date_debut_illisible")
        .when(F.col("info_url").isNull(), "lien_absent")
        .when(F.col("date_fin").isNotNull() & (F.col("date_fin") < F.col("date_debut")), "periode_incoherente"),
    )
    df_kept, df_duplicates = deduplicate_latest(
        df_valid,
        ["cis_code", "date_debut", "date_fin", "info_url"],
        [F.col("_snapshot_date").desc_nulls_last(), F.col("info_title").asc_nulls_last()],
    )
    return df_kept, combine_rejects(df_rejected, df_duplicates)


def build_silver_ct_links(df_bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Liens vers les avis de la Commission de la Transparence (HAS)."""
    df = clean_string_columns(prepare_bronze_lineage(df_bronze), ["code_dossier_has", "lien_page_ct"])
    df = df.withColumn("url_ok", F.coalesce(F.col("lien_page_ct").rlike("^https?://"), F.lit(False)))
    df_valid, df_rejected = split_valid_and_rejected(
        df,
        F.when(F.col("code_dossier_has").isNull(), "code_dossier_absent")
        .when(~F.col("url_ok"), "lien_invalide"),
    )
    df_kept, df_duplicates = deduplicate_latest(
        df_valid,
        ["code_dossier_has"],
        [F.col("_snapshot_date").desc_nulls_last(), F.col("lien_page_ct").asc()],
    )
    return df_kept, combine_rejects(df_rejected, df_duplicates)


STATELESS_SILVER_BUILDERS: dict[str, Callable[[DataFrame], tuple[DataFrame, DataFrame]]] = {
    "specialties": build_silver_specialties,
    "presentations": build_silver_presentations,
    "compositions": build_silver_compositions,
    "generics": build_silver_generics,
    "conditions": build_silver_conditions,
    "smr": build_silver_smr,
    "asmr": build_silver_asmr,
    "mitm": build_silver_mitm,
    "ct_links": build_silver_ct_links,
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6 · Orchestration couche Silver (écriture + quarantaine + historique cumulatif)

# CELL ********************

# ==============================================================================
# ORCHESTRATION COUCHE SILVER (ÉCRITURE + QUARANTAINE + HISTORIQUE CUMULATIF)
# ==============================================================================
def write_silver_table(
    table_io_client: TableIO,
    source_key: str,
    df_valid: DataFrame,
    df_rejected: DataFrame,
    current_run_id: str,
) -> StepResult:
    """Écrit la table Silver typée, lit les métriques une fois et met en quarantaine les rejets."""
    silver_table_name = SILVER_TABLES[source_key]
    df_audited = append_silver_audit(df_valid, current_run_id)
    table_io_client.overwrite(df_audited, silver_table_name)

    df_written = table_io_client.read(silver_table_name)
    inserted_rows = df_written.count()
    column_count = len(df_written.columns)

    df_rejected_cached = df_rejected.cache()
    try:
        rejected_count = df_rejected_cached.count()
        reasons_breakdown: dict[str, int] = {}
        if rejected_count:
            quarantine_rejected_rows(
                table_io_client, df_rejected_cached, silver_table_name, None, "silver", current_run_id
            )
            grouped_reasons = df_rejected_cached.groupBy("_reject_reason").count().collect()
            reasons_breakdown = {row["_reject_reason"]: row["count"] for row in grouped_reasons}
    finally:
        df_rejected_cached.unpersist()

    return StepResult(
        source=source_key,
        table=silver_table_name,
        status="SUCCESS",
        error=None,
        rows=inserted_rows,
        columns=column_count,
        details={"rejected": rejected_count, "reject_reasons": reasons_breakdown},
    )


def execute_silver_pipeline(
    spark_session,
    table_io_client: TableIO,
    current_run_id: str,
    as_of_date: date,
) -> dict:
    """Construit l'ensemble des tables Silver et maintient silver_bdpm_cip_history."""
    del spark_session
    start_time = time.time()
    step_results: list[StepResult] = []
    transformation_plan = [
        (key, builder_fn) for key, builder_fn in STATELESS_SILVER_BUILDERS.items()
    ] + [
        ("ruptures", lambda df_bronze: build_silver_ruptures(df_bronze, as_of_date)),
        ("important_info", lambda df_bronze: build_silver_important_info(df_bronze, as_of_date)),
    ]

    for source_key, builder_fn in transformation_plan:
        source_spec = BDPM_SOURCES[source_key]
        bronze_table_name = source_spec.table
        silver_table_name = SILVER_TABLES[source_key]
        try:
            if not table_io_client.exists(bronze_table_name):
                step_status = "SKIPPED" if source_spec.optional else "FAILED"
                step_results.append(
                    StepResult(source_key, silver_table_name, step_status, f"Table Bronze {bronze_table_name} absente")
                )
                continue

            df_bronze_source = table_io_client.read(bronze_table_name)
            df_valid, df_rejected = builder_fn(df_bronze_source)
            step_result = write_silver_table(table_io_client, source_key, df_valid, df_rejected, current_run_id)

            if source_key == "presentations":
                df_history_slice = table_io_client.read(silver_table_name).select(
                    "cip13", "cis_code", "snapshot_date", "prix_hors_honoraires", "prix_avec_honoraires",
                    "taux_remboursement", "honoraires_dispensation", "has_price", "is_reimbursed",
                )
                distinct_snapshots = [row[0] for row in df_history_slice.select("snapshot_date").distinct().collect()]
                table_io_client.replace_where(
                    df_history_slice, SILVER_PRICE_HISTORY_TABLE, "snapshot_date", distinct_snapshots
                )
                step_result.details["history_snapshots"] = [str(snapshot) for snapshot in distinct_snapshots]

            step_results.append(step_result)
        except Exception as exc:  # noqa: BLE001
            silver_logger.exception("Erreur Silver sur %s", source_key)
            step_results.append(StepResult(source_key, silver_table_name, "FAILED", f"{type(exc).__name__}: {exc}"))

    return summarize_results(
        step_results,
        CRITICAL_SOURCES,
        current_run_id,
        time.time() - start_time,
        extra={"as_of": as_of_date.isoformat(), "results": [res.to_dict() for res in step_results]},
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7 · Exécution
# 
# Résolution du contexte (format de table, landing, date de référence), bannière de traçabilité puis appel de la fonction d'orchestration.

# CELL ********************

table_io = create_table_io(spark)
reference_date = parse_run_date(run_date)
init_notebook_banner("nb_silver_clean_bdpm", run_id, reference_date, table_io.fmt)

payload = execute_silver_pipeline(
    spark_session=spark,
    table_io_client=table_io,
    current_run_id=run_id,
    as_of_date=reference_date,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8 · Rapport d'exécution et sortie JSON
# 
# Affiche la synthèse puis renvoie le résumé JSON au pipeline via `notebookutils.notebook.exit` (lisible dans la sortie de l'activité).

# CELL ********************

render_execution_report(
    payload,
    columns=["source", "table", "status", "rows", "columns"],
    summary_line=(
        f"🎯 Statut Silver : {payload.get('status')} | "
        f"{payload.get('total_rows', 0):,} lignes traitées | "
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
# *HealthTek — nb_silver_clean_bdpm · documentation générée selon le standard du projet.*
