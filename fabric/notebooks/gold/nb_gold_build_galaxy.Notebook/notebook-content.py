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

# # 🏆 [GOLD] `nb_gold_build_galaxy` — Constellation Analytique Galaxie & KPIs
# 
# > **HealthTek** · Plateforme analytique des ruptures et tensions d'approvisionnement de médicaments (données publiques BDPM / ANSM / HAS) — Microsoft Fabric
# 
# | Élément | Détail |
# |---|---|
# | **Couche** | Gold |
# | **Type** | Notebook exécutable |
# | **Orchestration** | Activité `run_gold` — pipeline `pl_master_health_pipeline` |
# | **Entrées** | `silver_bdpm_*`, `silver_bdpm_cip_history` |
# | **Sorties** | 8 dimensions, 5 faits, 4 KPI `gold_*` (Direct Lake) |
# | **Idempotence** | Oui (overwrite ; MERGE sur `cip13 × snapshot_date` en mode incrémental) |
# | **Documentation** | v3.1 — 10/11/2026 |
# 
# ## 🎯 Objectif
# 
# Construit le modèle dimensionnel en **Galaxie** (8 dimensions partagées, 5 tables de faits à grain homogène et 4 tables d'indicateurs KPIs) :
# - Clés de substitution déterministes (`xxhash64`) compatibles Power BI Direct Lake & Fabric Warehouse.
# - Gestion des dimensions à arrivée tardive (membres inférés `is_inferred` + membre « Inconnu » `-1`).
# - Cache sélectif des sources Silver réutilisées (libéré dans `try / finally: unpersist()`), optimisation `OPTIMIZE ZORDER BY` et gouvernance Delta `TBLPROPERTIES`.
# 
# ## 🔄 Flux de données
# 
# ```
# silver_bdpm_* + silver_bdpm_cip_history ──► 8 Dimensions + 5 Faits + 4 KPIs (schema_version=3.0.0) ──► gold_*
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
# | `mode` | `"incremental"` | `incremental` : MERGE du dernier instantané de prix ; `full` : reconstruction complète. |
# 
# ## 🧭 Plan du notebook
# 
# 1. Chargement des utilitaires
# 2. Paramètres
# 3. Imports
# 4. Helpers : clés de substitution, membres inconnus / inférés
# 5. Dimensions partagées
# 6. Faits et tables KPI
# 7. Orchestration Gold (cache, écriture, OPTIMIZE, gouvernance)
# 8. Exécution
# 9. Rapport et sortie JSON
# 
# ## 📤 Contrat de sortie
# 
# `{"status","mode","as_of","total_rows","results":[...]}`

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

# Mode de chargement du fait de prix : 'incremental' (MERGE dernier snapshot) ou 'full'
mode = "incremental"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3 · Imports et logger Gold

# CELL ********************

from __future__ import annotations

import time
from datetime import date
from typing import Optional

from pyspark.sql import Column, DataFrame, SparkSession, Window, functions as F
from pyspark.sql import types as T

gold_logger = get_logger("gold")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4 · Helpers clés de substitution, dimensions tardives et calendrier

# CELL ********************

from __future__ import annotations

# ==============================================================================
# HELPERS CLÉS DE SUBSTITUTION, DIMENSIONS TARDIVES & CALENDRIER
# ==============================================================================
def generate_surrogate_key(*column_names: str) -> Column:
    """Calcule une clé de substitution déterministe BIGINT via xxhash64."""
    return F.xxhash64(*[F.coalesce(F.col(col).cast("string"), F.lit("∅")) for col in column_names])


def append_unknown_member(
    spark_session: SparkSession,
    df_dim: DataFrame,
    surrogate_key_col: str,
    **overrides,
) -> DataFrame:
    """Ajoute le membre « Inconnu » (-1) pour éviter les jointures NULL dans Power BI."""
    unknown_row = tuple(
        UNKNOWN_SURROGATE_KEY if field.name == surrogate_key_col else overrides.get(field.name)
        for field in df_dim.schema.fields
    )
    nullable_schema = T.StructType([T.StructField(field.name, field.dataType, True) for field in df_dim.schema.fields])
    return df_dim.unionByName(spark_session.createDataFrame([unknown_row], nullable_schema))


def append_gold_audit(df: DataFrame, current_run_id: str) -> DataFrame:
    """Ajoute les métadonnées d'audit Gold."""
    return df.withColumn("_gold_created_at", F.current_timestamp()).withColumn("_pipeline_run_id", F.lit(current_run_id))


def to_date_surrogate_key(date_col: Column) -> Column:
    """Convertit une date en clé entière YYYYMMDD déterministe."""
    return F.when(date_col.isNotNull(), F.date_format(date_col, "yyyyMMdd").cast("int"))


def to_calendar_time_key(date_col: Column) -> Column:
    """Retourne time_sk uniquement si la date est dans l'horizon du calendrier."""
    return F.when(
        date_col.between(F.lit(CALENDAR_START_DATE).cast("date"), F.lit(CALENDAR_END_DATE).cast("date")),
        to_date_surrogate_key(date_col),
    )


def coalesce_foreign_key(key_col: Column) -> Column:
    """Remplace une clé étrangère orpheline par UNKNOWN_SURROGATE_KEY (-1)."""
    return F.coalesce(key_col, F.lit(UNKNOWN_SURROGATE_KEY))


def collect_valid_cis_codes(*frames: DataFrame) -> DataFrame:
    """Réunit l'ensemble des codes CIS conformes (8 chiffres) présents dans les DataFrames fournis."""
    selected = [frame.select("cis_code") for frame in frames]
    unioned = selected[0]
    for frame in selected[1:]:
        unioned = unioned.unionByName(frame)
    return unioned.filter(F.col("cis_code").rlike(r"^[0-9]{8}$")).distinct()


def _append_inferred_presentations(
    df_existing_dim: DataFrame,
    df_candidate_source: Optional[DataFrame],
    inferred_label: str,
) -> DataFrame:
    """Factorise l'ajout de présentations CIP13 inférées depuis ruptures ou historique de prix."""
    if df_candidate_source is None:
        return df_existing_dim
    missing_cips = (
        df_candidate_source.filter("cip13 IS NOT NULL")
        .select("cip13", "cis_code")
        .dropDuplicates(["cip13"])
        .join(df_existing_dim.select("cip13"), "cip13", "left_anti")
        .select(
            "cip13",
            "cis_code",
            *[
                F.lit(None).cast(field.dataType).alias(field.name)
                for field in df_existing_dim.schema.fields
                if field.name not in ("cip13", "cis_code", "is_inferred", "libelle_presentation")
            ],
            F.lit(inferred_label).alias("libelle_presentation"),
            F.lit(True).alias("is_inferred"),
        )
    )
    return df_existing_dim.unionByName(missing_cips.select(*df_existing_dim.columns))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5 · Construction des 8 dimensions partagées

# CELL ********************

# ==============================================================================
# CONSTRUCTION DES 8 DIMENSIONS PARTAGÉES
# ==============================================================================
def build_dim_drug(
    spark_session: SparkSession,
    df_specialties: DataFrame,
    df_compositions: DataFrame,
    df_generics: DataFrame,
    df_conditions: DataFrame,
    df_smr: DataFrame | None = None,
    df_asmr: DataFrame | None = None,
    df_mitm: DataFrame | None = None,
    other_cis_frames: list[DataFrame] | None = None,
) -> DataFrame:
    """Dimension Médicament (grain CIS, avec membres inférés et membre Inconnu -1)."""
    df_active_substances = df_compositions.filter("is_active_substance")
    primary_substance_window = Window.partitionBy("cis_code").orderBy(
        F.col("designation_element").asc_nulls_last(),
        F.col("code_substance").asc_nulls_last(),
        F.col("denomination_substance").asc_nulls_last(),
    )
    df_primary_substance = (
        df_active_substances.withColumn("_rn", F.row_number().over(primary_substance_window))
        .filter("_rn = 1")
        .select(
            "cis_code",
            F.col("denomination_substance").alias("substance_active_primary"),
            F.col("dosage_substance").alias("dosage_primary"),
            F.col("dosage_unit").alias("dosage_unit_primary"),
        )
    )
    df_substances_agg = df_active_substances.groupBy("cis_code").agg(
        F.array_sort(F.collect_set("denomination_substance")).alias("substances_array"),
        F.countDistinct("denomination_substance").alias("substance_count"),
    )

    generics_window = Window.partitionBy("cis_code").orderBy(
        F.col("is_princeps").desc(),
        F.col("generic_group_id").cast("int").asc_nulls_last(),
        F.col("generic_group_id").asc(),
    )
    df_generic_info = (
        df_generics.withColumn("_rn", F.row_number().over(generics_window))
        .filter("_rn = 1")
        .select("cis_code", "generic_group_id", "type_generique_label", "is_generic", "is_princeps")
    )
    df_conditions_agg = df_conditions.groupBy("cis_code").agg(
        F.array_sort(F.collect_set("prescription_category")).alias("prescription_categories"),
        F.max("is_prescription_required").alias("is_prescription_required"),
    )

    df_base_drugs = df_specialties.select(
        "cis_code", "denomination", "forme_pharmaceutique", "voie_administration",
        "voies_administration_array", "nombre_voies", "statut_amm", "type_procedure_amm",
        "etat_commercialisation", "is_commercialisee", "date_amm", "titulaire", "surveillance_renforcee",
    ).withColumn("is_inferred", F.lit(False))

    if other_cis_frames:
        missing_cis = collect_valid_cis_codes(*other_cis_frames).join(df_specialties.select("cis_code"), "cis_code", "left_anti")
        inferred_metadata = (
            df_mitm.select("cis_code", F.col("denomination").alias("_den"), F.col("titulaire").alias("_tit"))
            if df_mitm is not None
            else df_specialties.limit(0).select(
                "cis_code", F.lit(None).cast("string").alias("_den"), F.lit(None).cast("string").alias("_tit")
            )
        )
        df_inferred_drugs = (
            missing_cis.join(inferred_metadata, "cis_code", "left")
            .select(
                "cis_code",
                F.coalesce("_den", F.concat(F.lit("Spécialité hors fichier (CIS "), F.col("cis_code"), F.lit(")"))).alias("denomination"),
                *[
                    F.lit(None).cast(field.dataType).alias(field.name)
                    for field in df_base_drugs.schema.fields
                    if field.name not in ("cis_code", "denomination", "titulaire", "is_inferred")
                ],
                F.col("_tit").alias("titulaire"),
                F.lit(True).alias("is_inferred"),
            )
            .select(*df_base_drugs.columns)
        )
        df_base_drugs = df_base_drugs.unionByName(df_inferred_drugs)

    df_dim_drug = (
        df_base_drugs.join(df_primary_substance, "cis_code", "left")
        .join(df_substances_agg, "cis_code", "left")
        .join(df_generic_info, "cis_code", "left")
        .join(df_conditions_agg, "cis_code", "left")
    )

    for opinion_kind, df_opinion in (("smr", df_smr), ("asmr", df_asmr)):
        if df_opinion is not None:
            latest_opinion = (
                df_opinion.filter("is_latest_avis")
                .select(
                    "cis_code",
                    F.col(f"valeur_{opinion_kind}").alias(f"{opinion_kind}_latest"),
                    F.col(f"{opinion_kind}_rank").alias(f"{opinion_kind}_rank_latest"),
                )
                .dropDuplicates(["cis_code"])
            )
            df_dim_drug = df_dim_drug.join(latest_opinion, "cis_code", "left")
        else:
            df_dim_drug = (
                df_dim_drug.withColumn(f"{opinion_kind}_latest", F.lit(None).cast("string"))
                .withColumn(f"{opinion_kind}_rank_latest", F.lit(None).cast("int"))
            )

    if df_mitm is not None:
        mitm_attrs = df_mitm.select("cis_code", "code_atc", "atc_level1", "atc_level2", "is_mitm").dropDuplicates(["cis_code"])
        df_dim_drug = df_dim_drug.join(mitm_attrs, "cis_code", "left")
    else:
        df_dim_drug = (
            df_dim_drug.withColumn("code_atc", F.lit(None).cast("string"))
            .withColumn("atc_level1", F.lit(None).cast("string"))
            .withColumn("atc_level2", F.lit(None).cast("string"))
            .withColumn("is_mitm", F.lit(None).cast("boolean"))
        )

    atc_level1_map = F.create_map(*[item for code, label in ATC_LEVEL1.items() for item in (F.lit(code), F.lit(label))])
    df_dim_drug = (
        df_dim_drug.withColumn("voies_administration", F.concat_ws("; ", "voies_administration_array"))
        .withColumn("substances_list", F.concat_ws("; ", "substances_array"))
        .withColumn("prescription_categories_list", F.concat_ws("; ", "prescription_categories"))
        .withColumn("atc_level1_label", atc_level1_map[F.col("atc_level1")])
        .withColumn("substance_count", F.coalesce("substance_count", F.lit(0)))
        .withColumn("type_generique_label", F.coalesce("type_generique_label", F.lit("Non classé")))
        .withColumn("is_generic", F.coalesce("is_generic", F.lit(False)))
        .withColumn("is_princeps", F.coalesce("is_princeps", F.lit(False)))
        .withColumn("is_prescription_required", F.coalesce("is_prescription_required", F.lit(False)))
        .withColumn("is_mitm", F.coalesce("is_mitm", F.lit(False)))
        .drop("voies_administration_array", "substances_array", "prescription_categories")
        .withColumn("drug_sk", generate_surrogate_key("cis_code"))
    )
    return append_unknown_member(
        spark_session, df_dim_drug, "drug_sk",
        denomination="Médicament inconnu", cis_code=UNKNOWN_CIS_CODE, titulaire="Inconnu",
        type_generique_label="Non classé", is_inferred=False, is_generic=False, is_princeps=False,
        is_prescription_required=False, is_commercialisee=False, substance_count=0, nombre_voies=0, is_mitm=False,
    )


def build_dim_presentation(
    spark_session: SparkSession,
    df_presentations: DataFrame,
    df_ruptures: DataFrame | None = None,
    df_price_history: DataFrame | None = None,
) -> DataFrame:
    """Dimension Présentation (grain CIP13, avec membres inférés ruptures/historique)."""
    df_dim_pres = df_presentations.select(
        "cip13", "cis_code", "cip7", "libelle_presentation", "statut_administratif",
        "etat_commercialisation", "is_commercialisee", "date_declaration_commercialisation",
        "agrement_collectivites", "cip13_valid",
    ).withColumn("is_inferred", F.lit(False))

    df_dim_pres = _append_inferred_presentations(
        df_dim_pres, df_ruptures, "Présentation hors fichier (signalée par l'ANSM)"
    )
    df_dim_pres = _append_inferred_presentations(
        df_dim_pres, df_price_history, "Présentation retirée du fichier actuel (présente dans l'historique)"
    )

    df_dim_pres = df_dim_pres.withColumn("presentation_sk", generate_surrogate_key("cip13"))
    return append_unknown_member(
        spark_session, df_dim_pres, "presentation_sk",
        cip13=UNKNOWN_CIP13_CODE, libelle_presentation="Présentation inconnue",
        is_commercialisee=False, cip13_valid=False, is_inferred=False,
    )


def build_dim_generic_group(spark_session: SparkSession, df_generics: DataFrame, df_specialties: DataFrame) -> DataFrame:
    """Dimension Groupe Générique."""
    princeps_window = Window.partitionBy("generic_group_id").orderBy(F.col("cis_code").asc())
    df_princeps = (
        df_generics.filter("is_princeps")
        .withColumn("_rn", F.row_number().over(princeps_window))
        .filter("_rn = 1")
        .select("generic_group_id", F.col("cis_code").alias("princeps_cis_code"))
        .join(
            df_specialties.select(
                F.col("cis_code").alias("princeps_cis_code"),
                F.col("denomination").alias("princeps_denomination"),
            ),
            "princeps_cis_code",
            "left",
        )
    )
    df_group_metrics = df_generics.groupBy("generic_group_id").agg(
        F.min("libelle_groupe").alias("libelle_groupe"),
        F.countDistinct("cis_code").alias("total_members"),
        F.countDistinct(F.when(F.col("is_generic"), F.col("cis_code"))).alias("generic_count"),
        F.countDistinct(F.when(F.col("is_princeps"), F.col("cis_code"))).alias("princeps_count"),
    )
    df_group = (
        df_group_metrics.join(df_princeps, "generic_group_id", "left")
        .withColumn("has_generics", F.col("generic_count") > 0)
        .withColumn("group_sk", generate_surrogate_key("generic_group_id"))
        .select(
            "generic_group_id", "libelle_groupe", "total_members", "generic_count",
            "princeps_count", "princeps_cis_code", "princeps_denomination", "has_generics", "group_sk",
        )
    )
    return append_unknown_member(
        spark_session, df_group, "group_sk",
        generic_group_id="0", libelle_groupe="Hors groupe générique",
        total_members=0, generic_count=0, princeps_count=0, has_generics=False,
    )


def build_dim_substance(spark_session: SparkSession, df_compositions: DataFrame) -> DataFrame:
    """Dimension Substance Active."""
    freq_window = Window.partitionBy("code_substance").orderBy(F.col("n").desc(), F.col("denomination_substance").asc())
    df_frequent_names = (
        df_compositions.filter("code_substance IS NOT NULL AND denomination_substance IS NOT NULL")
        .groupBy("code_substance", "denomination_substance")
        .agg(F.count("*").alias("n"))
        .withColumn("_rn", F.row_number().over(freq_window))
        .filter("_rn = 1")
        .select("code_substance", "denomination_substance")
    )
    df_drug_counts = df_compositions.filter("code_substance IS NOT NULL").groupBy("code_substance").agg(
        F.countDistinct("cis_code").alias("drug_count"),
        F.countDistinct(F.when(F.col("is_active_substance"), F.col("cis_code"))).alias("drug_count_as_active"),
    )
    df_substance = (
        df_frequent_names.join(df_drug_counts, "code_substance", "right")
        .withColumn("substance_sk", generate_surrogate_key("code_substance"))
        .withColumn("denomination_substance", F.coalesce("denomination_substance", F.lit("Substance sans libellé")))
    )
    return append_unknown_member(
        spark_session, df_substance, "substance_sk",
        code_substance="XXXXX", denomination_substance="Substance inconnue",
        drug_count=0, drug_count_as_active=0,
    )


def build_dim_atc(spark_session: SparkSession, df_mitm: DataFrame) -> DataFrame:
    """Dimension Classification ATC (médicaments MITM)."""
    atc_level1_map = F.create_map(*[item for code, label in ATC_LEVEL1.items() for item in (F.lit(code), F.lit(label))])
    df_atc = (
        df_mitm.filter("atc_valid")
        .select("code_atc", "atc_level1", "atc_level2", "atc_level3", "atc_level4", "atc_level5")
        .dropDuplicates(["code_atc"])
        .withColumn("atc_level1_label", atc_level1_map[F.col("atc_level1")])
        .withColumn(
            "atc_depth",
            F.when(F.length("code_atc") == 7, 5)
            .when(F.length("code_atc") == 5, 4)
            .when(F.length("code_atc") == 4, 3)
            .otherwise(2),
        )
        .withColumn("atc_sk", generate_surrogate_key("code_atc"))
    )
    return append_unknown_member(spark_session, df_atc, "atc_sk", code_atc="XXXXXXX", atc_level1_label="ATC inconnu", atc_depth=0)


def build_dim_status(spark_session: SparkSession) -> DataFrame:
    """Dimension Statut ANSM de disponibilité."""
    status_rows = [
        (code, label, code in ACTIVE_STATUS_CODES, STATUT_SEVERITY[code])
        for code, label in STATUT_LABELS.items()
    ]
    df_status = spark_session.createDataFrame(
        status_rows, "status_code int, status_label string, is_active boolean, severity int"
    ).withColumn("status_sk", generate_surrogate_key("status_code"))
    return append_unknown_member(spark_session, df_status, "status_sk", status_label="Statut inconnu", is_active=False, severity=0)


def build_dim_opinion_level(spark_session: SparkSession, df_smr: DataFrame | None, df_asmr: DataFrame | None) -> DataFrame:
    """Dimension Niveau d'Avis HAS (SMR & ASMR) avec garde IndexError."""
    opinion_extracts = []
    if df_smr is not None:
        opinion_extracts.append(
            df_smr.select(F.lit("SMR").alias("opinion_type"), F.col("valeur_smr").alias("level_label"), F.col("smr_rank").alias("level_rank"))
        )
    if df_asmr is not None:
        opinion_extracts.append(
            df_asmr.select(F.lit("ASMR").alias("opinion_type"), F.col("valeur_asmr").alias("level_label"), F.col("asmr_rank").alias("level_rank"))
        )
    if not opinion_extracts:
        raise ValueError("build_dim_opinion_level requiert au moins un DataFrame SMR ou ASMR")

    df_opinions_union = opinion_extracts[0]
    for extra_extract in opinion_extracts[1:]:
        df_opinions_union = df_opinions_union.unionByName(extra_extract)

    df_opinion_level = (
        df_opinions_union.filter("level_label IS NOT NULL")
        .groupBy("opinion_type", "level_label")
        .agg(F.max("level_rank").alias("level_rank"))
        .withColumn("level_sk", generate_surrogate_key("opinion_type", "level_label"))
        .withColumn("is_ranked", F.col("level_rank").isNotNull())
    )
    return append_unknown_member(
        spark_session, df_opinion_level, "level_sk",
        opinion_type="?", level_label="Niveau inconnu", is_ranked=False,
    )


def build_dim_time(
    spark_session: SparkSession,
    start_date: str | date = CALENDAR_START_DATE,
    end_date: str | date = CALENDAR_END_DATE,
) -> DataFrame:
    """Dimension Calendrier continue (validée ISO pour prévenir toute injection SQL)."""
    parsed_start = start_date if isinstance(start_date, date) else date.fromisoformat(str(start_date))
    parsed_end = end_date if isinstance(end_date, date) else date.fromisoformat(str(end_date))
    if parsed_start > parsed_end:
        raise ValueError(f"Plage calendrier inversée : {parsed_start} > {parsed_end}")

    df_dates = spark_session.sql(
        f"SELECT explode(sequence(DATE'{parsed_start.isoformat()}', DATE'{parsed_end.isoformat()}', interval 1 day)) AS date_value"
    )
    iso_day_of_week = F.expr("pmod(dayofweek(date_value) - 2, 7) + 1")
    return (
        df_dates.withColumn("time_sk", to_date_surrogate_key(F.col("date_value")))
        .withColumn("year", F.year("date_value"))
        .withColumn("month", F.month("date_value"))
        .withColumn("day", F.dayofmonth("date_value"))
        .withColumn("quarter", F.quarter("date_value"))
        .withColumn("week_of_year", F.weekofyear("date_value"))
        .withColumn("iso_year", F.expr("extract(YEAROFWEEK FROM date_value)"))
        .withColumn("day_of_week", iso_day_of_week)
        .withColumn("day_name", F.element_at(F.array(*[F.lit(name) for name in DAYS_FR]), iso_day_of_week))
        .withColumn("month_name", F.element_at(F.array(*[F.lit(name) for name in MONTHS_FR]), F.col("month")))
        .withColumn("year_month", F.date_format("date_value", "yyyy-MM"))
        .withColumn("year_quarter", F.concat_ws("-T", F.col("year").cast("string"), F.col("quarter").cast("string")))
        .withColumn("is_weekend", F.col("day_of_week") >= 6)
        .withColumn("is_month_start", F.col("day") == 1)
        .withColumn("is_month_end", F.col("date_value") == F.last_day("date_value"))
        .withColumn("is_year_start", (F.col("month") == 1) & (F.col("day") == 1))
        .select(
            "time_sk", "date_value", "year", "quarter", "month", "day", "week_of_year",
            "iso_year", "day_of_week", "day_name", "month_name", "year_month",
            "year_quarter", "is_weekend", "is_month_start", "is_month_end", "is_year_start",
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6 · Construction des 5 tables de faits et 4 tables d'indicateurs KPI

# CELL ********************

# ==============================================================================
# CONSTRUCTION DES 5 TABLES DE FAITS & 4 TABLES D'INDICATEURS KPIs
# ==============================================================================
def build_fact_presentation_price(df_cip_history: DataFrame, df_dim_drug: DataFrame, df_dim_pres: DataFrame) -> DataFrame:
    """Fait Prix & Remboursement (grain CIP13 × snapshot_date)."""
    keys_drug = df_dim_drug.select("cis_code", "drug_sk")
    keys_pres = df_dim_pres.select("cip13", "presentation_sk")
    return (
        df_cip_history.join(keys_drug, "cis_code", "left")
        .join(keys_pres, "cip13", "left")
        .select(
            coalesce_foreign_key(F.col("drug_sk")).alias("drug_sk"),
            coalesce_foreign_key(F.col("presentation_sk")).alias("presentation_sk"),
            to_calendar_time_key(F.col("snapshot_date")).alias("time_sk"),
            "cis_code", "cip13", "snapshot_date", "prix_hors_honoraires", "prix_avec_honoraires",
            "taux_remboursement", "honoraires_dispensation", "has_price", "is_reimbursed",
        )
    )


def build_fact_shortage(
    df_ruptures: DataFrame,
    df_dim_drug: DataFrame,
    df_dim_pres: DataFrame,
    df_dim_status: DataFrame,
) -> DataFrame:
    """Fait Signalements de Ruptures & Tensions ANSM."""
    keys_drug = df_dim_drug.select("cis_code", "drug_sk")
    keys_pres = df_dim_pres.select("cip13", "presentation_sk")
    keys_status = df_dim_status.select(F.col("status_code").alias("code_statut"), "status_sk")
    return (
        df_ruptures.join(keys_drug, "cis_code", "left")
        .join(keys_pres, "cip13", "left")
        .join(keys_status, "code_statut", "left")
        .select(
            F.concat_ws("|", "cis_code", "cip13_key").alias("shortage_key"),
            coalesce_foreign_key(F.col("drug_sk")).alias("drug_sk"),
            coalesce_foreign_key(F.col("presentation_sk")).alias("presentation_sk"),
            coalesce_foreign_key(F.col("status_sk")).alias("status_sk"),
            to_calendar_time_key(F.col("date_debut")).alias("time_sk"),
            to_calendar_time_key(F.col("date_fin")).alias("time_end_sk"),
            "cis_code", "cip13", F.col("cip13").isNull().alias("is_cis_level"),
            "date_debut", "date_fin", "date_mise_a_jour", "code_statut", "classification", "is_active",
            "duration_days", "duration_bucket", "days_since_update", "remise_depassee", "lien_ansm", "as_of_date",
        )
    )


def build_fact_has_opinion(
    df_smr: DataFrame | None,
    df_asmr: DataFrame | None,
    df_dim_drug: DataFrame,
    df_dim_level: DataFrame,
    df_ct_links: DataFrame | None = None,
) -> DataFrame:
    """Fait Avis HAS SMR & ASMR (avec garde IndexError)."""
    opinion_frames = []
    for opinion_type, df_source in (("SMR", df_smr), ("ASMR", df_asmr)):
        if df_source is None:
            continue
        kind_lower = opinion_type.lower()
        opinion_frames.append(
            df_source.select(
                "cis_code", "code_dossier_has", "motif_evaluation", "date_avis",
                F.lit(opinion_type).alias("opinion_type"),
                F.col(f"valeur_{kind_lower}").alias("level_label"),
                F.col(f"{kind_lower}_rank").alias("level_rank"),
                F.col(f"libelle_{kind_lower}").alias("opinion_text"),
                "is_latest_avis",
            )
        )
    if not opinion_frames:
        raise ValueError("build_fact_has_opinion requiert au moins un DataFrame SMR ou ASMR")

    df_opinions_all = opinion_frames[0]
    for extra_frame in opinion_frames[1:]:
        df_opinions_all = df_opinions_all.unionByName(extra_frame)

    df_joined = (
        df_opinions_all.join(df_dim_drug.select("cis_code", "drug_sk"), "cis_code", "left")
        .join(df_dim_level.select("opinion_type", "level_label", "level_sk"), ["opinion_type", "level_label"], "left")
    )
    if df_ct_links is not None:
        df_joined = df_joined.join(
            df_ct_links.select("code_dossier_has", F.col("lien_page_ct").alias("ct_url")),
            "code_dossier_has",
            "left",
        )
    else:
        df_joined = df_joined.withColumn("ct_url", F.lit(None).cast("string"))

    return df_joined.select(
        F.concat_ws("|", "cis_code", "code_dossier_has", "opinion_type", F.coalesce("level_label", F.lit("?"))).alias("opinion_key"),
        coalesce_foreign_key(F.col("drug_sk")).alias("drug_sk"),
        coalesce_foreign_key(F.col("level_sk")).alias("level_sk"),
        to_calendar_time_key(F.col("date_avis")).alias("time_sk"),
        "cis_code", "code_dossier_has", "opinion_type", "motif_evaluation", "date_avis",
        "level_label", "level_rank", "is_latest_avis", "opinion_text", "ct_url",
    )


def build_fact_important_info(df_info: DataFrame, df_dim_drug: DataFrame) -> DataFrame:
    """Fait Informations Importantes de Sécurité ANSM."""
    return (
        df_info.join(df_dim_drug.select("cis_code", "drug_sk"), "cis_code", "left")
        .select(
            F.concat_ws("|", "cis_code", F.col("date_debut").cast("string"), F.coalesce(F.col("date_fin").cast("string"), F.lit("")), "info_url").alias("info_key"),
            coalesce_foreign_key(F.col("drug_sk")).alias("drug_sk"),
            to_calendar_time_key(F.col("date_debut")).alias("time_sk"),
            to_calendar_time_key(F.col("date_fin")).alias("time_end_sk"),
            "cis_code", "info_type", "info_title", "info_url", "date_debut", "date_fin",
            "is_active", "duration_days", "days_remaining", "as_of_date",
        )
    )


def build_fact_composition(df_compositions: DataFrame, df_dim_drug: DataFrame, df_dim_substance: DataFrame) -> DataFrame:
    """Fait Composition (relation spécialité × substance)."""
    return (
        df_compositions.join(df_dim_drug.select("cis_code", "drug_sk"), "cis_code", "left")
        .join(df_dim_substance.select("code_substance", "substance_sk"), "code_substance", "left")
        .select(
            F.concat_ws(
                "|",
                "cis_code",
                F.coalesce("designation_element", F.lit("")),
                F.coalesce("code_substance", F.lit("")),
                F.coalesce("nature_composant", F.lit("")),
            ).alias("composition_key"),
            coalesce_foreign_key(F.col("drug_sk")).alias("drug_sk"),
            coalesce_foreign_key(F.col("substance_sk")).alias("substance_sk"),
            "cis_code", "designation_element", "code_substance", "nature_composant",
            "is_active_substance", "dosage_numeric", "dosage_unit", "reference_dosage", "numero_liaison_sa_ft",
        )
    )


def build_kpi_shortage_risk_score(
    df_fact_shortage: DataFrame,
    df_dim_drug: DataFrame,
    as_of_date: date,
    weights: dict | None = None,
    thresholds: dict | None = None,
) -> DataFrame:
    """KPI Score de risque de rupture par médicament (calcul O(N) fenêtré sans produit cartésien)."""
    risk_weights = {**DEFAULT_RISK_WEIGHTS, **(weights or {})}
    risk_thresholds = {**DEFAULT_RISK_THRESHOLDS, **(thresholds or {})}
    as_of_lit = F.lit(as_of_date.isoformat()).cast("date")

    df_shortage_stats = df_fact_shortage.groupBy("cis_code").agg(
        F.count("*").alias("shortage_count"),
        F.sum(F.greatest(F.col("duration_days"), F.lit(0))).alias("total_shortage_days"),
        F.avg("duration_days").alias("avg_duration_days"),
        F.max("duration_days").alias("max_duration_days"),
        F.sum(F.col("is_active").cast("int")).alias("active_shortages"),
        F.max("date_debut").alias("last_shortage_start"),
    )

    df_active_cis = df_fact_shortage.filter("is_active").select("cis_code").distinct().withColumn("_in_shortage", F.lit(True))
    df_group_members = (
        df_dim_drug.filter(f"cis_code <> '{UNKNOWN_CIS_CODE}'")
        .select("cis_code", "generic_group_id", "is_commercialisee")
        .join(df_active_cis, "cis_code", "left")
        .withColumn("_is_available_alt", F.col("is_commercialisee") & F.col("_in_shortage").isNull())
    )

    group_window = Window.partitionBy("generic_group_id")
    df_alternatives = (
        df_group_members.filter(F.col("generic_group_id").isNotNull())
        .withColumn("_group_available_total", F.sum(F.col("_is_available_alt").cast("int")).over(group_window))
        .withColumn("alternatives_available", F.col("_group_available_total") - F.when(F.col("_is_available_alt"), 1).otherwise(0))
        .select("cis_code", "alternatives_available")
    )

    df_drug_attrs = df_dim_drug.select(
        "drug_sk", "cis_code", "denomination", "substance_active_primary",
        "is_mitm", "generic_group_id", "atc_level1", "atc_level1_label",
    )
    df_risk = (
        df_shortage_stats.join(df_drug_attrs, "cis_code", "left")
        .join(df_alternatives, "cis_code", "left")
        .withColumn("drug_sk", coalesce_foreign_key(F.col("drug_sk")))
        .withColumn("denomination", F.coalesce("denomination", F.lit("Médicament inconnu")))
        .withColumn("is_mitm", F.coalesce("is_mitm", F.lit(False)))
        .withColumn("in_generic_group", F.col("generic_group_id").isNotNull())
        .withColumn("alternatives_available", F.coalesce("alternatives_available", F.lit(0)))
        .withColumn("no_alternative", F.col("alternatives_available") == 0)
        .withColumn("days_since_last_shortage", F.datediff(as_of_lit, F.col("last_shortage_start")))
        .withColumn(
            "_recency_bonus",
            F.when(F.col("days_since_last_shortage") < 0, 0)
            .when(F.col("days_since_last_shortage") < 180, risk_weights["recent_180"])
            .when(F.col("days_since_last_shortage") < 365, risk_weights["recent_365"])
            .otherwise(0),
        )
        .withColumn(
            "risk_score",
            F.round(
                F.col("shortage_count") * risk_weights["per_shortage"]
                + F.least(F.coalesce("total_shortage_days", F.lit(0)), F.lit(risk_weights["days_cap"])) * risk_weights["per_day"]
                + F.col("active_shortages") * risk_weights["per_active"]
                + F.col("_recency_bonus")
                + F.when(F.col("is_mitm"), risk_weights["mitm"]).otherwise(0)
                + F.when(F.col("no_alternative") & (F.col("active_shortages") > 0), risk_weights["no_alternative"]).otherwise(0),
                1,
            ),
        )
        .withColumn(
            "risk_category",
            F.when(F.col("risk_score") >= risk_thresholds["Critique"], "Critique")
            .when(F.col("risk_score") >= risk_thresholds["Élevé"], "Élevé")
            .when(F.col("risk_score") >= risk_thresholds["Modéré"], "Modéré")
            .otherwise("Faible"),
        )
        .withColumn("as_of_date", as_of_lit)
        .drop("_recency_bonus")
    )
    return df_risk.select(
        "cis_code", "drug_sk", "denomination", "substance_active_primary", "atc_level1", "atc_level1_label",
        "is_mitm", "in_generic_group", "alternatives_available", "no_alternative", "shortage_count",
        "total_shortage_days", F.round("avg_duration_days", 1).alias("avg_duration_days"), "max_duration_days",
        "active_shortages", "last_shortage_start", "days_since_last_shortage", "risk_score", "risk_category", "as_of_date",
    )


def build_kpi_generic_price_gap(df_generics: DataFrame, df_presentations: DataFrame, df_dim_group: DataFrame) -> DataFrame:
    """KPI Écart de prix princeps vs générique par groupe générique."""
    df_priced = df_presentations.filter(
        "is_commercialisee AND prix_hors_honoraires IS NOT NULL AND prix_hors_honoraires > 0"
    ).select("cis_code", F.col("prix_hors_honoraires").cast("double").alias("prix"))

    df_joined = df_generics.select("generic_group_id", "cis_code", "is_generic", "is_princeps").join(df_priced, "cis_code", "inner")
    df_aggregated = (
        df_joined.groupBy("generic_group_id")
        .agg(
            F.avg(F.when(F.col("is_princeps"), F.col("prix"))).alias("avg_princeps_price"),
            F.avg(F.when(F.col("is_generic"), F.col("prix"))).alias("avg_generic_price"),
            F.countDistinct(F.when(F.col("is_princeps"), F.col("cis_code"))).alias("princeps_priced"),
            F.countDistinct(F.when(F.col("is_generic"), F.col("cis_code"))).alias("generic_priced"),
        )
        .withColumn(
            "gap_pct",
            F.when(
                F.col("avg_princeps_price") > 0,
                F.round((F.col("avg_princeps_price") - F.col("avg_generic_price")) / F.col("avg_princeps_price") * 100, 2),
            ),
        )
    )
    return (
        df_aggregated.join(df_dim_group.select("generic_group_id", "group_sk", "libelle_groupe"), "generic_group_id", "left")
        .select(
            "generic_group_id", "libelle_groupe", "princeps_priced", "generic_priced",
            F.round("avg_princeps_price", 4).alias("avg_princeps_price"),
            F.round("avg_generic_price", 4).alias("avg_generic_price"),
            "gap_pct", coalesce_foreign_key(F.col("group_sk")).alias("group_sk"),
        )
    )


def build_kpi_price_movement(df_cip_history: DataFrame) -> DataFrame:
    """KPI Variation de prix entre les deux dernières photos d'une présentation CIP13."""
    snapshot_window = Window.partitionBy("cip13").orderBy(F.col("snapshot_date").desc())
    df_last_two = df_cip_history.withColumn("_rn", F.row_number().over(snapshot_window)).filter("_rn <= 2")
    df_current = df_last_two.filter("_rn = 1").select(
        "cip13", "cis_code", F.col("snapshot_date").alias("snapshot_current"), F.col("prix_hors_honoraires").alias("price_current")
    )
    df_previous = df_last_two.filter("_rn = 2").select(
        "cip13", F.col("snapshot_date").alias("snapshot_previous"), F.col("prix_hors_honoraires").alias("price_previous")
    )
    return (
        df_current.join(df_previous, "cip13", "inner")
        .withColumn(
            "price_delta",
            F.when(
                F.col("price_current").isNotNull() & F.col("price_previous").isNotNull(),
                (F.col("price_current") - F.col("price_previous")).cast("decimal(12,2)"),
            ),
        )
        .withColumn(
            "price_delta_pct",
            F.when(
                F.col("price_previous") > 0,
                F.round(F.col("price_delta") / F.col("price_previous") * 100, 2).cast("decimal(12,2)"),
            ),
        )
        .withColumn(
            "movement",
            F.when(F.col("price_delta").isNull(), "Non comparable")
            .when(F.col("price_delta") < 0, "Baisse")
            .when(F.col("price_delta") > 0, "Hausse")
            .otherwise("Stable"),
        )
        .select(
            "cip13", "cis_code", "snapshot_previous", "snapshot_current",
            "price_previous", "price_current", "price_delta", "price_delta_pct", "movement",
        )
    )


def build_kpi_shortage_monthly(df_fact_shortage: DataFrame, df_dim_time: DataFrame) -> DataFrame:
    """KPI Mensuel : signalements débutés par mois et statut ANSM."""
    df_months = df_dim_time.filter("day = 1").select("year_month", "year", "month", F.col("date_value").alias("month_start"))
    df_monthly_agg = (
        df_fact_shortage.withColumn("year_month", F.date_format("date_debut", "yyyy-MM"))
        .groupBy("year_month", "code_statut", "classification")
        .agg(
            F.count("*").alias("shortages_started"),
            F.sum(F.col("is_active").cast("int")).alias("still_active"),
            F.sum(F.col("is_cis_level").cast("int")).alias("cis_level_signals"),
        )
    )
    return df_monthly_agg.join(df_months, "year_month", "inner").select(
        "year_month", "year", "month", "month_start", "code_statut",
        "classification", "shortages_started", "still_active", "cis_level_signals",
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7 · Orchestration couche Gold (cache sélectif + écriture + OPTIMIZE + gouvernance)

# CELL ********************

# ==============================================================================
# ORCHESTRATION COUCHE GOLD (CACHE SÉLECTIF + ÉCRITURE + OPTIMIZE + GOUVERNANCE)
# ==============================================================================
def _write_gold_table(
    table_io_client: TableIO,
    table_name: str,
    df_gold: DataFrame,
    current_run_id: str,
    results: list[StepResult],
    source_label: str,
    write_mode: str = "overwrite",
    upsert_keys: list[str] | None = None,
) -> None:
    """Écrit une table Gold avec colonnes d'audit et capture les métriques résultantes."""
    try:
        df_audited = append_gold_audit(df_gold, current_run_id)
        if write_mode == "upsert":
            table_io_client.upsert(df_audited, table_name, upsert_keys or [])
        else:
            table_io_client.overwrite(df_audited, table_name)
        df_persisted = table_io_client.read(table_name)
        results.append(StepResult(source_label, table_name, "SUCCESS", None, df_persisted.count(), len(df_persisted.columns)))
    except Exception as exc:  # noqa: BLE001
        gold_logger.exception("Erreur Gold sur la table %s", table_name)
        results.append(StepResult(source_label, table_name, "FAILED", f"{type(exc).__name__}: {exc}"))


def execute_gold_pipeline(
    spark_session: SparkSession,
    table_io_client: TableIO,
    current_run_id: str,
    as_of_date: date,
    load_mode: str = "incremental",
) -> dict:
    """Construit la galaxie Gold (8 dimensions, 5 faits, 4 KPIs) avec cache maîtrisé."""
    normalized_mode = str(load_mode).strip().lower()
    if normalized_mode not in ("incremental", "full"):
        raise ValueError(f"mode doit valoir 'incremental' ou 'full', reçu {load_mode!r}")

    start_time = time.time()
    step_results: list[StepResult] = []
    cached_frames: list[DataFrame] = []

    def read_cached(table_name: str) -> DataFrame:
        frame = table_io_client.read(table_name).cache()
        cached_frames.append(frame)
        return frame

    def read_optional_silver(source_key: str) -> Optional[DataFrame]:
        table_name = SILVER_TABLES[source_key]
        return table_io_client.read(table_name) if table_io_client.exists(table_name) else None

    try:
        df_specialties = read_cached(SILVER_TABLES["specialties"])
        df_presentations = read_cached(SILVER_TABLES["presentations"])
        df_ruptures = read_cached(SILVER_TABLES["ruptures"])
        df_compositions = read_cached(SILVER_TABLES["compositions"])
        df_generics = read_cached(SILVER_TABLES["generics"])
        df_conditions = table_io_client.read(SILVER_TABLES["conditions"])
        df_full_price_history = read_cached(SILVER_PRICE_HISTORY_TABLE)

        df_smr = read_optional_silver("smr")
        df_asmr = read_optional_silver("asmr")
        df_mitm = read_optional_silver("mitm")
        df_important_info = read_optional_silver("important_info")
        df_ct_links = read_optional_silver("ct_links")

        other_cis_frames = [
            df.select("cis_code")
            for df in (df_presentations, df_ruptures, df_mitm, df_important_info, df_generics, df_compositions, df_smr, df_asmr)
            if df is not None
        ]

        # 1. Construction et écriture des 8 Dimensions
        dimensions_plan = [
            ("dim_drug", build_dim_drug(spark_session, df_specialties, df_compositions, df_generics, df_conditions, df_smr, df_asmr, df_mitm, other_cis_frames)),
            ("dim_presentation", build_dim_presentation(spark_session, df_presentations, df_ruptures, df_full_price_history)),
            ("dim_generic_group", build_dim_generic_group(spark_session, df_generics, df_specialties)),
            ("dim_substance", build_dim_substance(spark_session, df_compositions)),
            ("dim_atc", build_dim_atc(spark_session, df_mitm) if df_mitm is not None else None),
            ("dim_status", build_dim_status(spark_session)),
            ("dim_opinion_level", build_dim_opinion_level(spark_session, df_smr, df_asmr) if (df_smr is not None or df_asmr is not None) else None),
            ("dim_time", build_dim_time(spark_session)),
        ]
        for dim_key, df_dimension in dimensions_plan:
            if df_dimension is None:
                step_results.append(StepResult(dim_key, GOLD_TABLES[dim_key], "SKIPPED", "Source Silver absente"))
                continue
            _write_gold_table(table_io_client, GOLD_TABLES[dim_key], df_dimension, current_run_id, step_results, dim_key)

        df_dim_drug = read_cached(GOLD_TABLES["dim_drug"])
        df_dim_pres = read_cached(GOLD_TABLES["dim_presentation"])
        df_dim_time = read_cached(GOLD_TABLES["dim_time"])

        # 2. Construction et écriture des 5 Faits
        df_price_slice = df_full_price_history
        if normalized_mode == "incremental":
            latest_snapshot_row = df_full_price_history.agg(F.max("snapshot_date")).first()
            latest_snapshot_date = latest_snapshot_row[0] if latest_snapshot_row else None
            if latest_snapshot_date is not None:
                df_price_slice = df_full_price_history.filter(F.col("snapshot_date") == F.lit(latest_snapshot_date))
            else:
                df_price_slice = df_full_price_history.limit(0)

        _write_gold_table(
            table_io_client,
            GOLD_TABLES["fact_presentation_price"],
            build_fact_presentation_price(df_price_slice, df_dim_drug, df_dim_pres),
            current_run_id,
            step_results,
            "fact_price",
            write_mode="upsert" if normalized_mode == "incremental" else "overwrite",
            upsert_keys=["cip13", "snapshot_date"],
        )

        _write_gold_table(
            table_io_client,
            GOLD_TABLES["fact_shortage"],
            build_fact_shortage(df_ruptures, df_dim_drug, df_dim_pres, table_io_client.read(GOLD_TABLES["dim_status"])),
            current_run_id,
            step_results,
            "fact_shortage",
        )

        if df_smr is not None or df_asmr is not None:
            _write_gold_table(
                table_io_client,
                GOLD_TABLES["fact_has_opinion"],
                build_fact_has_opinion(df_smr, df_asmr, df_dim_drug, table_io_client.read(GOLD_TABLES["dim_opinion_level"]), df_ct_links),
                current_run_id,
                step_results,
                "fact_has_opinion",
            )
        else:
            step_results.append(StepResult("fact_has_opinion", GOLD_TABLES["fact_has_opinion"], "SKIPPED", "Sources Silver SMR/ASMR absentes"))

        if df_important_info is not None:
            _write_gold_table(
                table_io_client,
                GOLD_TABLES["fact_important_info"],
                build_fact_important_info(df_important_info, df_dim_drug),
                current_run_id,
                step_results,
                "fact_important_info",
            )
        else:
            step_results.append(StepResult("fact_important_info", GOLD_TABLES["fact_important_info"], "SKIPPED", "Source Silver informations importantes absente"))

        _write_gold_table(
            table_io_client,
            GOLD_TABLES["fact_composition"],
            build_fact_composition(df_compositions, df_dim_drug, table_io_client.read(GOLD_TABLES["dim_substance"])),
            current_run_id,
            step_results,
            "fact_composition",
        )

        # 3. Construction et écriture des 4 Tables KPIs
        df_fact_shortage_persisted = table_io_client.read(GOLD_TABLES["fact_shortage"])
        _write_gold_table(
            table_io_client,
            GOLD_TABLES["kpi_shortage_risk_score"],
            build_kpi_shortage_risk_score(df_fact_shortage_persisted, df_dim_drug, as_of_date),
            current_run_id,
            step_results,
            "kpi_shortage_risk_score",
        )
        _write_gold_table(
            table_io_client,
            GOLD_TABLES["kpi_generic_price_gap"],
            build_kpi_generic_price_gap(df_generics, df_presentations, table_io_client.read(GOLD_TABLES["dim_generic_group"])),
            current_run_id,
            step_results,
            "kpi_generic_price_gap",
        )
        _write_gold_table(
            table_io_client,
            GOLD_TABLES["kpi_price_movement"],
            build_kpi_price_movement(df_full_price_history),
            current_run_id,
            step_results,
            "kpi_price_movement",
        )
        _write_gold_table(
            table_io_client,
            GOLD_TABLES["kpi_shortage_monthly"],
            build_kpi_shortage_monthly(df_fact_shortage_persisted, df_dim_time),
            current_run_id,
            step_results,
            "kpi_shortage_monthly",
        )
    finally:
        for cached_df in cached_frames:
            cached_df.unpersist()

    # 4. Optimisation Delta & Métadonnées de gouvernance
    for table_name, zorder_columns in GOLD_ZORDER_COLUMNS.items():
        try:
            if table_io_client.exists(table_name):
                table_io_client.optimize(table_name, zorder_columns)
        except Exception as exc:  # noqa: BLE001
            gold_logger.warning("OPTIMIZE ignoré pour %s : %s", table_name, exc)

    for table_name, description in GOLD_TABLE_DESCRIPTIONS.items():
        try:
            if table_io_client.exists(table_name):
                table_io_client.set_properties(
                    table_name,
                    {"description": description, "layer": "gold", "schema_version": SCHEMA_VERSION},
                )
        except Exception as exc:  # noqa: BLE001
            gold_logger.warning("Métadonnées TBLPROPERTIES ignorées pour %s : %s", table_name, exc)

    critical_tables = {"dim_drug", "fact_price", "fact_shortage"}
    return summarize_results(
        step_results,
        critical_tables,
        current_run_id,
        time.time() - start_time,
        extra={"mode": normalized_mode, "as_of": as_of_date.isoformat(), "results": [res.to_dict() for res in step_results]},
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8 · Exécution
# 
# Résolution du contexte (format de table, landing, date de référence), bannière de traçabilité puis appel de la fonction d'orchestration.

# CELL ********************

table_io = create_table_io(spark)
reference_date = parse_run_date(run_date)
init_notebook_banner("nb_gold_build_galaxy", run_id, reference_date, table_io.fmt)

payload = execute_gold_pipeline(
    spark_session=spark,
    table_io_client=table_io,
    current_run_id=run_id,
    as_of_date=reference_date,
    load_mode=mode,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9 · Rapport d'exécution et sortie JSON
# 
# Affiche la synthèse puis renvoie le résumé JSON au pipeline via `notebookutils.notebook.exit` (lisible dans la sortie de l'activité).

# CELL ********************

render_execution_report(
    payload,
    columns=["source", "table", "status", "rows", "columns"],
    summary_line=(
        f"🏆 Constellation Gold : {payload.get('succeeded', 0)}/{payload.get('total', 0)} tables générées "
        f"({payload.get('total_rows', 0):,} lignes cumulées) en {payload.get('duration_seconds', 0)} s"
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
# *HealthTek — nb_gold_build_galaxy · documentation générée selon le standard du projet.*
