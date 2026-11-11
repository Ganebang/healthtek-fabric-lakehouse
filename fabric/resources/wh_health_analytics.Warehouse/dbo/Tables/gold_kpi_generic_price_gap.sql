CREATE TABLE [dbo].[gold_kpi_generic_price_gap] (
    [generic_group_id]   VARCHAR (8000) NULL,
    [libelle_groupe]     VARCHAR (8000) NULL,
    [princeps_priced]    BIGINT         NULL,
    [generic_priced]     BIGINT         NULL,
    [avg_princeps_price] FLOAT (53)     NULL,
    [avg_generic_price]  FLOAT (53)     NULL,
    [gap_pct]            FLOAT (53)     NULL,
    [group_sk]           BIGINT         NULL,
    [_gold_created_at]   DATETIME2 (6)  NULL,
    [_pipeline_run_id]   VARCHAR (8000) NULL
);


GO