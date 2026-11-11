CREATE TABLE [dbo].[gold_fact_composition] (
    [composition_key]      VARCHAR (8000) NULL,
    [drug_sk]              BIGINT         NULL,
    [substance_sk]         BIGINT         NULL,
    [cis_code]             VARCHAR (8000) NULL,
    [designation_element]  VARCHAR (8000) NULL,
    [code_substance]       VARCHAR (8000) NULL,
    [nature_composant]     VARCHAR (8000) NULL,
    [is_active_substance]  BIT            NULL,
    [dosage_numeric]       FLOAT (53)     NULL,
    [dosage_unit]          VARCHAR (8000) NULL,
    [reference_dosage]     VARCHAR (8000) NULL,
    [numero_liaison_sa_ft] VARCHAR (8000) NULL,
    [_gold_created_at]     DATETIME2 (6)  NULL,
    [_pipeline_run_id]     VARCHAR (8000) NULL
);


GO