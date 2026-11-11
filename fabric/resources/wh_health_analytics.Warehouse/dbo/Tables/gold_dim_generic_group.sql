CREATE TABLE [dbo].[gold_dim_generic_group] (
    [generic_group_id]      VARCHAR (8000) NULL,
    [libelle_groupe]        VARCHAR (8000) NULL,
    [total_members]         BIGINT         NULL,
    [generic_count]         BIGINT         NULL,
    [princeps_count]        BIGINT         NULL,
    [princeps_cis_code]     VARCHAR (8000) NULL,
    [princeps_denomination] VARCHAR (8000) NULL,
    [has_generics]          BIT            NULL,
    [group_sk]              BIGINT         NOT NULL,
    [_gold_created_at]      DATETIME2 (6)  NULL,
    [_pipeline_run_id]      VARCHAR (8000) NULL
);


GO

ALTER TABLE [dbo].[gold_dim_generic_group]
    ADD CONSTRAINT [PK_gold_dim_generic_group] PRIMARY KEY NONCLUSTERED ([group_sk] ASC) NOT ENFORCED;


GO