CREATE TABLE [dbo].[gold_dim_presentation] (
    [cip13]                              VARCHAR (8000) NULL,
    [cis_code]                           VARCHAR (8000) NULL,
    [cip7]                               VARCHAR (8000) NULL,
    [libelle_presentation]               VARCHAR (8000) NULL,
    [statut_administratif]               VARCHAR (8000) NULL,
    [etat_commercialisation]             VARCHAR (8000) NULL,
    [is_commercialisee]                  BIT            NULL,
    [date_declaration_commercialisation] DATE           NULL,
    [agrement_collectivites]             VARCHAR (8000) NULL,
    [cip13_valid]                        BIT            NULL,
    [is_inferred]                        BIT            NULL,
    [presentation_sk]                    BIGINT         NOT NULL,
    [_gold_created_at]                   DATETIME2 (6)  NULL,
    [_pipeline_run_id]                   VARCHAR (8000) NULL
);


GO

ALTER TABLE [dbo].[gold_dim_presentation]
    ADD CONSTRAINT [PK_gold_dim_presentation] PRIMARY KEY NONCLUSTERED ([presentation_sk] ASC) NOT ENFORCED;


GO