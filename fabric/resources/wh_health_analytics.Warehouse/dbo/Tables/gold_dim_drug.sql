CREATE TABLE [dbo].[gold_dim_drug] (
    [cis_code]                     VARCHAR (8000) NULL,
    [denomination]                 VARCHAR (8000) NULL,
    [forme_pharmaceutique]         VARCHAR (8000) NULL,
    [voie_administration]          VARCHAR (8000) NULL,
    [nombre_voies]                 INT            NULL,
    [statut_amm]                   VARCHAR (8000) NULL,
    [type_procedure_amm]           VARCHAR (8000) NULL,
    [etat_commercialisation]       VARCHAR (8000) NULL,
    [is_commercialisee]            BIT            NULL,
    [date_amm]                     DATE           NULL,
    [titulaire]                    VARCHAR (8000) NULL,
    [surveillance_renforcee]       BIT            NULL,
    [is_inferred]                  BIT            NULL,
    [substance_active_primary]     VARCHAR (8000) NULL,
    [dosage_primary]               VARCHAR (8000) NULL,
    [dosage_unit_primary]          VARCHAR (8000) NULL,
    [substance_count]              BIGINT         NULL,
    [generic_group_id]             VARCHAR (8000) NULL,
    [type_generique_label]         VARCHAR (8000) NULL,
    [is_generic]                   BIT            NULL,
    [is_princeps]                  BIT            NULL,
    [is_prescription_required]     BIT            NULL,
    [smr_latest]                   VARCHAR (8000) NULL,
    [smr_rank_latest]              INT            NULL,
    [asmr_latest]                  VARCHAR (8000) NULL,
    [asmr_rank_latest]             INT            NULL,
    [code_atc]                     VARCHAR (8000) NULL,
    [atc_level1]                   VARCHAR (8000) NULL,
    [atc_level2]                   VARCHAR (8000) NULL,
    [is_mitm]                      BIT            NULL,
    [voies_administration]         VARCHAR (8000) NULL,
    [substances_list]              VARCHAR (8000) NULL,
    [prescription_categories_list] VARCHAR (8000) NULL,
    [atc_level1_label]             VARCHAR (8000) NULL,
    [drug_sk]                      BIGINT         NOT NULL,
    [_gold_created_at]             DATETIME2 (6)  NULL,
    [_pipeline_run_id]             VARCHAR (8000) NULL
);


GO

ALTER TABLE [dbo].[gold_dim_drug]
    ADD CONSTRAINT [PK_gold_dim_drug] PRIMARY KEY NONCLUSTERED ([drug_sk] ASC) NOT ENFORCED;


GO