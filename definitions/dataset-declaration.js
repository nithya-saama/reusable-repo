["encounter",
"patient",
"qadata_ptnt",
"practitioner"].forEach((name) =>
    declare({
        database: dataform.projectConfig.vars.defaultDatabase,
        schema: dataform.projectConfig.vars.src_dataset,
        name,
    })
);



["cmn_cd_2_tenant_cmn_cd_brg",
"cmn_cd_dim",
"encntr_2_pract_brg",
"encounter_dim",
"encounter_fct",
"patient_dim",
"practitioner_dim",
"src_stm_dim",
"tenant_cmn_cd_dim",
"tenant_dim"].forEach((name) =>
    declare({
        database: dataform.projectConfig.vars.defaultDatabase,
        schema: dataform.projectConfig.vars.tgt_dataset,
        name,
    })
);