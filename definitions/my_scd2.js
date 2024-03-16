function fn_SCD2load(processed_table, target_table, target, load_type) {
    // Destructuring properties from the target object
    const {
        bk,
        hash_dif,
        dk,
        insert_list,
        compare_list
    } = target;

    // Initialize variables for unique_key (bk), hash difference (hk), datakey (dk),
    // column list for insert, and compare list using the provided functions.
    let unique_key = bk;
    //let hash_dif = hk;
    let datakey = dk;
    let column_list = insert_list;

    // Generate the list of columns to compare
    let compare_listString = fn_updateColCompareList(compare_list);
    // Checking if hash_dif exists or not for the given target table, ignore hash key compariso if not exists
    if (hash_dif === "") {
        return `
      MERGE INTO ${target_table} AS tgt
      USING (
        -- Selecting all the records from the processed table, duplicating unique with different alias name
        SELECT distinct ${processed_table}.${unique_key} AS join_key, ${processed_table}.*
        FROM ${processed_table}
        WHERE vld_fm_ts IN (
          SELECT vld_fm_ts
          FROM (
            SELECT vld_fm_ts, ROW_NUMBER() OVER (PARTITION BY ${unique_key} ORDER BY vld_fm_ts DESC) AS rank
            FROM ${processed_table}
          )
          WHERE rank = 1
        )
        UNION ALL
        -- Adding a Null record for the inserting condition
        SELECT distinct CAST(NULL AS String) as join_key, src.*
        FROM ${processed_table} src
        JOIN ${target_table} tgt
        ON src.${unique_key} = tgt.${unique_key}
        WHERE (${compare_listString}
          AND tgt.vld_to_ts = '9999-12-31T23:59:59')
          AND src.vld_fm_ts IN (
            SELECT vld_fm_ts
            FROM (
              SELECT vld_fm_ts, ROW_NUMBER() OVER (PARTITION BY ${unique_key} ORDER BY vld_fm_ts DESC) AS rank
              FROM ${processed_table}
            )
            WHERE rank = 1
          )
      ) src
      -- Joining on join key to create duplicates of records to be updated or inserted
      ON src.join_key = tgt.${unique_key}
      WHEN MATCHED AND ${compare_listString} AND tgt.vld_to_ts = '9999-12-31T23:59:59'
      THEN
        -- Update condition for updating valid_to_ts value only and any data column
        UPDATE SET tgt.vld_to_ts = src.vld_fm_ts
      -- Insert records whether new or updated
      WHEN NOT MATCHED AND src.vld_to_ts='9999-12-31T23:59:59' THEN 
        INSERT (${column_list})
        VALUES (${column_list})`;
    } else {
        // Include hash_dif comparison if hash_dif exists for given target table
        return `
      MERGE INTO ${target_table} AS tgt 
      USING (
        -- Selecting all the records from the target table, duplicating unique with different alias name
        SELECT distinct ${processed_table}.${unique_key} AS join_key, ${processed_table}.*
        FROM ${processed_table}
        WHERE vld_fm_ts IN (
          SELECT vld_fm_ts
          FROM (
            SELECT vld_fm_ts, ROW_NUMBER() OVER (PARTITION BY ${unique_key} ORDER BY vld_fm_ts DESC) AS rank
            FROM ${processed_table}
          )
          WHERE rank = 1
        )
        UNION ALL
        -- Adding a Null record for the inserting condition
        SELECT distinct CAST(NULL AS String) as join_key, src.*
        FROM ${processed_table} src
        JOIN ${target_table} tgt
        ON src.${unique_key} = tgt.${unique_key}
        WHERE (
          src.${hash_dif} != tgt.${hash_dif} AND
          --src.${datakey} != tgt.${datakey} AND
          tgt.vld_to_ts = '9999-12-31T23:59:59'
        )
        AND src.vld_fm_ts IN (
          SELECT vld_fm_ts
          FROM (
            SELECT vld_fm_ts, ROW_NUMBER() OVER (PARTITION BY ${unique_key} ORDER BY vld_fm_ts DESC) AS rank
            FROM ${processed_table}
          )
          WHERE rank = 1
        )
      ) src
      -- Joining on join key to create duplicates of records to be updated or inserted
      ON src.join_key = tgt.${unique_key}
      WHEN MATCHED AND src.${hash_dif} != tgt.${hash_dif} AND tgt.vld_to_ts = '9999-12-31T23:59:59'
      THEN
        -- Update condition for updating valid_to_ts value only and any data column
        UPDATE SET tgt.vld_to_ts = src.vld_fm_ts
      -- Insert records whether new or updated
      WHEN NOT MATCHED THEN
        INSERT (${column_list})
        VALUES (${column_list})`;
    }
}


module.exports = {
    fn_SCD2load
};
