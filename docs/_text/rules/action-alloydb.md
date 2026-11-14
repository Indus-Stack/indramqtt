`alloydb` sends each result row to Google AlloyDB.

The sink runs batched `INSERT` statements against the `table`. `column_mappings` maps each payload field to a database column. `auth` selects password or IAM token login. The tier is enterprise.
