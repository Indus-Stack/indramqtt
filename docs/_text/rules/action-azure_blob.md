`azure_blob` stores result rows as Azure blobs.

The sink batches rows into one blob per flush. `blob_path_template` sets the blob path. `max_records_per_blob` and `max_bytes_per_blob` bound each blob. The tier is enterprise.
