//! Keep a v1 `disk_log` directory inside the kernel data directory.
//!
//! The v1 create route parses and validates the config first. It then
//! refuses a path that leaves the data directory. A valid path becomes
//! absolute before the sink opens it.

use crate::CreateConnectorRequest;

/// Validate the disk_log config and resolve `directory` under `data_dir`.
pub(crate) fn prepare_v1_disk_log(
    req: &mut CreateConnectorRequest,
    data_dir: &std::path::Path,
) -> Result<(), String> {
    let disk_config: broker_connectors::DiskLogSinkConfig =
        serde_json::from_value(req.config.clone())
            .map_err(|error| format!("invalid disk_log config: {error}"))?;
    disk_config
        .validate()
        .map_err(|error| format!("invalid disk_log config: {error}"))?;
    let directory = disk_config.directory.clone();
    if !crate::v5::rules::disk_log_path_contained(&directory, data_dir) {
        return Err(format!(
            "connector {} field `directory` must be a relative path inside the data directory",
            req.id
        ));
    }
    let joined = data_dir.join(&directory).to_string_lossy().into_owned();
    if let Some(map) = req.config.as_object_mut() {
        map.insert("directory".to_string(), serde_json::Value::String(joined));
    }
    Ok(())
}
