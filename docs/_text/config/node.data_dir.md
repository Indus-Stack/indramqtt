`node.data_dir` sets the directory for kernel state. The broker creates the directory when it is missing. It keeps the state file, the offline queue, the delayed entries and the cluster identity there.

Boot opens the directory before any listener binds. A relative path resolves from the process working directory. An unreadable directory fails boot.
