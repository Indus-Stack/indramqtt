`persistence.stream_dir` sets the directory of the durable stream journal. An empty value disables journaling.

The journal appends each publish through a bounded channel. The broker creates a missing directory. The default is empty.
