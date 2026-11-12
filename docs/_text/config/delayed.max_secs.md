`delayed.max_secs` bounds a `$delayed/<secs>/<topic>` deferral. The broker drops a request above the bound and writes a warning.

The default 86400 seconds is one day. The driver keeps pending entries in the data directory.
