`kv_store_get(<key>)` returns the node-global value for `key`.

The call takes one key, with one default option. It reads the shared node map. All rules share the same map. A missing key without a default reads as null. A missing key with a default returns the default. A wrong argument count reads as null.
