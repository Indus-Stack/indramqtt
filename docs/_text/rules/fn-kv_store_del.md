`kv_store_del(<key>)` deletes the node-global slot for `key`.

The call takes one key. It deletes the entry of the shared node map. It returns the removed value. A missing key reads as null. A wrong argument count reads as null. The test entry never deletes: it returns null and keeps the old state.
