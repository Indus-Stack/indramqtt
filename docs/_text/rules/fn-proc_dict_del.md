`proc_dict_del(<key>)` deletes the rule-local slot for `key`.

The call takes one key. It deletes the slot of the current rule. It returns the removed value. A missing key reads as null. A wrong argument count reads as null. The test entry never deletes: it returns null and keeps the old state.
