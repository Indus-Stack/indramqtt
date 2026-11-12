`proc_dict_get(<key>)` returns the rule-local value for `key`.

The call takes one key. It reads the slot of the current rule. One rule cannot read the slot of another rule. A missing key reads as null. A wrong argument count reads as null.
