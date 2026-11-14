`proc_dict_put(<key>, <value>)` writes the rule-local slot for `key`.

The call takes one key and one value. It writes the slot of the current rule. It returns the stored value. A key longer than 4096 characters reads as null. A wrong argument count reads as null. The test entry never writes: it returns null and keeps the old state. The map holds 10000 entries at most; a put past the cap returns null.
