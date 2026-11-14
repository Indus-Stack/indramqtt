`format_date(<unit>, <offset>, <pattern>[, <epoch>])` formats an epoch value as text.

The call takes the unit of the epoch, the offset in seconds, and the
pattern. The fourth argument sets the epoch; without it the call uses
the current time. The pattern uses `yyyy`, `MM`, `dd`, `HH`, `mm`,
`ss` and `S` tokens. A quoted part stays literal. A wrong unit, a
non-numeric offset or epoch, or a wrong argument count reads as null.
