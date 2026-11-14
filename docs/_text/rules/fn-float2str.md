`float2str(<f>, <precision>)` renders the number `f` as text.

The call takes two arguments. `precision` sets the count of decimals.
The result holds exactly `precision` decimals. `precision` runs from 0
to 20; a value outside that range reads as null. A text that is not a
number reads as null. A wrong argument count reads as null.
