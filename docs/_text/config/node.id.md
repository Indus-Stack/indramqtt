`node.id` sets the name of this broker node. Management reads use the name. The node name has the form `name@host` unless the value contains `@`.

Boot fixes the name before any component reads it. A restart applies a new value. A bad value fails validation.
