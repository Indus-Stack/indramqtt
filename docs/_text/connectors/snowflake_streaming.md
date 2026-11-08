`snowflake_streaming` is a documented Snowflake action name. The v5 actions route accepts it. The v1 and v5 connector routes reject it because it is not a connector type. The action stores its parameters opaquely.

The connector routes answer 400 and name the unknown type. The action route answers 201 and stores the posted type. The Snowflake parameters use the same fields as the `snowflake` connector.
