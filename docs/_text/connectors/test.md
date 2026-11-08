`test` is a unit-test helper name. It names a recording sink in the connector crate tests. It has no production send path of its own. The v1 connector route rejects the name. The v5 connector route accepts it and registers the console logger sink under the name and under `test:name`. The tier is community.

The registered sink writes each rule result to the broker log at INFO. It opens no socket and holds no buffer. The connector has no settings. The v5 route accepts an empty body next to the name and the type.
