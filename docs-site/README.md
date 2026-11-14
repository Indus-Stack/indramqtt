# Docs site

Static site for the user documentation in `docs/`. It uses the Starlight generator (MIT licence; see `node_modules/@astrojs/starlight/package.json` after install). The build copies `docs/` into the content directory, so the pages cannot drift from the tested sources.

## Commands

Run each command in `docs-site/`:

```sh
npm install
npm run build
```

Serve a preview:

```sh
npm run dev
```
