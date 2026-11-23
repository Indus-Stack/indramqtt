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

## Links between pages

The pages in `docs/` link to each other by file name, for example
`[Rule SQL](./sql.md)`. GitHub shows these links. The site serves
`docs/rules/sql.md` at `/rules/sql/`, so `remark-md-links.mjs` rewrites each
such link while the site builds. A link to a file outside `docs/` becomes a
link to the file in the repository. Keep the file-name form in the source
files.

`npm run sync-docs` copies `docs/` into the site. It leaves out the internal
folders whose names start with `_`, and it publishes
`docs/_verification/failures.md` at `/reference/verification-failures/`.

Check a change before you push it:

```
npm ci
npm test                                   # tests of the link plugin
npm run build                              # builds every page
python3 ../tools/check_site_links.py dist  # follows every link and anchor
```

The CI runs the same steps. A link that the checker cannot resolve fails the
pull request.
