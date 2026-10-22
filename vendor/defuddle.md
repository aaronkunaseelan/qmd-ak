Built with esbuild 0.28.1 (`--bundle --platform=node --target=node24 --format=cjs --minify --legal-comments=eof`), Defuddle 0.19.5, linkedom 0.18.12 and turndown 7.2.0; turndown's browser build shares linkedom rather than loading a second DOM.

Rebuild in a temporary directory (network at build time only):

```sh
npm install --ignore-scripts --save-exact defuddle@0.19.5 linkedom@0.18.12 turndown@7.2.0 esbuild@0.28.1
# Save the source below as entry.cjs.
node_modules/.bin/esbuild entry.cjs --bundle --platform=node --target=node24 --format=cjs --minify --legal-comments=eof --alias:turndown=./node_modules/turndown/lib/turndown.browser.cjs.js --banner:js="$(printf '/*! Defuddle 0.19.5\n'; < node_modules/defuddle/LICENSE tr '\r' ' '; printf '\n*/')" --outfile=defuddle.cjs
```

```js
const fs = require('node:fs');
const { Defuddle } = require('./node_modules/defuddle/dist/defuddle.js');
const { parseLinkedomHTML } = require('./node_modules/defuddle/dist/utils/linkedom-compat.js');
globalThis.window = { DOMParser: require('linkedom').DOMParser };
const { toMarkdown } = require('./node_modules/defuddle/dist/markdown.js');
const html = fs.readFileSync(0, 'utf8');
let document = parseLinkedomHTML(html, 'about:blank');
if (document.documentElement?.localName !== 'html') {
  document = parseLinkedomHTML(`<html><body>${html}</body></html>`, 'about:blank');
}
const options = { markdown: true, useAsync: false };
const result = new Defuddle(document, options).parse();
toMarkdown(result, options);
process.stdout.write(JSON.stringify({
  text: result.content,
  title: result.title || null,
  author: result.author || null,
  published: result.published || null,
}));
```

The bundle includes the MIT notices. Only HTML bytes enter stdin; parsing is synchronous and async/network extractors are disabled. System Node runs with one V8 worker to fit the converter's 1 GiB address-space limit, a 256 MiB JS heap and a 15-second wall deadline. No packages are installed at deployment or runtime.
