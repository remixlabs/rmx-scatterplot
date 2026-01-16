# scatterplot-webcomp

Remix Labs WebComponent that renders an interactive WebGL scatterplot using `regl-scatterplot`.

## Development

```bash
npm install
npm run dev
```

Open the dev page and you should see a scatterplot filling the page.

## Build

```bash
npm run build
```

This creates `dist/rmx-scatterplot.js`.

## Distribute

```bash
npm run bundle
```

This creates `dist/rmx-scatterplot.zip` containing:
- `rmx-scatterplot.js`
- `manifest.json`

## Manifest / Bindings

### Inputs
- `data` (`[{}]`): list of objects containing at least numeric `x` and `y` fields (any additional metadata is ignored)
- `pointSize` (`number`): point size in pixels

### Events
- none (empty list for now)
