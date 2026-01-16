# rmx-scatterplot

Remix web component for rendering interactive WebGL scatterplots backed by Parquet data.

## Manifest

The component is registered via `manifest.json`.


## Inputs (`ins`)

### `parquet-url`

URL to a Parquet file containing the point data.

```json
{
  "name": "parquet-url",
  "type": "url",
  "defaultValue": ""
}
```

### `x`

Name of the numeric column used for the x-axis projection.

```json
{
  "name": "x",
  "type": "string",
  "defaultValue": "projection_x"
}
```

### `y`

Name of the numeric column used for the y-axis projection.

```json
{
  "name": "y",
  "type": "string",
  "defaultValue": "projection_y"
}
```

### `cluster-id`

Name of the column containing cluster identifiers.

```json
{
  "name": "cluster-id",
  "type": "string",
  "defaultValue": "cluster_id"
}
```

### `point-size`

Rendered size of scatterplot points.

```json
{
  "name": "point-size",
  "type": "number",
  "defaultValue": 4
}
```

### `selected-cluster-name`

Name of the currently selected cluster.

Used to externally control cluster highlighting.

```json
{
  "name": "selected-cluster-name",
  "type": "string",
  "defaultValue": ""
}
```

### `select-cluster`

Event input used to trigger cluster selection programmatically.

```json
{
  "name": "select-cluster",
  "type": "event"
}
```

## Events (`events`)

### `selected-point`

Emitted when a single point is selected.

Payload contains the full metadata object for the selected point.

```json
{
  "name": "selected-point"
}
```

### `selected-points`

Emitted when multiple points are selected via lasso selection.

Payload is a list of metadata objects corresponding to the selected points.

```json
{
  "name": "selected-points",
  "payload": [{}]
}
```

### `clusters-changed`

Emitted when cluster assignments or visibility change.

Used to drive legends or external cluster controls.

```json
{
  "name": "clusters-changed",
  "payload": [{}]
}
```

## Parquet File Expectations

The Parquet source is expected to include:
- Numeric columns for x/y projection
- A cluster identifier column
- Arbitrary metadata columns passed through in selection events


## Development

Local development uses a minimal `index.html` harness.

```bash
npm install
npm run dev
```
