# Recursive Subdivision Temperature Grid

A geospatial analysis tool that builds adaptive temperature-colored grid polygons from point samples using recursive subdivision. The tool computes subregion statistics, generates interactive Folium maps, and produces static visualizations and Google Earth overlays.

## Features

- **Adaptive Grid Resolution**: Recursively subdivides regions based on sample density, creating finer grids where more data exists
- **Accurate Projection**: All subdivision and interpolation is performed in UTM meters (not degrees), avoiding distortion at higher latitudes
- **Interactive Maps**: Generates Folium maps with multiple layers (GeoJSON polygons, raw sample points, contour overlays)
- **Multiple Tile Layers**: Supports CartoDB, Esri Satellite, and OpenStreetMap base layers
- **Contour Visualization**: Creates transparent contour overlays exported as PNG, KML, and KMZ for Google Earth
- **Statistical Analysis**: Computes average temperature, standard deviation, and sample count per subregion
- **Flexible Input**: Auto-detects longitude/latitude and temperature columns with automatic unit conversion (°C to °F)

## Installation

```bash
pip install -r requirements.txt
```

### Dependencies

- folium
- geopandas
- matplotlib
- numpy
- pandas
- pyproj
- scipy
- shapely

## Usage

The main script is `Recursive_Sub_Sample_Script_V9.py`.

### Basic Run

```bash
python Recursive_Sub_Sample_Script_V9.py --csv InputData.csv
```

### CLI Options

| Option | Default | Description |
|--------|---------|-------------|
| `--csv` | `InputData.csv` | Path to input CSV with longitude, latitude, and temperature |
| `--min-samples` | `10` | Minimum samples required to subdivide a region |
| `--min-cell-samples` | `4` | Minimum samples required to keep a subregion cell |
| `--predicate` | `intersects` | Spatial join predicate (`intersects` or `within`) |
| `--output-centroids` | `intermediate_centroids.csv` | Output CSV for centroid coordinates and avg temperature |
| `--folium-html` | `folium_geojson_only.html` | Output HTML for interactive Folium map |
| `--with-raster` | off | Also render transparent contour PNG and raster Folium map |
| `--raster-png` | `contour_overlay.png` | Filename for saved transparent contour PNG |
| `--raster-html` | `folium_map.html` | Output HTML for Folium raster map |
| `--no-static-plots` | off | Disable Matplotlib static plots |
| `--color-table` | `1` | Color table: 1 = blue-cyan-green-yellow-red, 2 = blue-green-red |
| `--no-borders` | on | Hide rectangle borders in static plots and contour PNG (default) |
| `--show-borders` | off | Show rectangle borders in static plots and contour PNG |
| `--folium-output-fundamentals` | `folium_output_fundamentals.csv` | Output CSV of per-subregion statistics for ArcGIS |

### Examples

```bash
# Specify input CSV and minimum samples threshold
python Recursive_Sub_Sample_Script_V9.py --csv MyData.csv --min-samples 5

# Generate raster overlay and Google Earth files
python Recursive_Sub_Sample_Script_V9.py --csv InputData.csv --with-raster

# Run without opening static plot windows
python Recursive_Sub_Sample_Script_V9.py --csv InputData.csv --no-static-plots
```

## Input Data Format

The input CSV must contain columns for longitude, latitude, and temperature. Column names are auto-detected:

**Longitude**: `longitude`, `lon`, `lng`, `x`, `long`

**Latitude**: `latitude`, `lat`, `y`

**Temperature**: `temperature`, `temp`, `degf`, `deg_f`, `degc`, `deg_c`, or any column containing "temp"

Temperatures in °C are automatically converted to °F.

Example CSV:
```csv
longitude,latitude,temperature
-121.7623,37.6879,72.5
-121.7612,37.6891,73.2
-121.7634,37.6868,71.8
```

## Output Files

| File | Description |
|------|-------------|
| `folium_geojson_only.html` | Interactive map with temperature grid polygons, contour overlay, and raw sample points |
| `folium_map.html` | Interactive map with raster contour overlay (if `--with-raster`) |
| `intermediate_centroids.csv` | Centroid coordinates and average temperature per subregion |
| `folium_output_fundamentals.csv` | Per-subregion statistics (avg temp, std dev, sample count) for ArcGIS |
| `centroid_contour.png` | Transparent contour PNG embedded in the Folium map |
| `contour_overlay.png` | Transparent contour PNG for external use |
| `contour_overlay.kml` | KML file for Google Earth (references `contour_overlay.png`) |
| `contour_overlay.kmz` | Self-contained KMZ (KML + PNG bundled) for Google Earth |

## Pipeline

1. **Load** — Read CSV, auto-detect columns, convert units if needed
2. **Subdivide** — Recursively partition the bounding box into quadrants in UTM meters; stop when a cell has fewer than `--min-samples` points
3. **Filter** — Drop cells with fewer than `--min-cell-samples` points
4. **Aggregate** — Spatial join raw points to cells; compute avg temperature, std dev, sample count, and mean sensor position per cell
5. **Centroid** — Compute true geographic centroids in UTM, store in WGS84
6. **Interpolate** — Project cell mean-sensor positions to UTM, build a Delaunay triangulation, interpolate temperatures onto a 400×400 grid using barycentric weights; mask areas outside the data extent as transparent
7. **Render** — Save contour as a georeferenced transparent PNG; write KML and KMZ ground overlays; generate interactive Folium HTML maps and optional static Matplotlib plots

## License

MIT
