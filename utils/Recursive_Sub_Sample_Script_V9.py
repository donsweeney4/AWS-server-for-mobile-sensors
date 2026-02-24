"""
temperature_grid.py

Build temperature-colored grid polygons from point samples using a recursive
subdivision, compute subregion stats, generate Folium layers (GeoJSON and
optional raster overlay), and produce static Matplotlib plots.

CLI usage (examples):
---------------------
# Basic run with defaults:
python temperature_grid.py

# Specify input CSV and min samples:
python temperature_grid.py --csv InputData.csv --min-samples 5

# Produce Folium map with raster overlay (from a transparent contour image):
python temperature_grid.py --folium-html folium_map.html --with-raster
"""

from __future__ import annotations

import argparse
import json
import math
import os
import time
import zipfile
from dataclasses import dataclass
from typing import List, Tuple, Optional

import numpy as np
import numpy.ma as ma
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap

import geopandas as gpd
from shapely.geometry import Point, Polygon
from shapely.ops import unary_union
from shapely.prepared import prep  # optional but recommended
from scipy.interpolate import griddata
from pyproj import Transformer

import folium
import branca.colormap as bcm
from folium.raster_layers import ImageOverlay
from folium.plugins import FastMarkerCluster


# -------------------------------
# Custom Matplotlib Colormap
# -------------------------------
def get_custom_cmap():
    """Create a Matplotlib colormap using a high-contrast scheme."""
    hex_colors = [
        "#0000FF",  # blue (low)
        "#0000FF",
        "#00FF00",  # green (mid)
        "#00FF00",
        "#FF0000",  # red (high)
        "#FF0000",
    ]
    return LinearSegmentedColormap.from_list("high_contrast_temp", hex_colors, N=256)

HIGH_CONTRAST_CMAP = get_custom_cmap()


# -------------------------------
# Delaunay triangulation interpolation
# -------------------------------
def delaunay_interpolate(points, values, grid_points):
    """
    Perform Delaunay triangulation interpolation (linear/barycentric).

    Uses scipy.griddata with method='linear', which builds a Delaunay
    triangulation and interpolates within each triangle using barycentric
    coordinates. Points outside the convex hull return NaN.

    Args:
        points: (N, 2) array of sample point coordinates
        values: (N,) array of values at sample points
        grid_points: (M, 2) array of grid points to interpolate to

    Returns:
        (M,) array of interpolated values (NaN outside convex hull)
    """
    return griddata(points, values, grid_points, method='linear')


# -------------------------------
# Data container
# -------------------------------
@dataclass
class BuildArtifacts:
    gdf_points: gpd.GeoDataFrame
    subregions: gpd.GeoDataFrame
    image_bounds: Optional[List[List[float]]] = None
    contour_png: Optional[str] = None


# -------------------------------
# IO / Loading
# -------------------------------
def load_points_csv(path: str) -> gpd.GeoDataFrame:
    print(f"[loader] Loading points from '{path}'...")
    df = pd.read_csv(path)

    print(f"[CHECK] Raw CSV rows: {len(df)}")
    

    # Normalize column names
    norm = {c.lower().strip(): c for c in df.columns}

    # Find lon/lat
    lon_key = next((k for k in ["longitude", "lon", "lng", "x", "long"] if k in norm), None)
    lat_key = next((k for k in ["latitude", "lat", "y"] if k in norm), None)
    if not lon_key or not lat_key:
        raise ValueError(f"Could not find longitude/latitude columns in {list(df.columns)}")
    lon_col, lat_col = norm[lon_key], norm[lat_key]

    # Temperature column is always corrected_temperature_f
    temp_col = "corrected_temperature_f"
    if temp_col not in df.columns:
        raise ValueError(f"Could not find '{temp_col}' column in {list(df.columns)}")

    use = pd.DataFrame({
        "longitude": pd.to_numeric(df[lon_col], errors="coerce"),
        "latitude": pd.to_numeric(df[lat_col], errors="coerce"),
        "temperature": pd.to_numeric(df[temp_col], errors="coerce")
    }).dropna(subset=["longitude", "latitude", "temperature"])

    print(f"[loader] Using lon='{lon_col}', lat='{lat_col}', temp='{temp_col}' (°F).")
    print(f"[loader] Loaded {len(use)} valid points from '{path}'.")

    geometry = [Point(xy) for xy in zip(use["longitude"], use["latitude"])]
    gdf = gpd.GeoDataFrame(use[["temperature"]], geometry=geometry, crs="EPSG:4326")

    print(f"[CHECK] Valid points after coercion: {len(gdf)}")

    return gdf


# -------------------------------
# Recursive subdivision
# -------------------------------
def recursive_subdivision_geopandas(
    gdf_points: gpd.GeoDataFrame,
    min_samples,  # default value
    region_polygon: Optional[Polygon] = None,
    _stages_accumulator: Optional[List[List[Polygon]]] = None,
    utm_crs: Optional[str] = None
) -> List[Polygon]:
    if region_polygon is not None:
        x_min, y_min, x_max, y_max = region_polygon.bounds
    else:
        x_min, y_min, x_max, y_max = gdf_points.total_bounds
        region_polygon = Polygon([
            (x_min, y_min), (x_max, y_min),
            (x_max, y_max), (x_min, y_max),
            (x_min, y_min)
        ])

# +++  GUARD: stop when the region is numerically tiny --- guards against infinite recursion if there are many points very close together
    # For UTM coordinates: 10 meters minimum extent
    # For geographic coordinates: 1e-5° ≈ 1.1 m in latitude
    min_extent = 10.0 if utm_crs else 1e-5
    if (x_max - x_min) < min_extent or (y_max - y_min) < min_extent:
        # If there are no points, return empty; otherwise treat this as a leaf.
        if len(gdf_points) == 0:
            return []
        return [region_polygon]
# +++ -----------------------------------------------------------


    if len(gdf_points) < min_samples:
        return [region_polygon]

    mid_x = (x_min + x_max) / 2.0
    mid_y = (y_min + y_max) / 2.0

    subrects = [
        Polygon([(x_min, y_min), (mid_x, y_min), (mid_x, mid_y), (x_min, mid_y), (x_min, y_min)]),
        Polygon([(mid_x, y_min), (x_max, y_min), (x_max, mid_y), (mid_x, mid_y), (mid_x, y_min)]),
        Polygon([(x_min, mid_y), (mid_x, mid_y), (mid_x, y_max), (x_min, y_max), (x_min, mid_y)]),
        Polygon([(mid_x, mid_y), (x_max, mid_y), (x_max, y_max), (mid_x, y_max), (mid_x, mid_y)])
    ]

    if _stages_accumulator is not None:
        _stages_accumulator.append(subrects)

    final_subregions: List[Polygon] = []
    for subrect in subrects:
        sub_gdf = gdf_points[gdf_points.intersects(subrect)]
        if len(sub_gdf) >= min_samples:
            final_subregions.extend(
                recursive_subdivision_geopandas(sub_gdf, min_samples, subrect, _stages_accumulator, utm_crs)
            )
        else:
            final_subregions.append(subrect)
    return final_subregions

# This is the key function that builds the subregion GeoDataFrame.
def build_subregion_gdf(gdf_points: gpd.GeoDataFrame, min_samples: int = 50) -> gpd.GeoDataFrame:
    # Determine appropriate UTM CRS and transform points
    utm_crs = get_utm_crs(gdf_points)
    gdf_points_utm = gdf_points.to_crs(utm_crs)
    print(f"[projection] Using UTM projection: {utm_crs}")
    
    # Run recursive subdivision in UTM coordinates
    final_polys = recursive_subdivision_geopandas(gdf_points_utm, min_samples=min_samples, utm_crs=utm_crs)
    
    # Create GeoDataFrame in UTM, then transform back to WGS84
    subregion_gdf = gpd.GeoDataFrame({'geometry': final_polys}, crs=utm_crs)
    subregion_gdf = subregion_gdf[subregion_gdf.is_valid & ~subregion_gdf.is_empty]
    subregion_gdf = subregion_gdf.to_crs("EPSG:4326")
    print(f"[projection] Created {len(subregion_gdf)} subregions, transformed back to WGS84")
    return subregion_gdf


# -------------------------------
# Stats per subregion
# -------------------------------
def compute_average_temperature(
    subregion_gdf: gpd.GeoDataFrame,
    sample_points_gdf: gpd.GeoDataFrame,
    predicate: str = "intersects"
) -> gpd.GeoDataFrame:
    # Ensure both datasets are in the same CRS (WGS84) for spatial join
    if subregion_gdf.crs is None or subregion_gdf.crs.to_string() != "EPSG:4326":
        subregion_gdf = subregion_gdf.to_crs(4326)
    if sample_points_gdf.crs is None or sample_points_gdf.crs.to_string() != "EPSG:4326":
        sample_points_gdf = sample_points_gdf.to_crs(4326)

    pts = sample_points_gdf.copy()
    pts["lon"] = pts.geometry.x
    pts["lat"] = pts.geometry.y

    joined = gpd.sjoin(pts, subregion_gdf, how="inner", predicate=predicate)
    stats = joined.groupby("index_right").agg(
        avg_temperature=("temperature", "mean"),
        std_temperature=("temperature", "std"),
        sample_count=("temperature", "count"),
        mean_lon=("lon", "mean"),
        mean_lat=("lat", "mean"),
    )

    out = subregion_gdf.copy()
    out["avg_temperature"] = out.index.map(stats["avg_temperature"])
    out["std_temperature"] = out.index.map(stats["std_temperature"])
    out["sample_count"] = out.index.map(stats["sample_count"])
    out["mean_lon"] = out.index.map(stats["mean_lon"])
    out["mean_lat"] = out.index.map(stats["mean_lat"])
    return out


def write_fundamentals_csv(subregion_gdf: gpd.GeoDataFrame, path: str = "folium_output_data.csv") -> None:
    df = subregion_gdf[
        ["mean_lat", "mean_lon", "avg_temperature", "std_temperature", "sample_count"]
    ].copy()
    df = df.dropna(subset=["avg_temperature", "mean_lat", "mean_lon", "sample_count"])
    df = df.rename(
        columns={
            "mean_lat": "avg_lat",
            "mean_lon": "avg_lon",
            "avg_temperature": "average_temperature_F",
            "std_temperature": "standard_deviation_F",
            "sample_count": "number_of_data_points",
        }
    )
    df["avg_lat"] = df["avg_lat"].astype(float).round(7)
    df["avg_lon"] = df["avg_lon"].astype(float).round(7)
    df["average_temperature_F"] = df["average_temperature_F"].astype(float).round(3)
    df["standard_deviation_F"] = df["standard_deviation_F"].astype(float).round(3)
    df["number_of_data_points"] = df["number_of_data_points"].astype(int)

    df.to_csv(path, index=False)
    print(f"✅ Wrote fundamentals CSV for ArcGIS: {path} (rows={len(df)})")


# -------------------------------
# Coordinate system utilities
# -------------------------------
def get_utm_crs(gdf: gpd.GeoDataFrame) -> str:
    """Determine appropriate UTM CRS for the data extent."""
    if gdf.crs is None or gdf.crs.to_string() != "EPSG:4326":
        gdf = gdf.to_crs(4326)
    bounds = gdf.total_bounds
    lon_c = (bounds[0] + bounds[2]) / 2.0
    lat_c = (bounds[1] + bounds[3]) / 2.0
    zone = int((lon_c + 180) // 6) + 1
    epsg_code = 32600 + zone if lat_c >= 0 else 32700 + zone
    return f"EPSG:{epsg_code}"


# -------------------------------
# Geometry helpers
# -------------------------------
def add_centroids_wgs84(subregion_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    gdf_wgs = subregion_gdf.to_crs(4326)
    minx, miny, maxx, maxy = gdf_wgs.total_bounds
    lon_c = (minx + maxx) / 2.0
    lat_c = (miny + maxy) / 2.0
    zone = int((lon_c + 180) // 6) + 1
    epsg_proj = 32600 + zone if lat_c >= 0 else 32700 + zone
    centroids_proj = gdf_wgs.to_crs(epsg_proj).centroid
    centroids_wgs = centroids_proj.to_crs(4326)
    out = gdf_wgs.copy()
    out["centroid"] = centroids_wgs
    return out


# -------------------------------
# Colors
# -------------------------------
def _safe_linear_colormap(vmin: float, vmax: float, reverse: bool = False):
    colors = [
        "#0000FF", "#0000FF",
        "#0000FF", "#0000FF",
        "#00FF00", "#00FF00",
        "#FF0000", "#FF0000",
         "#FF0000", "#FF0000",
    ]
    if reverse:
        colors = list(reversed(colors))
    return bcm.LinearColormap(colors=colors, vmin=vmin, vmax=vmax)


def gdf_to_geojson(gdf: gpd.GeoDataFrame) -> dict:
    if gdf.crs is None or gdf.crs.to_string() != "EPSG:4326":
        gdf = gdf.to_crs(epsg=4326)
    gdf2 = gdf.copy()
    if "centroid" in gdf2.columns:
        gdf2["centroid_lon"] = gdf2["centroid"].apply(lambda p: p.x if p is not None else None)
        gdf2["centroid_lat"] = gdf2["centroid"].apply(lambda p: p.y if p is not None else None)
        gdf2 = gdf2.drop(columns=["centroid"])
    return json.loads(gdf2.to_json())


# -------------------------------
# Folium map creation with layers
# -------------------------------

def create_folium_map_with_layers(
    subregion_gdf: gpd.GeoDataFrame,
    image_file: Optional[str] = None,
    image_bounds: Optional[List[List[float]]] = None,
    add_centroids: bool = True,
    output_html: str = "folium_geojson_only.html",
    base_tiles: str = "CartoDB positron",
    points_gdf: Optional[gpd.GeoDataFrame] = None,
    points_as_cluster: bool = False,
    point_radius: int = 2,
    add_centroid_contour: bool = False,
    centroid_contour_png: str = "centroid_contour.png",
    centroid_contour_opacity: float = 0.55
) -> folium.Map:

    if subregion_gdf.crs is None or subregion_gdf.crs.to_string() != "EPSG:4326":
        subregion_gdf = subregion_gdf.to_crs(4326)

    minx, miny, maxx, maxy = subregion_gdf.total_bounds
    center_lat = (miny + maxy) / 2.0
    center_lon = (minx + maxx) / 2.0

    attribution_stamen = 'Map tiles by Stamen Design, under CC BY 3.0. Data by OpenStreetMap, under ODbL.'
    attribution_cartodb = 'Map tiles by CartoDB, under CC BY 3.0. Data by OpenStreetMap, under ODbL.'
    attribution_esri = 'Source: Esri, Maxar, GeoEye, Earthstar Geographics, CNES/Airbus DS, USDA, USGS, AeroGRID, IGN, and the GIS User Community'
    attribution_osm = 'Map data by OpenStreetMap contributors'

    m = folium.Map(location=[center_lat, center_lon], zoom_start=13, tiles='CartoDB positron')

    current_attr = attribution_cartodb
    folium.TileLayer(tiles=base_tiles, name=base_tiles, overlay=False, control=True, attr=current_attr).add_to(m)
    folium.TileLayer(tiles='Esri.WorldImagery', name='Satellite View (Esri)', overlay=False, control=True, attr=attribution_esri).add_to(m)
    folium.TileLayer(tiles='OpenStreetMap', name='OpenStreetMap (Fallback)', overlay=False, control=True, attr=attribution_osm).add_to(m)

    vals = subregion_gdf['avg_temperature'].astype(float).dropna()
    if vals.empty:
        vmin, vmax = 0.0, 1.0
    else:
        vmin, vmax = float(vals.min()), float(vals.max())
        if vmin == vmax:
            pad = 0.5 if vmin == 0.0 else 0.05 * abs(vmin)
            vmin -= pad
            vmax += pad

    cmap = _safe_linear_colormap(vmin, vmax)
    cmap.caption = "Avg Temperature (°F)"
    cmap.add_to(m)

    gj = gdf_to_geojson(subregion_gdf)
    if not gj.get("features"):
        print("⚠️ GeoJSON has 0 features — the layer would render nothing.")

    tooltip = folium.GeoJsonTooltip(
        fields=["avg_temperature", "std_temperature", "sample_count", "mean_lat", "mean_lon"],
        aliases=["Avg Temp (°F):", "Std Dev (°F):", "Samples:", "Mean Lat:", "Mean Lon:"],
        localize=True, sticky=True
    )
    popup_content = folium.GeoJsonPopup(
        fields=["avg_temperature", "std_temperature", "sample_count", "mean_lat", "mean_lon"],
        aliases=["Avg Temp (°F):", "Std Dev (°F):", "Samples:", "Mean Lat:", "Mean Lon:"],
        localize=True, labels=True, max_width=340
    )

    def style_function_with_borders(feature):
        temp = feature["properties"].get("avg_temperature", None)
        fill_color = cmap(temp) if temp is not None else "#cccccc"
        return {"fillColor": fill_color, "color": "black", "weight": 1.0, "fillOpacity": 0.7, "stroke": True}  # bounding polygon border is disabled with stroke=False

    poly_fg = folium.FeatureGroup(name="Temperature Grid (with borders)", show=False)
    folium.GeoJson(
        data=gj,
        name="Temperature Polygons",
        style_function=style_function_with_borders,
        tooltip=tooltip,
        popup=popup_content,
        highlight_function=lambda f: {"weight": 2, "color": "#333333"}
    ).add_to(poly_fg)
    poly_fg.add_to(m)

    # Raw points

    if points_gdf is not None and not points_gdf.empty:  # Add raw sample points if possible
        pts = points_gdf
        if pts.crs is None or pts.crs.to_string() != "EPSG:4326":
            pts = pts.to_crs(4326)
        if points_as_cluster:
            cluster_fg = folium.FeatureGroup(name="Raw Samples (cluster)", show=True, control=True)
            coords = [(geom.y, geom.x) for geom in pts.geometry if geom is not None]
            FastMarkerCluster(data=coords).add_to(cluster_fg)
            cluster_fg.add_to(m)

        else:

            pts_fg = folium.FeatureGroup(name="Raw Samples (points)", show=True, control=True)
            for _, row in pts.iterrows():
                geom = row.geometry
                if geom is None:
                    continue
                lat, lon = geom.y, geom.x
                t = row.get("temperature", None)
                folium.CircleMarker(
                    location=(lat, lon),
                    radius=max(4, point_radius),
                    color="black", weight=1,
                    fill=True, fill_color="black", fill_opacity=1.0,
                    tooltip=(f"Temp (°F): {float(t):.2f}" if pd.notna(t) else None)
                ).add_to(pts_fg)
            pts_fg.add_to(m)

   
    # Centroid Contour 

    if add_centroid_contour:
        try:
            bounds, contour_png_path = save_contour_image(
                subregion_gdf,
                image_filename=centroid_contour_png,
                output_dir='.',
                no_borders=True
            )
            ImageOverlay(
                name="Centroid Contours (transparent voids)",
                image=contour_png_path,
                bounds=bounds,
                # keep opacity < 1.0 to blend with base map if you like,
                # the PNG already has alpha=0 in voids.
                opacity=centroid_contour_opacity,
                interactive=True,
                cross_origin=False,
                zindex=2
            ).add_to(m)

        # 🚫 Do NOT add a white mask overlay anymore.

        except Exception as e:
            print(f"⚠️ Failed to build centroid contour overlay: {e}")




    folium.LayerControl(collapsed=False).add_to(m)

    if not subregion_gdf.empty:
        minx, miny, maxx, maxy = subregion_gdf.total_bounds
        if abs(maxx - minx) > 0.0001 or abs(maxy - miny) > 0.0001:
            m.fit_bounds([[miny, minx], [maxy, maxx]])
        else:
            m.location = [center_lat, center_lon]
            m.zoom_start = 16

    m.save(output_html)
    print(f"✅ Folium map saved to: {output_html}")
    return m


# -------------------------------
# Static plotting (optional)
# -------------------------------
def plot_temperature_colored_subregions(subregion_gdf: gpd.GeoDataFrame, title: str = 'Temperature-Colored Subregions', no_borders: bool = True) -> None:
    fig, ax = plt.subplots(figsize=(10, 10))
    subregion_gdf.plot(
        ax=ax,
        column='avg_temperature',
        cmap=HIGH_CONTRAST_CMAP,
        linewidth=0 if no_borders else 0.5,
        edgecolor='none' if no_borders else 'black',
        legend=True,
        legend_kwds={'label': "Avg Temperature (°F)"}
    )
    ax.set_title(title)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_aspect("equal", adjustable="box")
    plt.show()


def plot_rectangles_and_contours(subregion_gdf: gpd.GeoDataFrame, title: str = 'Temperature Rectangles + Contour Overlay', no_borders: bool = True) -> None:
    valid = subregion_gdf.dropna(subset=['avg_temperature'])
    
    # Transform to UTM for accurate interpolation
    utm_crs = get_utm_crs(valid)
    valid_utm = valid.to_crs(utm_crs)
    
    # Get UTM centroids from the transformed polygon geometry
    utm_centroids = valid_utm.geometry.centroid
    xs = utm_centroids.x.values
    ys = utm_centroids.y.values
    temps = valid_utm['avg_temperature'].values

    xi = np.linspace(xs.min(), xs.max(), 300)
    yi = np.linspace(ys.min(), ys.max(), 300)
    xi, yi = np.meshgrid(xi, yi)

    # Use Delaunay triangulation interpolation
    points = np.column_stack((xs, ys))
    grid_points = np.column_stack((xi.ravel(), yi.ravel()))
    zi_flat = delaunay_interpolate(points, temps, grid_points)
    zi = zi_flat.reshape(xi.shape)

    # Transform grid coordinates back to WGS84 for plotting
    transformer = Transformer.from_crs(utm_crs, "EPSG:4326", always_xy=True)
    xi_wgs, yi_wgs = transformer.transform(xi, yi)

    fig, ax = plt.subplots(figsize=(10, 10))
    subregion_gdf.plot(
        ax=ax, column='avg_temperature', cmap=HIGH_CONTRAST_CMAP,
        linewidth=0 if no_borders else 0.5,
        edgecolor='none' if no_borders else 'black'
    )
    contour = ax.contourf(xi_wgs, yi_wgs, zi, levels=20, cmap=HIGH_CONTRAST_CMAP, alpha=0.5)
    plt.colorbar(contour, ax=ax, label='Avg Temperature (°F)')
    ax.set_title(title)
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    ax.set_aspect('equal', adjustable='box')
    plt.show()


def plot_contour_only(subregion_gdf: gpd.GeoDataFrame, title: str = 'Temperature Contour Map (No Grid)') -> None:
    valid = subregion_gdf.dropna(subset=['avg_temperature'])
    
    # Transform to UTM for accurate interpolation
    utm_crs = get_utm_crs(valid)
    valid_utm = valid.to_crs(utm_crs)
    
    # Get UTM centroids from the transformed polygon geometry
    utm_centroids = valid_utm.geometry.centroid
    xs = utm_centroids.x.values
    ys = utm_centroids.y.values
    temps = valid_utm['avg_temperature'].values

    xi = np.linspace(xs.min(), xs.max(), 300)
    yi = np.linspace(ys.min(), ys.max(), 300)
    xi, yi = np.meshgrid(xi, yi)

    # Use Delaunay triangulation interpolation
    points = np.column_stack((xs, ys))
    grid_points = np.column_stack((xi.ravel(), yi.ravel()))
    zi_flat = delaunay_interpolate(points, temps, grid_points)
    zi = zi_flat.reshape(xi.shape)

    # Transform grid coordinates back to WGS84 for plotting
    transformer = Transformer.from_crs(utm_crs, "EPSG:4326", always_xy=True)
    xi_wgs, yi_wgs = transformer.transform(xi, yi)

    fig, ax = plt.subplots(figsize=(10, 10))
    contour = ax.contourf(xi_wgs, yi_wgs, zi, levels=20, cmap=HIGH_CONTRAST_CMAP)
    plt.colorbar(contour, ax=ax, label="Avg Temperature (°F)")
    ax.set_title(title)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_aspect("equal", adjustable="box")
    plt.show()


# -------------------------------
# Raster export + mask (core)
# -------------------------------
def save_contour_image(
    subregion_gdf: gpd.GeoDataFrame,
    image_filename: str = 'centroid_contour.png',
    output_dir: str = '.',
    no_borders: bool = True
) -> Tuple[List[List[float]], str]:
    """
    Save a transparent contour PNG clipped to kept subregions.
    Pixels outside kept subregions (void) are fully transparent so base tiles show through.

    Returns:
      bounds (for Folium ImageOverlay), contour_image_path
     
    import os
    from scipy.interpolate import griddata
    import numpy.ma as ma
    from shapely.geometry import Point, Polygon
    from shapely.ops import unary_union
    from shapely.prepared import prep
"""
    os.makedirs(output_dir, exist_ok=True)
    contour_path = os.path.join(output_dir, image_filename)

    # --- collect samples ---
    valid = subregion_gdf.dropna(subset=['avg_temperature']).copy()
    use_means = ('mean_lon' in valid.columns) and ('mean_lat' in valid.columns)
    if use_means:
        valid = valid.dropna(subset=['mean_lon', 'mean_lat'])
        xs = valid['mean_lon'].astype(float).values
        ys = valid['mean_lat'].astype(float).values
        src = "mean points"
    else:
        if 'centroid' not in valid.columns:
            raise ValueError("save_contour_image: need mean_lon/mean_lat or centroid column.")
        valid = valid.dropna(subset=['centroid'])
        xs = valid['centroid'].apply(lambda p: p.x).astype(float).values
        ys = valid['centroid'].apply(lambda p: p.y).astype(float).values
        src = "centroids (fallback)"

    temps = valid['avg_temperature'].astype(float).values
    if len(xs) < 3:
        raise ValueError("Not enough points to build a contour surface (need >=3).")

    # --- grid + interpolation in UTM coordinates ---
    # Transform to UTM for accurate interpolation
    utm_crs = get_utm_crs(subregion_gdf)
    subregion_gdf_utm = subregion_gdf.to_crs(utm_crs)
    
    # Recalculate coordinates in UTM
    if use_means:
        # Transform mean points to UTM
        mean_points_wgs = [Point(lon, lat) for lon, lat in zip(valid['mean_lon'], valid['mean_lat'])]
        mean_gdf = gpd.GeoDataFrame(geometry=mean_points_wgs, crs='EPSG:4326')
        mean_gdf_utm = mean_gdf.to_crs(utm_crs)
        xs = mean_gdf_utm.geometry.x.values
        ys = mean_gdf_utm.geometry.y.values
    else:
        # Use UTM centroids from the active UTM geometry
        valid_utm = subregion_gdf_utm.dropna(subset=['avg_temperature'])
        utm_centroids = valid_utm.geometry.centroid
        xs = utm_centroids.x.values
        ys = utm_centroids.y.values
    
    xi = np.linspace(xs.min(), xs.max(), 400)
    yi = np.linspace(ys.min(), ys.max(), 400)
    X, Y = np.meshgrid(xi, yi)
    
    # Use Delaunay triangulation interpolation
    points = np.column_stack((xs, ys))
    grid_points = np.column_stack((X.ravel(), Y.ravel()))
    Z_flat = delaunay_interpolate(points, temps, grid_points)
    Z = Z_flat.reshape(X.shape)

    # --- mask void areas outside kept subregions (in UTM) ---
    keep_union = unary_union(subregion_gdf_utm.geometry.values) if not subregion_gdf_utm.empty else None
    if keep_union is not None and not keep_union.is_empty:
        keep_prep = prep(keep_union)
        pts = (Point(x, y) for x, y in zip(X.ravel(), Y.ravel()))
        inside = np.fromiter((keep_prep.covers(p) for p in pts), dtype=bool, count=X.size).reshape(X.shape)
        Z[~inside] = np.nan
        print("✅ Applied transparent mask to void regions (outside kept subregions).")
    
    # --- Transform grid coordinates back to WGS84 for plotting ---
    from pyproj import Transformer
    transformer = Transformer.from_crs(utm_crs, "EPSG:4326", always_xy=True)
    xi_wgs, yi_wgs = transformer.transform(xi, yi)

    # --- make masked array so NaNs -> transparent ---
    masked_Z = ma.array(Z, mask=np.isnan(Z))

    # clone colormap and set masked ("bad") to transparent
    cmap = HIGH_CONTRAST_CMAP
    try:
        cmap = cmap.copy()  # mpl >= 3.7
    except Exception:
        from matplotlib.colors import LinearSegmentedColormap
        cmap = LinearSegmentedColormap.from_list("hc_copy", [HIGH_CONTRAST_CMAP(i/255) for i in range(256)], N=256)
    cmap.set_bad((1, 1, 1, 0))  # RGBA with alpha=0

    # --- render as RGBA image ---
    fig, ax = plt.subplots(figsize=(8, 8), dpi=150)
    ax.set_axis_off()
    ax.set_position([0, 0, 1, 1])
    ax.imshow(
        masked_Z,
        extent=(xi_wgs.min(), xi_wgs.max(), yi_wgs.min(), yi_wgs.max()),
        origin='lower',
        cmap=cmap,
        interpolation='bilinear'  # 'nearest' if you want crisp pixels
    )
    # --- draw bounding outlines for each rectangle (in WGS84) ---
    if not no_borders:
        for geom in subregion_gdf.geometry:
            if geom is None or geom.is_empty:
                continue

            if geom.geom_type == "Polygon":
                x, y = geom.exterior.xy
                ax.plot(x, y, color="black", linewidth=0.4, alpha=0.8)

            elif geom.geom_type == "MultiPolygon":
                for poly in geom.geoms:
                    x, y = poly.exterior.xy
                    ax.plot(x, y, color="black", linewidth=0.6, alpha=1.0)

    ax.set_xlim(xi_wgs.min(), xi_wgs.max())
    ax.set_ylim(yi_wgs.min(), yi_wgs.max())

    plt.savefig(contour_path, bbox_inches='tight', pad_inches=0, transparent=True)
    plt.close(fig)

    bounds = [[float(yi_wgs.min()), float(xi_wgs.min())], [float(yi_wgs.max()), float(xi_wgs.max())]]
    print(f"✅ Contour image (transparent voids) from {src}: {contour_path}  bounds={bounds}")
    return bounds, contour_path

# -------------------------------
# KML GroundOverlay writer
# -------------------------------

def write_kml_ground_overlay(
    image_filename: str,
    bounds,
    kml_filename: str = "contour_overlay.kml",
    name: str = "Contour Overlay"
):
    """
    bounds = [[lat_min, lon_min], [lat_max, lon_max]]
    """
    (lat_min, lon_min), (lat_max, lon_max) = bounds

    kml = f"""<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
  <GroundOverlay>
    <name>{name}</name>
    <Icon>
      <href>{image_filename}</href>
    </Icon>
    <LatLonBox>
      <north>{lat_max}</north>
      <south>{lat_min}</south>
      <east>{lon_max}</east>
      <west>{lon_min}</west>
    </LatLonBox>
  </GroundOverlay>
</kml>
"""

    with open(kml_filename, "w", encoding="utf-8") as f:
        f.write(kml)

    print(f"✅ Wrote KML overlay: {kml_filename}")

    # Package KML + image into a KMZ (ZIP archive)
    kmz_filename = kml_filename.replace(".kml", ".kmz")
    with zipfile.ZipFile(kmz_filename, "w", zipfile.ZIP_DEFLATED) as kmz:
        kmz.write(kml_filename, arcname=os.path.basename(kml_filename))
        if os.path.exists(image_filename):
            kmz.write(image_filename, arcname=os.path.basename(image_filename))
    print(f"✅ Wrote KMZ overlay: {kmz_filename}")

# -------------------------------
# Simple raster Folium map (used if --with-raster)
# -------------------------------
def create_folium_map_with_contour(
    image_path: str,
    image_bounds: List[List[float]],
    output_html: str = "folium_map.html",
    base_tiles: str = "CartoDB positron",
    opacity: float = 0.6
) -> folium.Map:
    (lat_min, lon_min), (lat_max, lon_max) = image_bounds
    center_lat = (lat_min + lat_max) / 2.0
    center_lon = (lon_min + lon_max) / 2.0

    m = folium.Map(location=[center_lat, center_lon], zoom_start=13, tiles=base_tiles)
    ImageOverlay(
        name="Contour Overlay",
        image=image_path,
        bounds=image_bounds,
        opacity=opacity,
        interactive=True,
        cross_origin=False,
        zindex=2
    ).add_to(m)
    folium.LayerControl(collapsed=False).add_to(m)
    m.fit_bounds([[lat_min, lon_min], [lat_max, lon_max]])
    m.save(output_html)
    return m


# -------------------------------
# Pipeline (build + folium + outputs)
# -------------------------------
def build_pipeline(
    csv_path: str = "InputData.csv",
    min_samples: int = 10,
    min_cell_samples: int = 4,
    join_predicate: str = "intersects",
    output_centroids_csv: str = "output_centroids.csv",
    folium_html: str = "folium_geojson_only.html",
    with_raster: bool = True,
    raster_png: str = "contour_overlay.png",
    raster_html: str = "folium_map.html",
    do_static_plots: bool = True,
    no_borders: bool = True,
    folium_output_fundamentals_csv: str = "folium_output_fundamentals.csv",
) -> BuildArtifacts:
    gdf_points = load_points_csv(csv_path)

    subregions = build_subregion_gdf(gdf_points, min_samples=min_samples)
    subregions = compute_average_temperature(subregions, gdf_points, predicate=join_predicate)

    # Filter out rectangles with < min_cell_samples samples
    initial_count = len(subregions)
    subregions = subregions[subregions['sample_count'] >= min_cell_samples].copy()
    final_count = len(subregions)
    print(f"✅ Filtered subregions: Dropped {initial_count - final_count} rectangles with < {min_cell_samples} samples.")

    print(f"[DIAGNOSTIC] Final subregion polygon count: {len(subregions)}")
    print(f"[DIAGNOSTIC] Raw point count: {len(gdf_points)}")

    subregions = add_centroids_wgs84(subregions)
    write_fundamentals_csv(subregions, folium_output_fundamentals_csv)

    # Centroids CSV
    out = subregions[['centroid', 'avg_temperature']].dropna().copy()
    out['longitude'] = out['centroid'].apply(lambda p: p.x)
    out['latitude'] = out['centroid'].apply(lambda p: p.y)
    out[['longitude', 'latitude', 'avg_temperature']].to_csv(output_centroids_csv, index=False)
    print(f"✅ Wrote centroids CSV: {output_centroids_csv}")

    # Folium vector map with contour + mask overlays
    create_folium_map_with_layers(
        subregion_gdf=subregions,
        output_html=folium_html,
        points_gdf=gdf_points,
        points_as_cluster=False,
        point_radius=2,
        add_centroid_contour=True,
        centroid_contour_png="centroid_contour.png",
        centroid_contour_opacity=0.55,
        base_tiles="CartoDB dark_matter"
    )
    print(f"✅ Wrote Folium map (GeoJSON): {folium_html}")

    image_bounds = None
    if with_raster:
        image_bounds, _mask = save_contour_image(subregions, image_filename=raster_png, output_dir=".", no_borders=no_borders)
        create_folium_map_with_contour(raster_png, image_bounds, output_html=raster_html)
        print(f"✅ Wrote Folium raster map: {raster_html}")

    # Optional static plots
    if do_static_plots:
        plot_temperature_colored_subregions(subregions, no_borders=no_borders)
        plot_rectangles_and_contours(subregions, no_borders=no_borders)
        plot_contour_only(subregions)

    return BuildArtifacts(
        gdf_points=gdf_points,
        subregions=subregions,
        image_bounds=image_bounds,
        contour_png=(raster_png if with_raster else None)
    )


# -------------------------------
# CLI
# -------------------------------
def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build temperature grid and Folium layers from point data.")
    p.add_argument("--csv", dest="csv_path", default="InputData.csv", help="Path to input CSV (with longitude, latitude, temperature).")
    p.add_argument("--min-samples", type=int, default=10, help="Minimum samples required to subdivide a region.")
    p.add_argument("--min-cell-samples", type=int, default=4, help="Minimum samples required to keep a subregion cell.")    
    p.add_argument("--predicate", default="intersects", choices=["intersects", "within"], help="Spatial join predicate for points→polygons.")
    p.add_argument("--output-centroids", default="intermediate_centroids.csv", help="Output CSV for centroid long/lat and avg temperature.")
    p.add_argument("--folium-html", default="folium_geojson_only.html", help="Output HTML for Folium GeoJSON map.")
    p.add_argument("--with-raster", action="store_true", help="Also render transparent contour PNG and a raster Folium map.")
    p.add_argument("--raster-png", default="contour_overlay.png", help="Filename for saved transparent contour PNG.")
    p.add_argument("--raster-html", default="folium_map.html", help="Output HTML for Folium raster map.")
    p.add_argument("--no-static-plots", action="store_true", help="Disable Matplotlib static plots.")
    p.add_argument("--no-borders", dest="no_borders", action="store_true", default=True, help="Hide rectangle borders in static plots (default).")
    p.add_argument("--show-borders", dest="no_borders", action="store_false", help="Show rectangle borders in static plots.")
    p.add_argument("--folium-output-fundamentals", default="folium_output_fundamentals.csv",
                   help="Output CSV of per-subregion fundamentals for ArcGIS "
                        "(avg_lat, avg_lon, average_temperature_F, standard_deviation_F, number_of_data_points).")
    return p.parse_args()


def main() -> None:
    args = _parse_args()

    artifacts = build_pipeline(
        csv_path=args.csv_path,
        min_samples=args.min_samples,
        min_cell_samples=args.min_cell_samples,
        join_predicate=args.predicate,
        output_centroids_csv=args.output_centroids,
        folium_html=args.folium_html,
        with_raster=args.with_raster,
        raster_png=args.raster_png,
        raster_html=args.raster_html,
        do_static_plots=not args.no_static_plots,
        no_borders=args.no_borders,
        folium_output_fundamentals_csv=args.folium_output_fundamentals,
    )

    # Prefer bounds already computed during the pipeline (if with_raster was enabled)
    image_bounds = artifacts.image_bounds

    # If with_raster was not enabled, generate the contour image now
    if image_bounds is None:
        image_bounds, _ = save_contour_image(
            artifacts.subregions,
            image_filename="contour_overlay.png",
            output_dir=".",
            no_borders=args.no_borders
        )

    write_kml_ground_overlay(
        image_filename="contour_overlay.png",
        bounds=image_bounds,
        kml_filename="contour_overlay.kml"
    )


if __name__ == "__main__":
    # Removed the trailing dot from the original snippet which was: main().
    main()

############################################################################
#. Some example usage instructions:

# To run the pipeline with default parameters:

# python RecursiveSubdivisionTemperatureGrid.py --csv <file-with-InputData.csv> 
#
