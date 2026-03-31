
import argparse
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import numpy as np
import json
import folium
import branca.colormap as cm
from pathlib import Path


def make_folium_map(df: "pd.DataFrame", x_col: str, csv_path: str, output_map_html: str) -> str:
    """Build a folium temperature map from df and save it. Returns the rendered HTML string."""
    color_table_min = float(np.percentile(df["corrected_temperature_f"].dropna(), 5))
    color_table_max = float(np.percentile(df["corrected_temperature_f"].dropna(), 95))
    dtemp = (color_table_max - color_table_min) / 4
    index = [color_table_min + i * dtemp for i in range(5)]

    colormap = cm.LinearColormap(
        colors=["blue", "cyan", "green", "yellow", "red"],
        index=index,
        vmin=color_table_min,
        vmax=color_table_max,
    )
    colormap.caption = "Temperature (°F)"
    colormap.width = 400
    colormap.height = 40

    center_lat = (df["Latitude"].min() + df["Latitude"].max()) / 2
    center_lon = (df["Longitude"].min() + df["Longitude"].max()) / 2

    m = folium.Map(location=[center_lat, center_lon], zoom_start=14, control_scale=True,
                   width="100%", height="100%")

    folium.TileLayer(
        tiles="https://{s}.google.com/vt/lyrs=s&x={x}&y={y}&z={z}",
        attr="Google Satellite",
        name="Satellite View",
        subdomains=["mt0", "mt1", "mt2", "mt3"],
        overlay=False,
        control=True,
    ).add_to(m)
    folium.TileLayer("OpenStreetMap", name="Standard Map").add_to(m)

    m.get_root().html.add_child(folium.Element(f"""
        <style>
        .leaflet-control .legend text {{
            font-size: 18px !important;
            font-weight: bold !important;
        }}
        .map-info {{
            position: absolute;
            top: 80px;
            right: 10px;
            background: rgba(255,255,255,0.95);
            padding: 8px 12px;
            border: 2px solid rgba(0,0,0,0.3);
            border-radius: 8px;
            font-family: Arial, sans-serif;
            font-size: 12px;
            color: #333;
            z-index: 1000;
            box-shadow: 0 2px 4px rgba(0,0,0,0.2);
            min-width: 200px;
        }}
        .map-info .title {{
            font-size: 14px;
            font-weight: bold;
            margin-bottom: 6px;
            color: #0066cc;
            border-bottom: 1px solid #ddd;
            padding-bottom: 2px;
        }}
        .map-info .row {{
            display: flex;
            justify-content: space-between;
            margin: 2px 0;
        }}
        .map-info .lbl {{ font-weight: bold; color: #444; }}
        .map-info .val {{ color: #666; text-align: right; }}
        </style>
        <div class="map-info">
            <div class="title">Data Info</div>
            <div class="row">
                <span class="lbl">File:</span>
                <span class="val">{Path(csv_path).name}</span>
            </div>
            <div class="row">
                <span class="lbl">Rows:</span>
                <span class="val">{len(df)}</span>
            </div>
            <div class="row">
                <span class="lbl">Temp min (5%):</span>
                <span class="val">{color_table_min:.2f} °F</span>
            </div>
            <div class="row">
                <span class="lbl">Temp max (95%):</span>
                <span class="val">{color_table_max:.2f} °F</span>
            </div>
        </div>
    """))

    # Build one FeatureGroup per SourceFile so each can be toggled independently.
    if "SourceFile" in df.columns:
        source_files = df["SourceFile"].unique().tolist()
    else:
        source_files = [None]

    for sf in source_files:
        group_name = str(sf) if sf is not None else "Temperature Data"
        group = folium.FeatureGroup(name=group_name, show=True, overlay=True)
        subset = df[df["SourceFile"] == sf] if sf is not None else df
        for _, row in subset.iterrows():
            temp = row["corrected_temperature_f"]
            if pd.isna(temp) or pd.isna(row["Latitude"]) or pd.isna(row["Longitude"]):
                continue
            fill_color = colormap(temp)
            sf_line = f"Source: {sf}<br>" if sf is not None else ""
            popup_html = (
                f"{sf_line}"
                f"Row: {int(row['local_row_number'])}<br>"
                f"Time: {row[x_col]}<br>"
                f"Elapsed: {row['time_since_start']}<br>"
                f"Corrected Temp: {temp:.2f} °F<br>"
                f"Relative Humidity: {row['Humidity (%)']:.1f} %<br>"
                f"VPD: {row['vpd_kPa']:.4f} kPa<br>"
                f"Travel dist: {row['travel_distance_km']:.2f} km<br>"
                f"Lat: {row['Latitude']:.6f}<br>"
                f"Lon: {row['Longitude']:.6f}<br>"
                f"<a href=\"https://www.google.com/maps?q=&layer=c&cbll={row['Latitude']},{row['Longitude']}\" "
                f"target=\"_blank\">Open in Street View</a>"
            )
            folium.CircleMarker(
                location=[row["Latitude"], row["Longitude"]],
                radius=6,
                color=fill_color,
                fill=True,
                fill_color=fill_color,
                fill_opacity=0.8,
                popup=folium.Popup(popup_html, max_width=300),
            ).add_to(group)
        group.add_to(m)

    colormap.add_to(m)
    folium.LayerControl(position="topleft", collapsed=False).add_to(m)

    map_var = m.get_name()
    m.get_root().script.add_child(folium.Element(f"""
        (function() {{
            var _highlightMarker = null;
            window.addEventListener('message', function(e) {{
                if (!e.data || e.data.type !== 'highlight_point') return;
                var lat = e.data.lat;
                var lon = e.data.lon;
                var mapObj = window['{map_var}'];
                if (!mapObj) return;
                if (_highlightMarker) {{
                    _highlightMarker.setLatLng([lat, lon]);
                }} else {{
                    _highlightMarker = L.circleMarker([lat, lon], {{
                        radius: 10,
                        color: 'black',
                        weight: 3,
                        fillOpacity: 0,
                        interactive: false
                    }}).addTo(mapObj);
                }}
                mapObj.panTo([lat, lon]);
            }});
        }})();
    """))

    html_map = m.get_root().render()
    Path(output_map_html).write_text(html_map, encoding="utf-8")
    print(f"Folium map saved to: {output_map_html}")
    return html_map


def make_plot(
    csv_path: str = "RHS031126_combined_data_with_corrections.csv",
    distanceflag: int = 1,
    halfgraph: bool = True,
    output_augmented_csv: str = "Augmented_data_with_new_row_and_distance.csv",
    output_map_html: str = "temperature_map.html",
    output_vpd_html: str = "vpd_vs_travel_distance.html",
    output_combined_html: str = "combined_temperature_plots.html",
) -> None:
    data_path = Path(csv_path)
    if not data_path.exists():
        raise FileNotFoundError(f"CSV file not found: {data_path}")

    df = pd.read_csv(data_path)

    if "corrected_temperature_f" not in df.columns:
        raise ValueError("Column 'corrected_temperature_f' was not found in the CSV.")

    if not {"Latitude", "Longitude"}.issubset(df.columns):
        raise ValueError("Columns 'Latitude' and 'Longitude' are required in the CSV.")

    if "LocalTime" in df.columns:
        x_col = "LocalTime"
        df[x_col] = pd.to_datetime(df[x_col], errors="coerce")
    elif {"Local Date", "Local Time"}.issubset(df.columns):
        x_col = "Local DateTime"
        df[x_col] = pd.to_datetime(
            df["Local Date"].astype(str) + " " + df["Local Time"].astype(str),
            errors="coerce",
        )
    elif "Local Time" in df.columns:
        x_col = "Local Time"
    else:
        raise ValueError("No local time column found (expected 'LocalTime' or 'Local Time').")

    lat = pd.to_numeric(df["Latitude"], errors="coerce")
    lon = pd.to_numeric(df["Longitude"], errors="coerce")

    if pd.isna(lat.iloc[0]) or pd.isna(lon.iloc[0]):
        raise ValueError("First row must contain valid Latitude and Longitude for origin.")

    lat0 = float(lat.iloc[0])
    lon0 = float(lon.iloc[0])

    earth_radius_m = 6371000.0
    lat_rad = np.deg2rad(lat)
    lon_rad = np.deg2rad(lon)
    lat0_rad = np.deg2rad(lat0)
    lon0_rad = np.deg2rad(lon0)

    df["local_x_m"] = (lon_rad - lon0_rad) * np.cos(lat0_rad) * earth_radius_m
    df["local_y_m"] = (lat_rad - lat0_rad) * earth_radius_m
    df["distance_from_first_m"] = np.sqrt(df["local_x_m"] ** 2 + df["local_y_m"] ** 2)

    step_x_m = (lon_rad - lon_rad.shift(1)) * np.cos(lat0_rad) * earth_radius_m
    step_y_m = (lat_rad - lat_rad.shift(1)) * earth_radius_m
    step_m = np.sqrt(step_x_m ** 2 + step_y_m ** 2).fillna(0)
    df["travel_distance"] = step_m.cumsum().round(3)
    df["travel_distance_km"] = (df["travel_distance"] / 1000).round(2)

    t_c = pd.to_numeric(df["corrected_temperature_c"], errors="coerce")
    rh = pd.to_numeric(df["Humidity (%)"], errors="coerce")
    es = 0.6108 * np.exp(17.27 * t_c / (t_c + 237.3))
    df["vpd_kPa"] = (es * (1 - rh / 100)).round(4)

    if distanceflag == 1:
        # Distance from the first (origin) point
        df["distance_from_first_km"] = (df["distance_from_first_m"] / 1000).round(3)
    else:
        # Cumulative distance traveled along the traverse path
        x_m = (lon_rad - lon_rad.shift(1)) * np.cos(lat0_rad) * earth_radius_m
        y_m = (lat_rad - lat_rad.shift(1)) * earth_radius_m
        step_m = np.sqrt(x_m ** 2 + y_m ** 2).fillna(0)
        df["distance_from_first_km"] = (step_m.cumsum() / 1000).round(3)

    if "SourceFile" in df.columns:
        unique_sources = df["SourceFile"].dropna().unique().tolist()
        label_away = unique_sources[0] if len(unique_sources) > 0 else "moving away"
        label_back = unique_sources[1] if len(unique_sources) > 1 else "backtracking"
    else:
        label_away = "moving away"
        label_back = "backtracking"

    distance_delta = df["distance_from_first_km"].diff()
    df["movement_direction"] = np.where(distance_delta < 0, "moving toward", label_away)

    if "rownumber" in df.columns:
        df["rownumber"] = np.arange(1, len(df) + 1)

    df["local_row_number"] = np.arange(1, len(df) + 1)

    time_values = pd.to_datetime(df[x_col], errors="coerce")
    first_row_time = time_values.iloc[0]
    if pd.isna(first_row_time):
        first_row_time = time_values.dropna().iloc[0]
    elapsed_seconds = (time_values - first_row_time).dt.total_seconds().round()

    def format_elapsed_mmss(seconds: float) -> str:
        if pd.isna(seconds):
            return ""
        total_seconds = int(seconds)
        minutes, secs = divmod(total_seconds, 60)
        return f"{minutes}:{secs:02d}"

    df["time_since_start"] = elapsed_seconds.apply(format_elapsed_mmss)

    time_plot_df = df[
        [
            x_col,
            "corrected_temperature_f",
            "movement_direction",
            "distance_from_first_km",
            "time_since_start",
            "local_row_number",
        ]
    ].dropna()

    fig = px.scatter(
        time_plot_df,
        x=x_col,
        y="corrected_temperature_f",
        color="movement_direction",
        custom_data=["local_row_number", "time_since_start", "distance_from_first_km"],
        color_discrete_map={label_away: "green", "moving toward": "red"},
        title="Corrected Temperature (°F) vs Local Time",
        labels={
            x_col: "Local Time",
            "corrected_temperature_f": "Corrected Temperature (°F)",
            "movement_direction": "",
        },
    )

    fig.update_traces(
        mode="markers",
        marker={"size": 5, "symbol": "circle"},
        hovertemplate=(
            "Row: %{customdata[0]}<br>"
            "Elapsed time (min:sec): %{customdata[1]}<br>"
            "Corrected Temperature (°F): %{y:.2f}<br>"
            "Distance from start (km): %{customdata[2]:.3f}<extra></extra>"
        ),
    )
    fig.update_layout(hovermode="closest", legend_title_text="")

    df.to_csv(output_augmented_csv, index=False)
    print(f"Augmented CSV saved to: {output_augmented_csv}")

    if distanceflag == 1:
        dist_x_col = "distance_from_first_km"
        dist_title = "Corrected Temperature (°F) vs Distance from First Point"
        dist_x_label = "Distance from First Point (km)"
    else:
        dist_x_col = "travel_distance_km"
        dist_title = "Corrected Temperature (°F) vs Total Travel Distance from First Point"
        dist_x_label = "Travel Distance (km)"

    distance_plot_df = df[
        [
            "time_since_start",
            dist_x_col,
            "corrected_temperature_f",
            "movement_direction",
            "local_row_number",
            "Latitude",
            "Longitude",
        ]
    ].dropna()

    distance_fig = px.scatter(
        distance_plot_df,
        x=dist_x_col,
        y="corrected_temperature_f",
        color="movement_direction",
        custom_data=["local_row_number", "time_since_start"],
        color_discrete_map={label_away: "green", "moving toward": "red"},
        title=dist_title,
        labels={
            dist_x_col: dist_x_label,
            "corrected_temperature_f": "Corrected Temperature (°F)",
            "movement_direction": "",
        },
    )

    distance_fig.update_traces(
        mode="markers",
        marker={"size": 5, "symbol": "circle"},
        hovertemplate=(
            "Row: %{customdata[0]}<br>"
            "Elapsed time (min:sec): %{customdata[1]}<br>"
            "Distance from start (km): %{x:.3f}<br>"
            "Corrected Temperature (°F): %{y:.2f}<extra></extra>"
        ),
    )

    if distanceflag == 2:
        max_travel = df["travel_distance_km"].max()
        df["backtrack_distance_km"] = (max_travel - df["travel_distance_km"]).round(2)
        backtrack_df = df[
            ["backtrack_distance_km", "corrected_temperature_f", "time_since_start", "local_row_number"]
        ].dropna()
        distance_fig.add_trace(
            go.Scatter(
                x=backtrack_df["backtrack_distance_km"],
                y=backtrack_df["corrected_temperature_f"],
                mode="markers",
                marker={"size": 5, "symbol": "circle", "color": "red"},
                name=label_back,
                customdata=backtrack_df[["local_row_number", "time_since_start"]].values,
                hovertemplate=(
                    "Row: %{customdata[0]}<br>"
                    "Elapsed time (min:sec): %{customdata[1]}<br>"
                    "Distance from last point (km): %{x:.3f}<br>"
                    "Corrected Temperature (°F): %{y:.2f}<extra></extra>"
                ),
            )
        )

    vpd_df = df[["travel_distance_km", "vpd_kPa", "local_row_number", "time_since_start"]].dropna()
    distance_fig.add_trace(
        go.Scatter(
            x=vpd_df["travel_distance_km"],
            y=vpd_df["vpd_kPa"],
            mode="markers",
            marker={"size": 4, "symbol": "circle", "color": "blue"},
            name="VPD (kPa)",
            yaxis="y2",
            customdata=vpd_df[["local_row_number", "time_since_start"]].values,
            hovertemplate=(
                "Row: %{customdata[0]}<br>"
                "Elapsed time (min:sec): %{customdata[1]}<br>"
                "Travel distance (km): %{x:.3f}<br>"
                "VPD (kPa): %{y:.4f}<extra></extra>"
            ),
        )
    )
    x_max = distance_plot_df[dist_x_col].max()
    xaxis_range = [0, x_max / 2] if halfgraph else [0, x_max]
    distance_fig.update_layout(
        hovermode="closest",
        legend_title_text="",
        xaxis_range=xaxis_range,
        yaxis2=dict(
            title="Vapor Pressure Deficit (kPa)",
            overlaying="y",
            side="right",
            showgrid=False,
        ),
    )

    folium_html = make_folium_map(df, x_col, csv_path, output_map_html)

    vpd_plot_df = df[["travel_distance_km", "vpd_kPa", "local_row_number", "time_since_start"]].dropna()
    vpd_fig = px.scatter(
        vpd_plot_df,
        x="travel_distance_km",
        y="vpd_kPa",
        custom_data=["local_row_number", "time_since_start"],
        title="Vapor Pressure Deficit (kPa) vs Total Travel Distance",
        labels={
            "travel_distance_km": "Total Travel Distance from First Point (km)",
            "vpd_kPa": "Vapor Pressure Deficit (kPa)",
        },
    )
    vpd_fig.update_traces(
        mode="markers",
        marker={"size": 5, "symbol": "circle", "color": "steelblue"},
        hovertemplate=(
            "Row: %{customdata[0]}<br>"
            "Elapsed time (min:sec): %{customdata[1]}<br>"
            "Travel distance (km): %{x:.3f}<br>"
            "VPD (kPa): %{y:.4f}<extra></extra>"
        ),
    )
    vpd_fig.update_layout(hovermode="closest")
    vpd_fig.write_html(output_vpd_html, include_plotlyjs="cdn")
    print(f"VPD plot saved to: {output_vpd_html}")

    fig2_json = pio.to_json(distance_fig, pretty=False)
    lower_point_lookup = {
        str(int(row.local_row_number)): {
            "x": float(getattr(row, dist_x_col)),
            "y": float(row.corrected_temperature_f),
        }
        for row in distance_plot_df.itertuples(index=False)
    }
    lower_point_lookup_json = json.dumps(lower_point_lookup)

    row_to_latlon = {
        str(int(row.local_row_number)): {
            "lat": float(row.Latitude),
            "lon": float(row.Longitude),
        }
        for row in distance_plot_df.itertuples(index=False)
    }
    row_to_latlon_json = json.dumps(row_to_latlon)

    if distanceflag == 1:
        fig1_json = pio.to_json(fig, pretty=False)
        plot1_div = '<div id="plot1" style="width: 100%; height: 38vh;"></div>'
        plot2_height = "38vh"
        plot1_script = f"""
        const fig1 = {fig1_json};
        const plot1 = document.getElementById('plot1');"""
        plot1_render = "Plotly.newPlot(plot1, fig1.data, fig1.layout, {responsive: true}),"
        plot1_click = """
            plot1.on('plotly_click', function(evt) {
                const point = evt.points && evt.points[0];
                if (!point || !point.customdata) return;
                const rowId = point.customdata[0];
                Plotly.Fx.hover(plot1, [{ curveNumber: point.curveNumber, pointNumber: point.pointNumber }]);
                highlightPlot2PointByRowId(rowId);
                setTimeout(function() { highlightPlot2PointByRowId(rowId); }, 0);
            });"""
    else:
        plot1_div = ""
        plot2_height = "48vh"
        plot1_script = ""
        plot1_render = ""
        plot1_click = ""

    combined_html = f"""<!DOCTYPE html>
<html lang=\"en\">
<head>
    <meta charset=\"UTF-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />
    <title>Combined Temperature Plots</title>
    <script src=\"https://cdn.plot.ly/plotly-2.35.2.min.js\"></script>
</head>
<body>
    {plot1_div}
    <div id=\"plot2\" style=\"width: 100%; height: {plot2_height};\"></div>
    <iframe srcdoc=\"{folium_html.replace(chr(34), '&quot;')}\" style=\"width: 100%; height: 50vh; border: none;\"></iframe>

    <script>
        {plot1_script}
        const fig2 = {fig2_json};
        const lowerPointByRow = {lower_point_lookup_json};
        const rowToLatLon = {row_to_latlon_json};
        const plot2 = document.getElementById('plot2');
        const mapIframe = document.querySelector('iframe');

        let plot2HighlightTraceIndex = null;

        function ensurePlot2HighlightTrace() {{
            if (plot2HighlightTraceIndex !== null) return Promise.resolve();
            return Plotly.addTraces(plot2, {{
                x: [],
                y: [],
                mode: 'markers',
                marker: {{
                    size: 16,
                    symbol: 'circle-open',
                    color: 'black',
                    line: {{ color: 'black', width: 2 }}
                }},
                hoverinfo: 'skip',
                showlegend: false,
                name: 'selection'
            }}).then(function() {{
                plot2HighlightTraceIndex = plot2.data.length - 1;
            }});
        }}

        function highlightPlot2PointByRowId(rowId) {{
            const key = String(parseInt(rowId, 10));
            const point = lowerPointByRow[key];
            if (!point) return Promise.resolve();
            return ensurePlot2HighlightTrace().then(function() {{
                return Plotly.restyle(
                    plot2,
                    {{ x: [[point.x]], y: [[point.y]], visible: true }},
                    [plot2HighlightTraceIndex]
                );
            }});
        }}

        Promise.all([
            {plot1_render}
            Plotly.newPlot(plot2, fig2.data, fig2.layout, {{responsive: true}})
        ]).then(function() {{
            {plot1_click}
            plot2.on('plotly_click', function(evt) {{
                const point = evt.points && evt.points[0];
                if (!point || !point.customdata) return;
                const rowId = String(parseInt(point.customdata[0], 10));
                const ll = rowToLatLon[rowId];
                if (ll && mapIframe && mapIframe.contentWindow) {{
                    mapIframe.contentWindow.postMessage(
                        {{type: 'highlight_point', lat: ll.lat, lon: ll.lon}}, '*'
                    );
                }}
            }});
        }});
    </script>
</body>
</html>
"""
    Path(output_combined_html).write_text(combined_html, encoding="utf-8")
    print(f"Combined interactive plot saved to: {output_combined_html}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plot corrected temperature traverse data.")
    parser.add_argument(
        "--csv",
        default="RHS031126_combined_data_with_corrections.csv",
        help="Path to the input CSV data file.",
    )
    parser.add_argument(
        "--distanceflag",
        type=int,
        choices=[1, 2],
        default=1,
        help="Distance calculation mode: 1 = distance from first point, 2 = cumulative distance traveled.",
    )
    parser.add_argument(
        "--halfgraph",
        type=lambda v: v.lower() not in ("false", "0", "no"),
        default=True,
        metavar="BOOL",
        help="If True (default), limit x-axis to half the maximum distance. Use --halfgraph false for full range.",
    )
    args = parser.parse_args()
    make_plot(csv_path=args.csv, distanceflag=args.distanceflag, halfgraph=args.halfgraph)
