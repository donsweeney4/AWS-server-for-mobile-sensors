import io
import re
import pandas as pd
import numpy as np
import boto3
import folium
import branca.colormap as cm
import plotly.express as px
import plotly.io as pio
import logging
from quart import has_request_context, session
from sklearn.linear_model import LinearRegression as SklearnLinearRegression

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

#// #########################################################################
def LinearRegression(df_step):
    # Drop rows with missing data
    df = df_step[['Timestamp', 'Temperature (°C)']].dropna().copy()

    # Convert datetime to numeric timestamp in seconds
    df['time_seconds'] = df['Timestamp'].astype('datetime64[s]').astype(int)

    X = df['time_seconds'].values.reshape(-1, 1)
    y = df['Temperature (°C)'].values

    # Fit linear regression model
    model = SklearnLinearRegression()
    model.fit(X, y)

    slope = model.coef_[0]
    intercept = model.intercept_

    logger.info(f"Linear regression slope in C/sec: {slope:.6f} °C/sec")
    logger.info(f"Linear regression slope in F/sec: {slope:.6f}*9/5 °C/sec")
    logger.info(f"Intercept: {intercept:.2f} °C")
    return slope

#// ##########################################################################
def get_s3_objects(bucket_name, root_name):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=root_name)
    csv_pattern = re.compile(rf'^{re.escape(root_name)}_\d{{3}}\.csv$')
    csv_keys = []

    logger.info(f"🔍 Searching for CSV files in S3 bucket '{bucket_name}' with prefix '{root_name}'")

    for page in page_iterator:
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.csv') and csv_pattern.match(key):
                logger.info(f"✅ Found matching CSV: {key}")
                csv_keys.append(key)

    logger.info(f"📦 Total matching CSV files for '{root_name}': {len(csv_keys)}")
    return csv_keys

#// ##########################################################################
def save_to_s3(bucket_name, key, content, is_binary=False):
    s3 = boto3.client('s3')
    body = content if is_binary else content.encode('utf-8')
    s3.put_object(Bucket=bucket_name, Key=key, Body=body)

#// ##########################################################################
def mainProcessData(root_names,
                    start_time_adjustment_minutes=0.0,
                    end_time_adjustment_minutes=0.0,
                    cutoff_speed_MPH=1.0,
                    slope_option=1,
                    temperature_drift_f=0.0,
                    color_table_min_quantile=5,
                    color_table_max_quantile=95,
                    solid_color=False):

    # Allow single string or list
    if isinstance(root_names, str):
        root_names = [root_names]

    logger.info(f"Starting mainProcessData with root_names={root_names}")
    bucket_name = 'urban-heat-island-data'

    # Gather all CSV keys matching any of the root_names
    csv_keys = []
    for rn in root_names:
        csv_keys.extend(get_s3_objects(bucket_name, rn))
    csv_keys = sorted(set(csv_keys))

    if not csv_keys:
        raise ValueError(f"No CSV files found in S3 bucket '{bucket_name}' for prefixes {root_names}.")

    # Time adjustments in ms
    start_ms = start_time_adjustment_minutes * 60 * 1000
    end_ms = end_time_adjustment_minutes * 60 * 1000

    # Prepare accumulators
    df_step5 = pd.DataFrame()
    df_step6 = pd.DataFrame()

    s3 = boto3.client('s3')
    for key in csv_keys:
        logger.info(f"📄 Processing file: {key}")
        obj = s3.get_object(Bucket=bucket_name, Key=key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()), header=0)
        df['SourceFile'] = key
        df.replace('', np.nan, inplace=True)

        # Convert timestamps
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='ms', errors='coerce')
        df = df.dropna(subset=['Timestamp'])

        # Filter by time window
        tmin, tmax = df['Timestamp'].min(), df['Timestamp'].max()
        start_ts = tmin + pd.Timedelta(milliseconds=start_ms)
        end_ts = tmax - pd.Timedelta(milliseconds=end_ms)
        df2 = df[(df['Timestamp'] > start_ts) & (df['Timestamp'] < end_ts)].copy()
        if df2.empty:
            raise ValueError(f"No data after time filtering for file {key}.")

        # Filter by speed
        if cutoff_speed_MPH < 1e-3:
            df3 = df2
        else:
            df3 = df2[df2['Speed (MPH)'] >= cutoff_speed_MPH].copy()
        if df3.empty:
            raise ValueError(f"No data after speed filtering for file {key}.")

        # Accumulate
        df_step5 = pd.concat([df_step5, df3], ignore_index=True)
        df_step6 = pd.concat([df_step6, df], ignore_index=True)

    if df_step5.empty:
        raise ValueError("No data available after filtering.")

    # Add time_delta
    df_step5['time_delta'] = (df_step5['Timestamp'] - df_step5['Timestamp'].min()).dt.total_seconds()

    # Determine temperature drift
    if slope_option == 1:
        temperature_drift_c = 0.0
        temperature_drift_f = 0.0
    elif slope_option == 2:
        temperature_drift_c = LinearRegression(df_step5)
        temperature_drift_f = temperature_drift_c * 9.0 / 5.0
        if has_request_context():
            session.temperature_drift_f = round(temperature_drift_f, 6)
    elif slope_option == 3:
        temperature_drift_f = temperature_drift_f
        temperature_drift_c = temperature_drift_f * 5.0 / 9.0
    else:
        raise ValueError(f"Invalid slope option: {slope_option}")

    # Apply corrections
    df_step5['temperature_correction_c'] = df_step5['time_delta'] * temperature_drift_c
    df_step5['corrected_temperature_c'] = (df_step5['Temperature (°C)'] - df_step5['temperature_correction_c']).round(2)
    df_step5['corrected_temperature_f'] = (df_step5['corrected_temperature_c'] * 9/5 + 32).round(2)

    # Calculate color table bounds
    cmin = np.percentile(df_step5['corrected_temperature_f'], color_table_min_quantile)
    cmax = np.percentile(df_step5['corrected_temperature_f'], color_table_max_quantile)
    df_step5['LocalTime'] = pd.to_datetime(df_step5['Timestamp'], unit='ms', origin='unix', utc=True)
    df_step5['LocalTime'] = df_step5['LocalTime'].dt.tz_convert('US/Pacific')

    # Create colormap
    index_vals = np.linspace(cmin, cmax, num=5)
    colormap = cm.LinearColormap(
        colors=['blue', 'cyan', 'green', 'yellow', 'red'],
        index=index_vals,
        vmin=cmin,
        vmax=cmax
    )
    colormap.caption = "Corrected Temperature (°F)"

    # Build Folium map
    center_lat = df_step5['Latitude'].mean()
    center_lon = df_step5['Longitude'].mean()
    m = folium.Map(location=[center_lat, center_lon], zoom_start=12, control_scale=True)
    folium.TileLayer('OpenStreetMap', name='Standard Map').add_to(m)
    folium.TileLayer(
        tiles='https://{s}.google.com/vt/lyrs=s&x={x}&y={y}&z={z}',
        attr='Google Satellite', name='Satellite View',
        subdomains=['mt0','mt1','mt2','mt3']
    ).add_to(m)
    # Custom CSS for legend
    m.get_root().html.add_child(folium.Element("""
        <style>
        .leaflet-control .legend text { font-size: 24px !important; font-weight: bold !important; }
        </style>
    """))

    # Add data points
    solid_colors = ['red','blue','green','purple','orange','darkred','lightred','beige','darkblue','darkgreen']
    for idx, key in enumerate(csv_keys, start=1):
        fg = folium.FeatureGroup(name=f"Sensor Data: {key}")
        subset = df_step5[df_step5['SourceFile'] == key]
        for i, row in subset.iterrows():
            if solid_color:
                color = solid_colors[idx % len(solid_colors)]
            else:
                color = colormap(row['corrected_temperature_f'])
            folium.CircleMarker(
                location=[row['Latitude'], row['Longitude']],
                radius=6, color=color, fill=True, fill_color=color,
                fill_opacity=0.5,
                tooltip=(
                    f"File: {key}<br>Local Time: {row['LocalTime']}<br>"
                    f"Corrected Temp: {row['corrected_temperature_f']} °F"
                )
            ).add_to(fg)
        fg.add_to(m)
    colormap.add_to(m)
    folium.LayerControl(position='topleft', collapsed=False).add_to(m)

    html_map = m.get_root().render()

    # Use combined identifier for filenames
    combined_id = "_".join(root_names)
    save_to_s3(bucket_name, f"{combined_id}_color_coded_temperature_map.html", html_map)

    # Figure 1: corrected temperature
    fig1 = px.line(df_step5, x='LocalTime', y='corrected_temperature_f', color='SourceFile')
    fig1.update_layout(
        title_text='Figure 1. Drift-Corrected Temperature over Time',
        xaxis_title='Local Time', yaxis_title='Corrected Temperature (°F)',
        legend=dict(x=0, y=0, xanchor='left', yanchor='bottom')
    )
    buf1 = io.StringIO(); pio.write_html(fig1, buf1, auto_open=False)
    save_to_s3(bucket_name, f"{combined_id}_fig1_corrected_temperature_map.html", buf1.getvalue())
    buf1.close()

    # Figure 2: raw temperature
    fig2 = px.line(df_step6, x='Timestamp', y='Temperature (°C)', color='SourceFile')
    fig2.update_layout(
        title_text='Figure 2. Uncorrected Temperature over Full Time',
        xaxis_title='Timestamp', yaxis_title='Temperature (°C)',
        legend=dict(x=0, y=0, xanchor='left', yanchor='bottom')
    )
    buf2 = io.StringIO(); pio.write_html(fig2, buf2, auto_open=False)
    save_to_s3(bucket_name, f"{combined_id}_fig2_uncorrected_temperature_map.html", buf2.getvalue())
    buf2.close()

    # Save CSV outputs
    save_to_s3(bucket_name, f"{combined_id}_combined_data.csv", df_step5.to_csv(index=False))
    reduced = df_step5[['Latitude','Longitude','Altitude (m)','corrected_temperature_f','Humidity (%)','SourceFile']]
    save_to_s3(bucket_name, f"{combined_id}_combined_data_reduced_columns.csv", reduced.to_csv(index=False))

    # Return key metrics
    duration_sec = df_step5['time_delta'].max()
    max_corrected = df_step5['corrected_temperature_f'].max()
    min_corrected = df_step5['corrected_temperature_f'].min()
    max_correction = temperature_drift_f * duration_sec
    logger.info("Processing complete.")
    return temperature_drift_f, duration_sec, max_correction, max_corrected, min_corrected

#// ###########################################################################
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('root_names', nargs='+', help='One or more root name prefixes')
    parser.add_argument('--start_time_adjustment_minutes', type=float, default=1.0)
    parser.add_argument('--end_time_adjustment_minutes', type=float, default=1.0)
    parser.add_argument('--cutoff_speed_MPH', type=float, default=1.0)
    parser.add_argument('--slope_option', type=int, default=1)
    parser.add_argument('--temperature_drift_f', type=float, default=0.0)
    parser.add_argument('--color_table_min_quantile', type=int, default=5)
    parser.add_argument('--color_table_max_quantile', type=int, default=95)
    parser.add_argument('--solid_color', action='store_true', help='Use solid color by route')
    parser.add_argument('--no-solid_color', action='store_false', dest='solid_color')
    parser.set_defaults(solid_color=False)

    args = parser.parse_args()
    mainProcessData(
        root_names=args.root_names,
        start_time_adjustment_minutes=args.start_time_adjustment_minutes,
        end_time_adjustment_minutes=args.end_time_adjustment_minutes,
        cutoff_speed_MPH=args.cutoff_speed_MPH,
        slope_option=args.slope_option,
        temperature_drift_f=args.temperature_drift_f,
        color_table_min_quantile=args.color_table_min_quantile,
        color_table_max_quantile=args.color_table_max_quantile,
        solid_color=args.solid_color
    )
