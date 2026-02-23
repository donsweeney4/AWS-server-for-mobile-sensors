# process_routes.py

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

    logger.info(f"📦 Total matching CSV files: {len(csv_keys)}")
    return csv_keys


#// ##########################################################################
def save_to_s3(bucket_name, key, content, is_binary=False):
    s3 = boto3.client('s3')
    body = content if is_binary else content.encode('utf-8')
    s3.put_object(Bucket=bucket_name, Key=key, Body=body)

#// ##########################################################################


def mainProcessData(root_name,
                    bucket_name,
                    start_time_adjustment_minutes=0.0,
                    end_time_adjustment_minutes=0.0,
                    cutoff_speed_MPH=1.0,
                    slope_option=1,
                    temperature_drift_f=0.0,
                    color_table_min_quantile=5,
                    color_table_max_quantile=95,
                    solid_color =False):

    logger.info(f"Starting mainProcessData with root_name={root_name}")
    #breakpoint()

    csv_keys = get_s3_objects(bucket_name, root_name)

    if not csv_keys:
        # Raise an error if no CSV files are found for the given root_name
        raise ValueError(f"No CSV files found in S3 bucket '{bucket_name}' for prefix '{root_name}'. Please check the campaign ID.")

    

    start_time_adjustment_ms = start_time_adjustment_minutes * 60 * 1000
    end_time_adjustment_ms = end_time_adjustment_minutes * 60 * 1000

    logger.info(f"Timestamp start adjustment ms: {start_time_adjustment_ms}, Timestamp end adjustment ms: {end_time_adjustment_ms}")
    logger.info(f"Slope option: {slope_option}")
    logger.info(f"Temperature Drift in degF/sec: {temperature_drift_f}")
    logger.info(f"Temperature Drift in degF/hr: {temperature_drift_f*3600.0}")
    logger.info(f"Cutoff speed in MPH: {cutoff_speed_MPH}")
    logger.info(f"Cutoff speed in m/s: {cutoff_speed_MPH * 0.44704}")
    logger.info(f"Color table min quantile: {color_table_min_quantile}")
    logger.info(f"Color table max quantile: {color_table_max_quantile}")
    logger.info(f"Solid color option: {solid_color}")

    solid_color_list = ['red', 'blue', 'green', 'purple', 'orange', 'darkred', 'lightred',
                        'beige', 'darkblue', 'darkgreen']
    
    #breakpoint()
    s3 = boto3.client('s3')
    df_step5 = pd.DataFrame()
    df_step6 = pd.DataFrame()
    i = 0

    for key in csv_keys:
        logger.info(f"📄 Processing file: {key}")

        obj = s3.get_object(Bucket=bucket_name, Key=key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()), header=0)
        df['SourceFile'] = key
        df.replace('', np.nan, inplace=True)

        # ✅ CONVERT Unix seconds to datetime
        try:
            df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='ms', errors='coerce')
            if df['Timestamp'].isna().any():
                logger.warning("⚠️ Some timestamps were invalid and converted to NaT.")
            df = df.dropna(subset=['Timestamp'])
        except Exception as e:
            logger.error(f"❌ Unexpected error while parsing timestamps: {e}")

        logger.info(f"df shape: {df.shape} (rows, columns)")

        df_min = df['Timestamp'].min()
        df_max = df['Timestamp'].max()
        timestamp_start = df_min + pd.Timedelta(milliseconds=start_time_adjustment_ms)
        timestamp_end = df_max - pd.Timedelta(milliseconds=end_time_adjustment_ms)
        df_step2 = df[(df['Timestamp'] > timestamp_start) & (df['Timestamp'] < timestamp_end)].copy()
        if df_step2.empty:
            logger.warning(f"⚠️ No data in {key} after time filtering. Skipping this file.")
            raise ValueError(f"No data available after time filtering in file {key}. Check the time adjustments or data quality.")
            continue

        logger.info(f"df_step2 shape: {df_step2.shape} (rows, columns)")

        if cutoff_speed_MPH < 1.e-3:  # If cutoff speed is very small (including 0) skip filtering
            logger.warning(f"⚠️ Cutoff speed is very small ({cutoff_speed_MPH} MPH). Skipping speed filtering.")
            df_step3 = df_step2.copy()
        else:
            logger.warning(f"⚠️ Cutoff speed  ({cutoff_speed_MPH} MPH). Filtering speed.")
            df_step3 = df_step2[df_step2['Speed (MPH)'] >= cutoff_speed_MPH].copy()

        if df_step3.empty:
            logger.warning(f"⚠️ No data in {key} after speed filtering. Skipping this file.")
            raise ValueError(f"No data available after speed filtering in file {key}. Check the cutoff speed or data quality.")
            continue

        logger.info(f"df_step3 shape: {df_step3.shape} (rows, columns)")

        df_step4 = df_step3.copy()
        df_step5 = pd.concat([df_step5, df_step4], ignore_index=True)
        df_step6 = pd.concat([df_step6, df], ignore_index=True)

    #######################################################################
    ### END OF LOOP PROCESSING EACH CSV FILE
    logger.info(f"Total rows of all files in the campaign: {df_step5.shape[0]}")
    #breakpoint()
    if df_step5.empty:
        raise ValueError("No data available after filtering. Check input filters or data.")

    ## Add colomn ofr time delta in seconds
    df_step5['time_delta'] = (df_step5['Timestamp'] - df_step5['Timestamp'].min()).dt.total_seconds()

    

    # Determine temperature drift based on user selection
    if slope_option == 1:   # Default option - o slope correction
        logger.error(f"No slope correction option selected. Using default temperature drift of 0.0 °C/sec.")
        temperature_drift_c = 0.0
        temperature_drift_f = 0.0
    elif slope_option == 2: # Linear regression option
        logger.info(f"   Using linear regression to calculate temperature drift for {len(df_step5)} data points.")
        temperature_drift_c = LinearRegression(df_step5)  # in deg C/sec
        logger.info(f"Temperature drift calculated: {temperature_drift_c:.6f} °C/sec")
        temperature_drift_f = temperature_drift_c * 9. / 5.
        logger.info(f"Temperature drift calculated: {temperature_drift_f:.6f} °F/sec")
        if has_request_context():
            session.temperature_drift_f = round(temperature_drift_f, 6)
    elif slope_option == 3: # User provided temperature drift value
        logger.info("   Using provided temperature drift value.")
       
        temperature_drift_c = temperature_drift_f * 5. / 9.
    else:
        raise ValueError(f"Invalid slope option selected: {slope_option}. Must be 1, 2, or 3.")
    
    logger.info(f"Temperature drift in degF/sec: {temperature_drift_f:.6f} F/sec")
    logger.info(f"Temperature drift in degC/sec: {temperature_drift_c:.6f} C/sec")
    logger.info(f"Temperature drift in degF/hr: {temperature_drift_f * 3600:.6f} F/hr")
    logger.info(f"Temperature drift in degC/hr: {temperature_drift_c * 3600:.6f} C/hr")

    logger.info(f"Maximum temperature correction in deg F applied to data: {temperature_drift_f * df_step5['time_delta'].max():.3f}F" )    
 
    df_step5['temperature_correction_c'] = df_step5['time_delta'] * temperature_drift_c
    df_step5['corrected_temperature_c'] = (df_step5['Temperature (°C)'] - df_step5['temperature_correction_c']).round(2)

    df_step5['corrected_temperature_f'] = df_step5['corrected_temperature_c'] * 9/5 + 32
            #  df_step5['Temperature (°F)'] = df_step5['Temperature (°C)'] * 9/5 + 32

    df_step5['corrected_temperature_f'] = df_step5['corrected_temperature_f'].round(2)
            #  df_step5['Temperature (°F)'] = df_step5['Temperature (°F)'].round(2)

    max_corrected_temperature_f = df_step5['corrected_temperature_f'].max()
    min_corrected_temperature_f = df_step5['corrected_temperature_f'].min()        

    color_table_min = np.percentile(df_step5['corrected_temperature_f'], color_table_min_quantile)
    color_table_max = np.percentile(df_step5['corrected_temperature_f'], color_table_max_quantile)

    df_step5['Timestamp'] = pd.to_datetime(df_step5['Timestamp'])
    df_step5['LocalTime'] = df_step5['Timestamp'].dt.tz_localize('UTC').dt.tz_convert('US/Pacific')

    dtemp = (color_table_max - color_table_min) / 4
    index = [color_table_min + i * dtemp for i in range(5)]

    ########################################################
    # Folium colormap —── MODIFIED: styling only (keep upper‑right) ──
    colormap = cm.LinearColormap(
        colors=['blue', 'cyan', 'green', 'yellow', 'red'],
        index=index,
        vmin=color_table_min,
        vmax=color_table_max
    )
    
    colormap.caption = "Temperature (°F)"
    colormap.width  = 400   # px length
    colormap.height = 40    # px thickness 
    ########################################################

    logger.info("Creating Folium map with sensor data...")

    center_lat = (df_step5['Latitude'].min() + df_step5['Latitude'].max()) / 2
    center_lon = (df_step5['Longitude'].min() + df_step5['Longitude'].max()) / 2

    m = folium.Map(location=[center_lat, center_lon], zoom_start=12, control_scale=True,width="100%",height="78%" )

    folium.TileLayer(
        tiles='https://{s}.google.com/vt/lyrs=s&x={x}&y={y}&z={z}',
        attr='Google Satellite',
        name='Satellite View',
        subdomains=['mt0', 'mt1', 'mt2', 'mt3'],
        overlay=False,
        control=True
    ).add_to(m)

    #  Inject custom CSS to style the caption text and add parameter info:
    m.get_root().html.add_child(folium.Element(f"""
        <style>
        /*
            Branca's colormap ends up in <div class="legend leaflet-control">,
            and the caption is rendered as an SVG <text> inside there.
            This selector bumps all <text> in that .legend up to 36px bold.
        */
        .leaflet-control .legend text {{
            font-size: 18px !important;
            font-weight: bold !important;
        }}
        /* Custom parameter display */
        .parameter-info {{
            position: absolute;
            top: 80px;
            right: 10px;
            background: rgba(255, 255, 255, 0.95);
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
        .parameter-info .title {{
            font-size: 14px;
            font-weight: bold;
            margin-bottom: 6px;
            color: #0066cc;
            border-bottom: 1px solid #ddd;
            padding-bottom: 2px;
        }}
        .parameter-info .param-row {{
            display: flex;
            justify-content: space-between;
            margin: 2px 0;
            padding: 1px 0;
        }}
        .parameter-info .param-label {{
            font-weight: bold;
            color: #444;
        }}
        .parameter-info .param-value {{
            color: #666;
            text-align: right;
        }}
        .params-toggle-btn {{
            position: absolute;
            top: 50px;
            right: 10px;
            background: rgba(255, 255, 255, 0.95);
            padding: 4px 10px;
            border: 2px solid rgba(0,0,0,0.3);
            border-radius: 6px;
            font-family: Arial, sans-serif;
            font-size: 12px;
            font-weight: bold;
            color: #0066cc;
            z-index: 1001;
            cursor: pointer;
            box-shadow: 0 2px 4px rgba(0,0,0,0.2);
        }}
        .params-toggle-btn:hover {{
            background: rgba(230, 240, 255, 0.98);
        }}
        </style>
        <button class="params-toggle-btn" id="paramsToggleBtn" onclick="toggleParamInfo()">Hide Params</button>
        <script>
        function toggleParamInfo() {{
            var panel = document.getElementById('paramInfoPanel');
            var btn = document.getElementById('paramsToggleBtn');
            if (panel.style.display === 'none') {{
                panel.style.display = 'block';
                btn.textContent = 'Hide Params';
            }} else {{
                panel.style.display = 'none';
                btn.textContent = 'Show Params';
            }}
        }}
        </script>
        <div class="parameter-info" id="paramInfoPanel">
            <div class="title">Processing Parameters</div>
            <div class="param-row">
                <span class="param-label">Location:</span>
                <span class="param-value">{bucket_name.replace('uhi-', '').replace('-', ' ').title()}</span>
            </div>
            <div class="param-row">
                <span class="param-label">ID:</span>
                <span class="param-value">{root_name}</span>
            </div>
            <div class="param-row">
                <span class="param-label">Start trim:</span>
                <span class="param-value">{start_time_adjustment_minutes:.1f} min</span>
            </div>
            <div class="param-row">
                <span class="param-label">End trim:</span>
                <span class="param-value">{end_time_adjustment_minutes:.1f} min</span>
            </div>
            <div class="param-row">
                <span class="param-label">Cutoff:</span>
                <span class="param-value">{cutoff_speed_MPH:.1f} MPH</span>
            </div>
            <div class="param-row">
                <span class="param-label">Drift correction:</span>
                <span class="param-value">{temperature_drift_f * 3600:.3f} °F/hr</span>
            </div>
            <div class="param-row">
                <span class="param-label">Color min:</span>
                <span class="param-value">{color_table_min_quantile}%</span>
            </div>
            <div class="param-row">
                <span class="param-label">Color max:</span>
                <span class="param-value">{color_table_max_quantile}%</span>
            </div>
        </div>
    """))

    folium.TileLayer('OpenStreetMap', name='Standard Map').add_to(m)

    logger.info(f"Adding sensor data to Folium map.solid_color: {solid_color}")
    #breakpoint()

    for key in csv_keys:
        group = folium.FeatureGroup(name=f"Sensor Data: {key}", show=True, overlay=True)
        df_key = df_step5[df_step5['SourceFile'] == key]

        logger.info(f"solid_color: {solid_color}")
        i=1;
        for _, row in df_key.iterrows():

            if solid_color:
                fill_color = solid_color_list[i % len(solid_color_list)]
                #  logger.info(f"Using solid color: {fill_color} for row {i}")
            else:
                fill_color = colormap(row['corrected_temperature_f'])

            popup_html = (
                f"File: {key}<br>"
                f"Local Time: {row['LocalTime']}<br>"
                f"Corrected Temp: {row['corrected_temperature_f']:.2f} °F<br>"
                f"Uncorrected Temp: {(row['Temperature (°C)'])*9/5+32:.2f} °F<br>"
                f"Humidity: {row['Humidity (%)']} %<br>"
                f"Speed: {row['Speed (MPH)']} MPH<br>"
                f"Lat: {row['Latitude']}<br>"
                f"Lon: {row['Longitude']}<br>"
                f"<a href=\"https://www.google.com/maps?q=&layer=c&cbll={row['Latitude']},{row['Longitude']}\" target=\"_blank\">Open in Street View</a>"
            )

            popup = folium.Popup(popup_html, max_width=300)


            folium.CircleMarker(
                location=[row['Latitude'], row['Longitude']],
                radius=6,
                color=fill_color,
                fill=True,
                fill_color=fill_color,
                fill_opacity=0.5,
                popup=popup,
            ).add_to(group)

        group.add_to(m)
        i += 1

    logger.info("Adding color map to Folium map...")

    # Simply add the colormap (it will stay in upper‑right by default):
    colormap.add_to(m)

    folium.LayerControl(position='topleft', collapsed=False).add_to(m)

    html_map = m.get_root().render()

    #html_filename = f"{root_name}_{'color_coded_route_map' if solid_color else 'color_coded_temperature_map'}.html"
    html_filename = f"{root_name}_{'color_coded_temperature_map'}.html"

    save_to_s3(bucket_name, html_filename, html_map) # Primary color coded html temperature map 

    ######################################################################
    # Figure 1 — temperature with time filtering and corrected temperature in deg F

    logger.info("Creating figure 1...")

    fig1 = px.line(df_step5, x='LocalTime', y='corrected_temperature_f', color='SourceFile')

    # ──────────── MODIFIED: move legend to lower‑left & control font size ────────────
    fig1.update_layout(
        title=dict(
            text='Sensor Temperatures over Accepted Time Window, includes drift correction, if any',
            font=dict(size=24),
            x=0.5,
            xanchor='center'
        ),
        font=dict(size=16),
        xaxis=dict(
            title='Local Time',
            tickformat='%H:%M:%S'
        ),
        yaxis=dict(
            title='Temperature (°F)',
            tickformat='.2f'
        ),
        legend_title_text='Sensor Files',
        margin=dict(l=20, r=20, t=80, b=20),
        hovermode='x unified',
        hoverlabel=dict(bgcolor='white', font_size=12, bordercolor='black'),
        legend=dict(
            x=0,            # far left
            y=0,            # bottom
            xanchor='left',
            yanchor='bottom',
            font=dict(size=12)
        )
    )
    
    # Add Processing Parameters panel as text annotation
    parameters_text = f"""<b>Processing Parameters</b><br>
Location: {bucket_name.replace('uhi-', '').replace('-', ' ').title()}<br>
ID: {root_name}<br>
Start trim: {start_time_adjustment_minutes:.1f} min<br>
End trim: {end_time_adjustment_minutes:.1f} min<br>
Cutoff: {cutoff_speed_MPH:.1f} MPH<br>
Drift correction: {temperature_drift_f * 3600:.3f} °F/hr<br>
Color min: {color_table_min_quantile}%<br>
Color max: {color_table_max_quantile}%"""

    annotation_dict = dict(
        text=parameters_text,
        xref="paper", yref="paper",
        x=0.98, y=0.98,
        xanchor="right", yanchor="top",
        showarrow=False,
        font=dict(size=11, color="black"),
        bgcolor="rgba(255, 255, 255, 0.9)",
        bordercolor="rgba(0, 0, 0, 0.3)",
        borderwidth=2
    )
    fig1.add_annotation(**annotation_dict)

    fig1.update_layout(
        updatemenus=[
            dict(
                type="buttons",
                showactive=True,
                x=0.98,
                y=0.76,
                xanchor="right",
                yanchor="top",
                buttons=[
                    dict(
                        label="Toggle Params",
                        method="relayout",
                        args=[{"annotations": []}],
                        args2=[{"annotations": [annotation_dict]}]
                    ),
                ]
            )
        ]
    )
    # ────────────────────────────────────────────────────────────────────────────────

    fig1.update_traces(mode='lines+markers', marker=dict(size=4, line=dict(width=1, color='DarkSlateGrey')))
    html_buf1 = io.StringIO()
    pio.write_html(fig1, file=html_buf1, auto_open=False)

    fig1_filename = f"{root_name}_{'fig1_corrected_temperature_map_time_window'}.html"
    logger.info(f"Saving HTML figure to S3: {fig1_filename}")
    save_to_s3(bucket_name, fig1_filename, html_buf1.getvalue())
    html_buf1.close()

    #####################################################################
    # Figure 2 — Temperature over time

    logger.info("Creating figure 2...")

    fig2 = px.line(df_step6, x='Timestamp', y='Temperature (°C)', color='SourceFile')
    fig2.update_traces(mode='lines+markers', marker=dict(size=4, line=dict(width=1, color='DarkSlateGrey')))

    # ──────────── MODIFIED: move legend to lower‑left & control font size ────────────
    fig2.update_layout(
        title='Figure 2. Uncorrected Temperature over Full Time',
        xaxis_title='Timestamp',
        yaxis_title='Temperature (°C)',
        legend_title_text='Sensor File',
        margin=dict(l=20, r=20, t=20, b=20),
        hovermode='x unified',
        hoverlabel=dict(bgcolor='white', font_size=12, bordercolor='black'),
        legend=dict(
            x=0,            # far left
            y=0,            # bottom
            xanchor='left',
            yanchor='bottom',
            font=dict(size=12)
        )
    )
    # ────────────────────────────────────────────────────────────────────────────────

                        # html_buf2 = io.StringIO()
                        # pio.write_html(fig2, file=html_buf2, auto_open=False)
                        # save_to_s3(bucket_name, f"{root_name}_fig_2_corrected_temperature_map_entire_time.html", html_buf2.getvalue())
                        # html_buf2.close()

    ##########################################################################
# Save CSVs

    #logger.info("Saving processed data to S3...")

    combined_csv = df_step5.to_csv(index=False)
#SAVE df_step5
    save_to_s3(bucket_name, f"{root_name}_combined_data_with_corrections.csv", combined_csv)

                        #    reduced_df = df_step5[['Latitude', 'Longitude', 'Altitude (m)', 'corrected_temperature_f', 'Humidity (%)', 'SourceFile']]
                        #    reduced_csv = reduced_df.to_csv(index=False)
                        # SAVE df_step5 with fewer columns
                        #    save_to_s3(bucket_name, f"{root_name}_combined_data_reduced_columns_with_corrections.csv", reduced_csv)
    ##########################################################################    

    campaign_duration_seconds = (df_step5['Timestamp'].max() - df_step5['Timestamp'].min()).total_seconds()
    logger.info(f"Campaign duration in seconds: {campaign_duration_seconds} using method 1")
    campaign_duration_seconds = df_step5['time_delta'].max()
    logger.info(f"Maximum time delta in seconds: {campaign_duration_seconds} using method 2")

    maximum_temperature_correction_f = temperature_drift_f * df_step5['time_delta'].max()
    logger.info(f"Maximum temperature correction in deg F: {maximum_temperature_correction_f:.3f}")

    logger.info(f"Minimum corrected temperature in deg F: {min_corrected_temperature_f:.3f}")
    logger.info(f"Maximum corrected temperature in deg F: {max_corrected_temperature_f:.3f}")   

    logger.info("Processing complete. Returning results...")

    return temperature_drift_f,campaign_duration_seconds,maximum_temperature_correction_f,max_corrected_temperature_f,min_corrected_temperature_f  # return the temperature drift in deg F/sec for the web page

#// ###########################################################################
# Optional CLI use

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('root_name', help='Required root name')
    parser.add_argument('bucket_name',  help='Required S3 bucket name')
    parser.add_argument('--start_time_adjustment_minutes', type=float, default=1.0)
    parser.add_argument('--end_time_adjustment_minutes', type=float, default=1.0)
    parser.add_argument('--cutoff_speed_MPH', type=float, default=1.0)
    parser.add_argument('--slope_option',type=int,default=1)
    parser.add_argument('--temperature_drift_f', type=float, default=0.0)
    parser.add_argument('--color_table_min_quantile', type=int, default=5)
    parser.add_argument('--color_table_max_quantile', type=int, default=95)
    parser.add_argument('--solid_color', action='store_true', help='Use solid color by route')
    parser.add_argument('--no-solid_color', action='store_false', dest='solid_color', help='Do not use solid color by route')
    parser.set_defaults(solid_color=False)

    args = parser.parse_args()

# This is the main data processing function
    mainProcessData(
        root_name=args.root_name,
        bucket_name=args.bucket_name,
        start_time_adjustment_minutes=args.start_time_adjustment_minutes,
        end_time_adjustment_minutes=args.end_time_adjustment_minutes,
        cutoff_speed_MPH=args.cutoff_speed_MPH,
        slope_option=args.slope_option,
        temperature_drift_f=args.temperature_drift_f,
        color_table_min_quantile=args.color_table_min_quantile,
        color_table_max_quantile=args.color_table_max_quantile,
        solid_color=args.solid_color
    )