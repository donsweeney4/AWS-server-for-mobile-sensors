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
    df = df_step[['Timestamp', 'Temperature (°C)']].dropna().copy()      # LocalTime is in datetime object

    # Convert datetime to numeric timestamp in seconds
    df['time_seconds'] = df['Timestamp'].astype('datetime64[s]').astype(int)   # Convert to seconds since epoch


    X = df['time_seconds'].values.reshape(-1, 1)  # X is an NumPy array of shape (n_samples, 1) representing time in seconds
    y = df['Temperature (°C)'].values

    # Fit linear regression model
    model = SklearnLinearRegression()
    model.fit(X, y)

    slope = model.coef_[0]  # in degrees F per second
    intercept = model.intercept_

    logger.info(f"Linear regression slope: {slope:.6f} °C/sec")
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
                    start_time_adjustment_minutes=0.0,
                    end_time_adjustment_minutes=0.0,
                    cuttoff_speed_MPH=1.0,
                    temperature_drift=0.0,
                    color_table_min_quantile=5,
                    color_table_max_quantile=95,
                    solid_color_by_route=False):

    logger.info(f"Starting mainProcessData with root_name={root_name}")

    bucket_name = 'urban-heat-island-data'
    csv_keys = get_s3_objects(bucket_name, root_name)

    cuttoff_speed = cuttoff_speed_MPH

    

    start_time_adjustment_ms = start_time_adjustment_minutes * 60 * 1000 # units will be in milliseconds
    end_time_adjustment_ms = end_time_adjustment_minutes * 60 * 1000    # units will be in milliseconds

    logger.info(f"Timestamp start adjustment ms: {start_time_adjustment_ms}, Timestamp end adjustment ms: {end_time_adjustment_ms}")

    logger.info(f"Temperature Drift in degF/sec: {temperature_drift}")
    logger.info(f"Temperature Drift in degF/hr: {temperature_drift*3600.0}")
    logger.info(f"Cuttoff speed in MPH: {cuttoff_speed}")
    logger.info(f"Cuttoff speed in m/s: {cuttoff_speed * 0.44704}")
    logger.info(f"Color table min quantile: {color_table_min_quantile}")
    logger.info(f"Color table max quantile: {color_table_max_quantile}")


 
    solid_color_list = ['red', 'blue', 'green', 'purple', 'orange', 'darkred', 'lightred',
                        'beige', 'darkblue', 'darkgreen']

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

##############################     
        try:
    # Convert to datetime, forcing errors to NaT (not a time)for bad values
            df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='ms', errors='coerce')

    # Log if any were invalid
            if df['Timestamp'].isna().any():
                logger.warning("⚠️ Some timestamps were invalid and converted to NaT.")

    # Optional: drop rows with invalid timestamps
            df = df.dropna(subset=['Timestamp'])

        except Exception as e:
            logger.error(f"❌ Unexpected error while parsing timestamps: {e}")   
 ##############################   

        logger.info(f"df shape: {df.shape} (rows, columns)")


        df_min = df['Timestamp'].min()
        df_max = df['Timestamp'].max()
        timestamp_start = df_min + pd.Timedelta(milliseconds=start_time_adjustment_ms)
        timestamp_end = df_max - pd.Timedelta(milliseconds=end_time_adjustment_ms)
# Filter the DataFrame based on the adjusted timestamps
        df_step2 = df[(df['Timestamp'] > timestamp_start) & (df['Timestamp'] < timestamp_end)].copy()
        if df_step2.empty:
            logger.warning(f"⚠️ No data in {key} after time filtering. Skipping this file.")
            continue
# Filter the DataFrame based on the adjusted speed
        logger.info(f"df_step2 shape: {df_step2.shape} (rows, columns)")
        df_step3 = df_step2[df_step2['Speed (MPH)'] >= cuttoff_speed].copy()
        if df_step3.empty:
            logger.warning(f"⚠️ No data in {key} after speed filtering. Skipping this file.")
            continue

        logger.info(f"df_step3 shape: {df_step3.shape} (rows, columns)")

        df_step4 = df_step3.copy()  # df_step4 is a deep copy of df_step3

        df_step5 = pd.concat([df_step5, df_step4], ignore_index=True)
        df_step6 = pd.concat([df_step6, df], ignore_index=True)

#######################################################################      
### END OF LOOP PROCESSING EACH CSV FILE
###  df_step5 and df_step6 contain all the concatenated data from all the CSV files in the campaign 
###  df_step5 contains the filtered data based on speed and time
###  df_step6 contains the unfiltered data from all the CSV files in the campaign
#######################################################################
    logger.info(f"Total rows of all files in the campaign: {df_step5.shape[0]}")

    if df_step5.empty:
        raise ValueError("No data available after filtering. Check input filters or data.")


## Temperature Drift Correction for df_step5; that is all the data that has been filtered by speed and time

# The temperture drift is a slope.  The temperature correction is based on the time delta from the first timestamp in the dataset and is in degrees Celsius per second.
# A negative temperature drift value indicates that the ambient temperature is decreasing over time and needs to be corrected.
# A positive temperature drift value indicates that the ambient temperature is increasing over time.

    df_step5['time_delta'] = (df_step5['Timestamp'] - df_step5['Timestamp'].min()).dt.total_seconds()  # adds a new column with the time delta in seconds from the first timestamp in the dataset





# Calculate the temperature drift based on the time delta and temperature drift
    temperature_drift_f = temperature_drift # confusing --> but this is the user entered value in deg F/sec
                                                # and the value computed by linear regression is in deg C/sec
    if temperature_drift == 0.0:
        # If no temperature drift is provided, calculate it using linear regression
        logger.info(f"   Using linear regression to calculate temperature drift for {len(df_step5)} data points.")
        # Ensure the DataFrame has enough data points for linear regression
        temperature_drift = LinearRegression(df_step5)  # in deg C/sec

        logger.info(f"Temperature drift calculated: {temperature_drift:.6f} °C/sec")
        temperature_drift_f = temperature_drift * 9. / 5.
        logger.info(f"Temperature drift calculated: {temperature_drift_f:.6f} °F/sec")            
        if has_request_context():   # if this is a web request, store the temperature drift in the session but only if 
                                    # we are in a web request context 
            session.temperature_drift = round(temperature_drift_f,6)   # store the temperature drift in deg F/sec


# units are deg C for the next three line
    df_step5['temperature_correction'] = df_step5['time_delta'] * temperature_drift
    df_step5['corrected_temperature'] = (df_step5['Temperature (°C)'] - df_step5['temperature_correction']).round(2)


### Add temperature in Fahrenheit
    df_step5['corrected_temperature (°F)'] = df_step5['corrected_temperature'] * 9/5 + 32
    df_step5['Temperature (°F)'] = df_step5['Temperature (°C)'] * 9/5 + 32

    df_step5['corrected_temperature (°F)'] = df_step5['corrected_temperature (°F)'].round(2)

    df_step5['Temperature (°F)'] = df_step5['Temperature (°F)'].round(2)
##########################

   






    color_table_min = np.percentile(df_step5['corrected_temperature (°F)'], color_table_min_quantile)
    color_table_max = np.percentile(df_step5['corrected_temperature (°F)'], color_table_max_quantile)


    # Convert to local time if needed (example: Pacific Time)
    # Assume the original timestamps are in UTC
    df_step5['Timestamp'] = pd.to_datetime(df_step5['Timestamp'])  # ensure datetime
    df_step5['LocalTime'] = df_step5['Timestamp'].dt.tz_localize('UTC').dt.tz_convert('US/Pacific')



      #  .dt.tz_localize('UTC')           # tell pandas: these timestamps are UTC
      #  .dt.tz_convert('US/Pacific')     # convert to local time
     


    dtemp = (color_table_max - color_table_min) / 4
    index = [color_table_min + i * dtemp for i in range(5)]

    colormap = cm.LinearColormap(colors=['blue', 'cyan', 'green', 'yellow', 'red'], index=index,
                                  vmin=color_table_min, vmax=color_table_max)
    
#################  Folium control ########################################################
    logger.info("Creating Folium map with sensor data...")

    # Create a Folium map centered around the average latitude and longitude of the data
    center_lat = (df_step5['Latitude'].min() + df_step5['Latitude'].max()) / 2
    center_lon = (df_step5['Longitude'].min() + df_step5['Longitude'].max()) / 2

    m = folium.Map(location=[center_lat, center_lon], zoom_start=12, control_scale=True)


# Add Google satellite layer
    folium.TileLayer(
    tiles='https://{s}.google.com/vt/lyrs=s&x={x}&y={y}&z={z}',
    attr='Google Satellite',
    name='Satellite View',
    subdomains=['mt0', 'mt1', 'mt2', 'mt3'],
    overlay=False,
    control=True
).add_to(m)

# Add default OpenStreetMap layer for comparison
    folium.TileLayer('OpenStreetMap', name='Standard Map').add_to(m)



####################  End Folium control ####################################################

    logger.info("Adding sensor data to Folium map...")

    for key in csv_keys:
        group = folium.FeatureGroup(name=f"Sensor Data: {key}", show=True, overlay=True)

        df_key = df_step5[df_step5['SourceFile'] == key]

        for _, row in df_key.iterrows():
            fill_color = solid_color_list[i % len(solid_color_list)] if solid_color_by_route else str(colormap(row['corrected_temperature (°F)']))
            tooltip = (
                f"File: {key}<br>"
                f"Local Time: {row['LocalTime']}<br>"
                f"Corrected Temp: {row['corrected_temperature (°F)']:.2f} °F<br>"
                f"Uncorrected Temp: {row['Temperature (°F)']:.2f} °F<br>"
                f"Humidity: {row['Humidity (%)']} %<br>"
                f"Speed: {row['Speed (MPH)']} MPH<br>"
                f"Lat: {row['Latitude']}<br>"
                f"Lon: {row['Longitude']}"
            )

            folium.CircleMarker(
                location=[row['Latitude'], row['Longitude']],
                radius=6,
                color=fill_color,
                fill=True,
                fill_color=fill_color,
                fill_opacity=0.5,
                tooltip=tooltip
            ).add_to(group)

        group.add_to(m)
        i += 1
    logger.info("Adding color map to Folium map...")

    colormap.caption = 'Corrected Temperature (°F)'
    colormap.add_to(m)
    folium.LayerControl(collapsed=False).add_to(m)

    html_map = m.get_root().render()

    
    html_filename = f"{root_name}_{'color_coded_route_map' if solid_color_by_route else 'color_coded_temperature_map'}.html"
    save_to_s3(bucket_name, html_filename, html_map)

######################################################################
# Figure 1 — temperature with time filtering and corrected temperature in deg F

    logger.info("Creating figure 1...")

    fig1 = px.line(df_step5, x='LocalTime', y='corrected_temperature (°F)', color='SourceFile')

    fig1.update_layout(
        title=dict(
            text='Figure 1. Drift-Corrected Temperature over Time Window',
            font=dict(size=24),
            x=0.5,
            xanchor='center'
        ),
        font=dict(size=16),
        xaxis=dict(
            title='Local Time',
            tickformat='%H:%M:%S'  # Show only time in 24-hour format
        ),
        yaxis=dict(
            title='Drift-Corrected Temperature (°F)',
            tickformat='.2f'
        ),
        legend_title_text='Sensor Files',
        margin=dict(l=20, r=20, t=80, b=20),
        hovermode='x unified',
        hoverlabel=dict(bgcolor='white', font_size=12, bordercolor='black')
    )
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

    fig2.update_layout(
        title='Figure 2. Uncorrected Temperature over Full Time',
        xaxis_title='Timestamp',
        yaxis_title='Temperature (°C)',
        legend_title_text='Sensor File',
        margin=dict(l=20, r=20, t=20, b=20),
        hovermode='x unified',
        hoverlabel=dict(bgcolor='white', font_size=12, bordercolor='black')
    )
    html_buf2 = io.StringIO()
    pio.write_html(fig2, file=html_buf2, auto_open=False)
    save_to_s3(bucket_name, f"{root_name}_fig_2_corrected_temperature_map_entire_time.html", html_buf2.getvalue())
    html_buf2.close()

########################################################################
# Save CSVs

    logger.info("Saving processed data to S3...")


    combined_csv = df_step5.to_csv(index=False)
    save_to_s3(bucket_name, f"{root_name}_combined_data", combined_csv)

    reduced_df = df_step5[['Latitude', 'Longitude', 'Altitude (m)', 'corrected_temperature', 'Humidity (%)', 'SourceFile']]
    reduced_csv = reduced_df.to_csv(index=False)
    save_to_s3(bucket_name, f"{root_name}_combined_data_reduced_columns.csv", reduced_csv)

    return temperature_drift_f   # return the temperature drift in deg F/sec for the web page

#// ###########################################################################
 ##########################################################################
 ##########################################################################
 
# Optional CLI use

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('root_name', help='Required root name')
    parser.add_argument('--start_time_adjustment_minutes', type=float, default=1.0)
    parser.add_argument('--end_time_adjustment_minutes', type=float, default=1.0)
    parser.add_argument('--cuttoff_speed_MPH', type=float, default=1.0)
    parser.add_argument('--temperature_drift', type=float, default=0.0)
    parser.add_argument('--color_table_min_quantile', type=int, default=5)
    parser.add_argument('--color_table_max_quantile', type=int, default=95)
    parser.add_argument('--solid_color_by_route', type=bool, default=False)
    args = parser.parse_args()

    mainProcessData(
        root_name=args.root_name,
        start_time_adjustment_minutes=args.start_time_adjustment_minutes,
        end_time_adjustment_minutes=args.end_time_adjustment_minutes,
        cuttoff_speed_MPH=args.cuttoff_speed_MPH,
        temperature_drift=args.temperature_drift,
        color_table_min_quantile=args.color_table_min_quantile,
        color_table_max_quantile=args.color_table_max_quantile,
        solid_color_by_route=args.solid_color_by_route
    )
