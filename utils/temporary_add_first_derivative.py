import pandas as pd
import folium
import numpy as np
from scipy import stats
from scipy.interpolate import griddata
import plotly.graph_objects as go
import argparse
import os
import matplotlib.pyplot as plt
import shutil

def create_3d_plot(results_df):
    """
    Generates and saves an interactive 3D plot showing slope magnitudes from a zero plane.

    This version uses the filtered results DataFrame to create the plot.
    Args:
        results_df (pd.DataFrame): DataFrame containing the slope data, including 'rounded_lon', 'rounded_lat', and 'slope'.

    Usage: python cooling_rate.py --input_csv LivermoreSep4_combined_data.csv
    
    """
    print("\nGenerating interactive 3D plot with Plotly from filtered data...")
    
    plot_data = results_df.dropna(subset=['slope'])

    if len(plot_data) < 3:
        print("\nWARNING: Cannot generate a 3D plot.")
        print("Reason: Need at least 3 unique data points, but found " + str(len(plot_data)) + ".")
        return

    x = plot_data['rounded_lon']
    y = plot_data['rounded_lat']
    z_original = plot_data['slope'] # Keep original slope for hover text
    z = plot_data['slope'].abs()  # Use magnitude for z-axis height
    lat_int = plot_data['j']
    lon_int = plot_data['k']

    # --- DIAGNOSTICS START ---
    print("\n--- 3D Plot Diagnostics ---")
    print(f"Number of data points: {len(x)}")
    print(f"X (Longitude) min: {x.min()}, max: {x.max()}")
    print(f"Y (Latitude) min: {y.min()}, max: {y.max()}")
    print(f"Z (Slope Magnitude) min: {z.min()}, max: {z.max()}")
    # --- DIAGNOSTICS END ---

    fig = go.Figure()

    # Add a transparent plane at z=0 to serve as a baseline
    grid_x, grid_y = np.mgrid[x.min():x.max():2j, y.min():y.max():2j]
    fig.add_trace(go.Surface(
        x=grid_x,
        y=grid_y,
        z=np.zeros_like(grid_x),
        colorscale=[[0, 'lightgrey'], [1, 'lightgrey']],
        showscale=False,
        opacity=0.3,
        name='z=0 Plane',
        hoverinfo='none'
    ))

    # Add solid lines from the z=0 plane to the data point's slope value
    for i in range(len(x)):
        fig.add_trace(go.Scatter3d(
            x=[x.iloc[i], x.iloc[i]],
            y=[y.iloc[i], y.iloc[i]],
            z=[0, z.iloc[i]],
            mode='lines',
            line=dict(color='blue', width=5),
            showlegend=False,
            hoverinfo='none'
        ))

    # Add red circles on the z=0 plane
    hover_text = [
        f"Slope: {z_orig:.3f}<br>lat_int: {lat_i}<br>lon_int: {lon_i}<br>Lon: {lon:.4f}<br>Lat: {lat:.4f}"
        for z_orig, lat_i, lon_i, lat, lon in zip(z_original, lat_int, lon_int, y, x)
    ]
    
    fig.add_trace(go.Scatter3d(
        x=x,
        y=y,
        z=np.zeros_like(z),
        mode='markers',
        marker=dict(
            color='red',
            size=5,
            line=dict(width=1, color='black')
        ),
        text=hover_text,
        hoverinfo='text',
        name='Locations'
    ))

    # Update layout for a 3D isometric view
    fig.update_layout(
        title='Isometric 3D Plot of Temperature Slope Magnitude',
        scene=dict(
            xaxis_title='Longitude',
            yaxis_title='Latitude',
            zaxis_title='Slope Magnitude (F/hr)',
            camera=dict(
                eye=dict(x=1.5, y=1.5, z=1.5)
            ),
            zaxis_range=[0, z.max() * 1.1]
        ),
        autosize=False,
        width=900,
        height=800,
        showlegend=True
    )
    
    map_filename = 'slope_3d_plot.html'
    fig.write_html(map_filename)
    print(f"Interactive 3D plot saved as '{map_filename}'")
    fig.show()

def clear_directory(path):
    """Deletes all files in a specified directory, but not the directory itself."""
    if os.path.exists(path):
        for filename in os.listdir(path):
            file_path = os.path.join(path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f'Failed to delete {file_path}. Reason: {e}')

def main(input_filename):
    """
    Reads a CSV file with geographic coordinates, time, and temperature,
    groups them by location, performs linear regression, and plots the results.
    """
    try:
        # 1. Read the specified CSV file into a pandas DataFrame
        df = pd.read_csv(input_filename)
        print(f"\nSuccessfully loaded '{input_filename}'.")
        
        # --- Create and clear subdirectory for plots ---
        output_plot_dir = 'regression_plots'
        if os.path.exists(output_plot_dir):
            print(f"Clearing contents of '{output_plot_dir}'...")
            clear_directory(output_plot_dir)
        
        os.makedirs(output_plot_dir, exist_ok=True)
        print(f"Plots will be saved in the '{output_plot_dir}' directory.")

        # Convert 'Local Time' to datetime and calculate elapsed minutes
        print("Converting 'Local Time' and calculating elapsed time...")
        df['Local Time'] = pd.to_datetime(df['Local Time'])
        start_time = df['Local Time'].min()
        df['time_delta_minutes'] = (df['Local Time'] - start_time).dt.total_seconds() / 60
        
        print("Original DataFrame with calculated time delta (first 50 rows):")
        print(df.head(50))
        print("-" * 50)

        # 2. Round Latitude and Longitude to four decimal points
        lat_rounded = df['Latitude'].round(4)
        lon_rounded = df['Longitude'].round(4)

        # 3. Compute the minimum values of the rounded coordinates
        min_lat_rounded = lat_rounded.min()
        min_lon_rounded = lon_rounded.min()

        # 4. Calculate the 'lat_int' and 'lon_int' columns
        df['lat_int'] = ((lat_rounded - min_lat_rounded) * 1.e4).astype(int)
        df['lon_int'] = ((lon_rounded - min_lon_rounded) * 1.e4).astype(int)
        
        # --- Section for counting and linear regression ---
        print("Beginning count and regression for coordinate pairs...")

        def calculate_regression_metrics(group):
            if len(group) >= 3:
                result = stats.linregress(group['time_delta_minutes'], group['Temperature (°C)'])
                r_squared = result.rvalue**2
                slope_per_hour = result.slope * 60
                time_span = group['time_delta_minutes'].max() - group['time_delta_minutes'].min()
                return pd.Series({'slope': slope_per_hour, 'r_squared': r_squared, 'time_span_minutes': time_span})
            return pd.Series({'slope': np.nan, 'r_squared': np.nan, 'time_span_minutes': np.nan})

        grouped = df.groupby(['lat_int', 'lon_int'])
        
        counts = grouped.size().rename('n')
        regression_results = grouped.apply(calculate_regression_metrics)
        results_df = pd.concat([counts, regression_results], axis=1).reset_index()
        results_df = results_df.rename(columns={'lat_int': 'j', 'lon_int': 'k'})

        # 5. Add rounded lat/lon back to the results DataFrame
        results_df['rounded_lat'] = (results_df['j'] / 1.e4) + min_lat_rounded
        results_df['rounded_lon'] = (results_df['k'] / 1.e4) + min_lon_rounded

        # Added new filtering criteria for the slope.
        filtered_results_df = results_df[
            (results_df['n'] >= 2) & 
            (results_df['r_squared'] > 0.98) & 
            (results_df['time_span_minutes'] > 4.0) &
            (results_df['slope'] >= -5)
        ].copy()
        
        # --- MODIFIED PART: Save the filtered DataFrame to CSV immediately after calculation ---
        # 6. Save the filtered results DataFrame to a new CSV file
        counts_output_filename = 'filtered_cooling_rates.csv'
        filtered_results_df.to_csv(counts_output_filename, index=False)
        print(f"\nFiltered results data frame saved to '{counts_output_filename}'.")
        # --- END MODIFIED PART ---
        
                            # --- Generate and save a plot for each filtered group ---
                            # if not filtered_results_df.empty:
                            #     print(f"\nGenerating {len(filtered_results_df)} regression plots for filtered groups...")
                            #     for index, row in filtered_results_df.iterrows():
                            #         lat_int = int(row['j'])
                            #         lon_int = int(row['k'])
                            #         
                            #         # Get the original data points for this specific group
                            #         group_df = df[(df['lat_int'] == lat_int) & (df['lon_int'] == lon_int)]
                            #         
                            #         x_vals = group_df['time_delta_minutes']
                            #         y_vals = group_df['Temperature (°C)']
                            #         
                            #         # Perform linear regression again to get the intercept for plotting
                            #         result = stats.linregress(x_vals, y_vals)
                            #         
                            #         # Create the plot
                            #         plt.figure(figsize=(10, 6))
                            #         plt.scatter(x_vals, y_vals, label='Temperature Readings', color='blue')
                            #         
                            #         # Create the regression line data
                            #         line_x = np.array([x_vals.min(), x_vals.max()])
                            #         line_y = result.slope * line_x + result.intercept
                            #         
                            #         # Plot the regression line
                            #         plt.plot(line_x, line_y, color='red', 
                            #                         label=f'Linear Fit (Slope: {result.slope * 60:.2f} C/hr)')
                            #         
                            #         # Add plot details
                            #         plt.title(f'Temperature vs. Time\nLocation ID (lat_int: {lat_int}, lon_int: {lon_int})')
                            #         plt.xlabel('Time Since Start (minutes)')
                            #         plt.ylabel('Temperature (°C)')
                            #         plt.legend()
                            #         plt.grid(True)
                            #         
                            #         # Save the plot to a file
                            #         plot_filename = os.path.join(output_plot_dir, f'regression_lat_{lat_int}_lon_{lon_int}.png')
                            #         plt.savefig(plot_filename)
                            #         plt.close() # Close the figure to free up memory
                            #     print(f"Finished saving plots.")
                            # else:
                            #     print("\nNo groups met the filtering criteria. No regression plots were generated.")

        # Round the specified columns to 3 decimal places for display purposes
        columns_to_round = ['slope', 'r_squared', 'time_span_minutes']
        for col in columns_to_round:
            if col in filtered_results_df.columns:
                filtered_results_df[col] = filtered_results_df[col].round(3)

        print(f"\nAnalysis finished. Displaying final results:")
        print("Filtered Coordinate Counts, Slopes, R-squared, and Time Span (rounded for display):")
        print(filtered_results_df)

        # 7. Generate the 3D surface plot from the FULLY FILTERED results
        create_3d_plot(filtered_results_df)









        # Scale open-circle diameter by |cooling rate|
        # Folium CircleMarker radius is in *pixels* (radius = diameter / 2)

        MIN_DIAMETER_PX = 2     # tweak as desired
        MAX_DIAMETER_PX = 80    # tweak as desired
        min_abs = filtered_results_df['slope'].abs().min()
        max_abs = filtered_results_df['slope'].abs().max()

        def diameter_from_abs_slope(abs_s):
            if max_abs == min_abs:
                return (MIN_DIAMETER_PX + MAX_DIAMETER_PX) / 2.0
            return MIN_DIAMETER_PX + (abs_s - min_abs) * (MAX_DIAMETER_PX - MIN_DIAMETER_PX) / (max_abs - min_abs)

        map_center = [filtered_results_df['rounded_lat'].mean(), filtered_results_df['rounded_lon'].mean()]
        m = folium.Map(location=map_center, zoom_start=16, tiles='CartoDB positron')

        # Check if there's any data to plot
        if filtered_results_df.empty:
            print("\nWARNING: No data met the filtering criteria. The map will be empty.")
        else:
            for _, row in filtered_results_df.iterrows():
                abs_s = abs(row['slope'])
                diam = diameter_from_abs_slope(abs_s)
                radius_px = diam / 2.0

                # --- MODIFIED & MORE ROBUST POPUP/TOOLTIP ---
                
                # Create the HTML content for the popup
                popup_html = f"""
                <b>Location Details</b><br>
                --------------------<br>
                Cooling Rate: <b>{row['slope']:.4f} °C/hr</b><br>
                R² Value: {row['r_squared']:.3f}<br>
                Time Span (min): {row['time_span_minutes']:.1f}<br>
                Coords: ({row['rounded_lat']:.4f}, {row['rounded_lon']:.4f})<br>
                <a href="https://www.google.com/maps?q=&layer=c&cbll={row['rounded_lat']},{row['rounded_lon']}" target="_blank">Open in Street View</a>
                """
                
                # Create a Folium Popup object
                popup = folium.Popup(popup_html, max_width=300)
                
                # Create a simple tooltip for hover
                tooltip_text = f"Click for details. Rate: {row['slope']:.2f} °C/hr"

                folium.CircleMarker(
                    location=[row['rounded_lat'], row['rounded_lon']],
                    radius=radius_px,
                    color='black',
                    weight=2,
                    fill=False,
                    opacity=1.0,
                    popup=popup, # Use the Popup object
                    tooltip=tooltip_text # Add a hover tooltip
                ).add_to(m)

        output_filename = 'cooling_rates_open_circles_map.html'
        m.save(output_filename)

        print("\nSuccessfully created the map!")
        print(f"Open '{output_filename}' in your browser to view it.")






    except FileNotFoundError:
        print(f"\nError: '{input_filename}' not found. Please ensure the file exists and is spelled correctly.")
    except KeyError as e:
        print(f"\nError: A required column was not found in the CSV file: {e}")
        print("Please ensure your CSV has columns named 'Latitude', 'Longitude', 'Local Time', and 'Temperature (°C)'.")
    except ImportError:
        print("\nError: Plotly, Scipy, or Matplotlib is not installed. Please install them to generate the map: pip install plotly scipy matplotlib")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")






if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process location data from a CSV file.")
    parser.add_argument(
        '--input_csv', 
        type=str, 
        required=True,
        help="Path to the input CSV file. Must contain 'Latitude', 'Longitude', 'Local Time', and 'Temperature (°C)' columns."
    )
    args = parser.parse_args()
    main(args.input_csv)
