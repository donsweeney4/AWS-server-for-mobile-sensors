import os
from datetime import datetime

# --- Configuration ---
LOG_FILE_PATH = "/home/ubuntu/HeatIslandResultsServer/crontab.log"  # <--- IMPORTANT: Change this to your actual log file path
LINES_TO_KEEP = 5000

# --- Logging Function (for script's own output) ---
def script_log(message):
    """Logs messages to console with a timestamp."""
    timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    print(f"{timestamp} {message}")

# --- Main Log File Management Function ---
def manage_log_file(file_path: str, lines_to_keep: int):
    """
    Manages a log file by trimming it to keep the newest specified number of lines.

    Args:
        file_path (str): The full path to the log file.
        lines_to_keep (int): The maximum number of lines to retain in the log file.
    """
    script_log(f"Starting log file management for: {file_path}")

    if not os.path.exists(file_path):
        script_log(f"Warning: Log file not found at '{file_path}'. Skipping management.")
        return

    try:
        # 1. Read all lines from the file
        with open(file_path, 'r') as f:
            lines = f.readlines()

        current_line_count = len(lines)
        script_log(f"Current line count: {current_line_count}")

        # 2. Check if trimming is needed
        if current_line_count > lines_to_keep:
            script_log(f"File exceeds {lines_to_keep} lines. Trimming oldest lines...")

            # Calculate the starting index to keep the newest `lines_to_keep`
            # If current_line_count = 5005 and lines_to_keep = 5000,
            # then lines_to_keep_start_index = 5.
            # We want to keep lines from index 5 onwards.
            lines_to_keep_start_index = current_line_count - lines_to_keep
            retained_lines = lines[lines_to_keep_start_index:] # This was the main logical error

            # 3. Overwrite the file with the retained lines
            with open(file_path, 'w') as f:
                f.writelines(retained_lines)

            new_line_count = len(retained_lines)
            script_log(f"Successfully trimmed. New line count: {new_line_count}")
        else:
            script_log("No trimming needed. Line count is within limits.")

    except FileNotFoundError:
        script_log(f"Error: The file '{file_path}' was not found. This should ideally be caught by os.path.exists.")
    except IOError as e:
        script_log(f"Error: An I/O error occurred while processing '{file_path}': {e}")
    except Exception as e:
        script_log(f"An unexpected error occurred: {e}")

# --- Run the script ---
if __name__ == "__main__":
    manage_log_file(LOG_FILE_PATH, LINES_TO_KEEP) # Pass LINES_TO_KEEP directly
