// Store the original console.error function
const originalConsoleError = console.error;

/**
 * Logs an error to the S3 log file via the backend and also displays it
 * in the browser's developer console.
 * @param {...any} args - The arguments to log, same as console.error.
 */
function logAndDisplayError(...args) {
  // --- 1. Log to S3 via the backend ---
  try {
    // **COMPATIBILITY FIX 1: Replaced optional chaining (?.)**
    const processIdElement = document.getElementById('process_id');
    const campaignId = (processIdElement && processIdElement.value) || 'UNKNOWN_CAMPAIGN';

    // Format the error message from all arguments passed to the function
    const errorMessage = args.map(function(arg) {
      if (typeof arg === 'object' && arg !== null) {
        // Stringify objects to capture their content
        return JSON.stringify(arg, null, 2);
      }
      return String(arg);
    }).join(' ');

    const logPayload = {
      message: 'Frontend Console Error: ' + errorMessage
    };
    
    // **COMPATIBILITY FIX 2: Replaced object spread (...) with Object.assign()**
    const finalPayload = Object.assign({}, logPayload, { campaign_id: campaignId });


    // Send the log "fire-and-forget" style. We don't need to wait (await) for
    // this to complete, as we don't want to slow down the frontend.
    fetch('/log_activity', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(finalPayload)
    }).catch(function(networkError) {
      // If the logging itself fails, log that to the console *without* re-triggering our custom logger.
      originalConsoleError('Failed to send error log to backend:', networkError);
    });

  } catch (e) {
    originalConsoleError('Error within custom logger:', e);
  }

  // --- 2. Call the original console.error ---
  // Use .apply() to pass all original arguments to the native console.error
  originalConsoleError.apply(console, args);
}

// Overwrite the global console.error with our new function
window.console.error = logAndDisplayError;