function downloadCSV(content, fileName) {
    // Create a Blob containing the CSV data
    const blob = new Blob([content], { type: 'text/csv' });
  
    // Create a URL for the Blob
    const url = URL.createObjectURL(blob);
  
    // Create a link element to trigger the download
    const a = document.createElement('a');
    a.href = url;
    a.download = fileName || 'download.csv';
  
    // Trigger the download by clicking the link
    a.click();
  
    // Clean up by revoking the URL
    URL.revokeObjectURL(url);
  }

function capitalizeFLetter(word) {
    return word[0].toUpperCase() + word.slice(1);
}

// Helper function to escape double-quotes within a string
function escapeDoubleQuotes(str) {
    return str.replace(/"/g, '""');
}

function arrayToCSVLine(array) {

    // Create an array of CSV-safe values
    const csvValues = array.map((value) => {
      if (typeof value === 'string' && (value.includes(',') || value.includes("\n"))) {
        // Wrap the value in double-quotes and escape any double-quotes within it
        return `"${escapeDoubleQuotes(value)}"`;
      } else {
        return value;
      }
    });
  
    // Join the CSV-safe values with commas
    return csvValues.join(',');
}

