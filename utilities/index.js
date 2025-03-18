const XLSX = require("xlsx");

function getHeaders() {
  return {
    "Content-Type": "application/json",
  };
}

function getHeadersForExcel() {
  return {
    "Content-Type": "vnd.ms-excel",
  };
}

function getTableDataFromExcel(file_path, tableName) {
  try {
    console.log(`Reading table: ${tableName} from Excel`);
    const workbook = XLSX.readFile(file_path);
    const worksheet = workbook.Sheets[tableName];

    if (!worksheet) {
      console.log(`Table ${tableName} not found in Excel`);
      return [];
    }

    return XLSX.utils.sheet_to_json(worksheet);
  } catch (error) {
    console.log(`Error reading table ${tableName} from Excel:`, error);
    return [];
  }
}

function writeToExcel(file_path, records, workbook, table_name) {
  if (records.length === 0) return;

  let worksheet;

  if (workbook.Sheets[table_name]) {
    /** If sheet exists, append new data without losing the old data */
    worksheet = workbook.Sheets[table_name];
    XLSX.utils.sheet_add_json(worksheet, records, {
      skipHeader: true, // Avoid rewriting column headers
      origin: -1, // Append new rows at the end
    });
  } else {
    // Create new worksheet
    worksheet = XLSX.utils.json_to_sheet(records);
    XLSX.utils.book_append_sheet(workbook, worksheet, table_name);
  }

  XLSX.writeFile(workbook, file_path);
  console.log(` Data appended to ${table_name}`);
}

module.exports = {
  getHeaders,
  getHeadersForExcel,
  getTableDataFromExcel,
  writeToExcel,
};
