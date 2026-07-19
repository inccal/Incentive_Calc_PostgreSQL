import XLSX from 'xlsx';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const files = [
  'pioneer recruitors.xlsx',
  'pioneer recuitors.xlsx',
  'updated excel exacmple.xlsx'
];

for (const file of files) {
  const filePath = path.resolve(__dirname, '../../', file);
  console.log(`\n=================== FILE: ${file} ===================`);
  try {
    const workbook = XLSX.readFile(filePath);
    console.log(`Sheet Names: ${workbook.SheetNames.join(', ')}`);
    for (const sheetName of workbook.SheetNames) {
      const sheet = workbook.Sheets[sheetName];
      const data = XLSX.utils.sheet_to_json(sheet, { header: 1 });
      console.log(`\nSheet: ${sheetName}`);
      console.log(`Total Rows: ${data.length}`);
      
      // Let's find rows that look like headers or contain data
      let headerCount = 0;
      for (let i = 0; i < Math.min(data.length, 50); i++) {
        const row = data[i];
        if (!row || !row.length) continue;
        const rowStrings = row.map(c => String(c || "").trim());
        const rowLower = rowStrings.map(c => c.toLowerCase());
        
        const isHeader = rowLower.includes("team") || rowLower.includes("candidate name") || rowLower.includes("recruiter name");
        if (isHeader) {
          console.log(`Row ${i} (Header):`, rowStrings.filter(Boolean).slice(0, 15));
        } else {
          // If it's a non-empty row, print it if it looks interesting
          const nonB = rowStrings.filter(Boolean);
          if (nonB.length > 5 && headerCount < 5) {
            console.log(`Row ${i} (Data Sample):`, nonB.slice(0, 15));
            headerCount++;
          }
        }
      }
    }
  } catch (err) {
    console.error(`Error reading ${file}:`, err.message);
  }
}
