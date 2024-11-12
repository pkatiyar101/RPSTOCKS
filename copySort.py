import pandas as pd
import xlwings as xw

# Open the workbook and the sheet
wb = xw.Book('YFin.xls')  # Ensure the file path is correct
sheet = wb.sheets['COPY']  # Replace with the correct sheet name

# Get the data from the AK column (adjusting for correct range)
data = sheet.range('AK1:AK10000').value  # Adjust range as needed

# Convert the data to a pandas DataFrame for easier manipulation
df = pd.DataFrame(data, columns=['Values'])

# Clean the data by removing blanks and hyphens ('-')
df = df[df['Values'].notna()]  # Remove blank values
df = df[df['Values'] != '-']   # Remove hyphen values

# Optionally, filter for values starting with 'NSE:' (if needed for valid format)
df = df[df['Values'].str.startswith('NSE:')]  # Filter for 'NSE:XXXXX' format

# Find the top 10 most repeated values (mode)
top_10_values = df['Values'].value_counts().head(10).index.tolist()

# Copy the top 10 most repeated values to column AT starting from AT3
sheet.range('AQ2:AQ25').clear_contents()
sheet.range('AQ2').value = [[value] for value in top_10_values]

# Save the workbook if needed
wb.save()

# Close the workbook if needed
# wb.close()

