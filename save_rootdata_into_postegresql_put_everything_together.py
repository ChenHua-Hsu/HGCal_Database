import uproot
import pandas as pd
import psycopg2

# Open the ROOT file
root_file = uproot.open("/Users/chenhua/root_file/pedestal_run0_M84.root")

# Access the tree (replace 'runsummary/summary' with your actual path)
tree = root_file["runsummary/summary"]

# Convert the tree to a pandas DataFrame
df = tree.arrays(library="pd")

cell_mapping = {}
with open('/Users/chenhua/WaferCellMapTraces.txt', 'r') as file:
    next(file)
    for line in file:
        if line.strip():  # Skip empty lines
            parts = line.split()
            chip = int(parts[1])
            channel = str(parts[4])
            cell = int(parts[5])
            cell_mapping[(chip, channel)] = cell

# Initialize the cells list and a mutable container for 'a'
cells = []
a = -100  # Using a list to allow modification in inner function

# Process each row in the DataFrame
# def get_cell(row):
#     if row['channeltype'] == 100:
#         cell = a[0]
#         a[0] -= 1
#     elif row['channeltype'] == 1:
#         channel = 'CALIB' + str(row['channel'])
#         cell = cell_mapping.get((row['chip'], channel), None)
#     elif row['channeltype'] == 0:
#         cell = cell_mapping.get((row['chip'], str(row['channel'])), None)
#     else:
#         cell = None
#     return cell

# df['cell'] = df.apply(get_cell, axis=1)

# Connect to PostgreSQL database
conn = psycopg2.connect(
    dbname="hgcdb",
    user="teststand_user",
    password="33665146",
    host="localhost",
    port="5432"
)
cur = conn.cursor()
chips = df['chip'].tolist()
channels = df['channel'].tolist()
channeltypes = df['channeltype'].tolist()
adc_medians = df['adc_median'].tolist()
adc_iqrs = df['adc_iqr'].tolist()
tot_medians = df['tot_median'].tolist()
tot_iqrs = df['tot_iqr'].tolist()
toa_medians = df['toa_median'].tolist()
toa_iqrs = df['toa_iqr'].tolist()
adc_means = df['adc_mean'].tolist()
adc_stdds = df['adc_stdd'].tolist()
tot_means = df['tot_mean'].tolist()
tot_stdds = df['tot_stdd'].tolist()
toa_means = df['toa_mean'].tolist()
toa_stdds = df['toa_stdd'].tolist()
tot_efficiencies = df['tot_efficiency'].tolist()
tot_efficiency_errors = df['tot_efficiency_error'].tolist()
toa_efficiencies = df['toa_efficiency'].tolist()
toa_efficiency_errors = df['toa_efficiency_error'].tolist()
#cells = df['cell'].tolist()



for chip, channel, channeltype in zip(chips, channels, channeltypes):
    if channeltype == 100:
        cells.append(a)
        a -= 1
    elif channeltype == 1:
        channel_str = 'CALIB' + str(channel)
        cell = cell_mapping.get((chip, channel_str), None)
        cells.append(cell)
    elif channeltype == 0:
        cell = cell_mapping.get((chip, str(channel)), None)
        cells.append(cell)
print(cells)
# Prepare the SQL insert statement
insert_query = """
INSERT INTO module_pedestal_test (
    chip, channel, channeltype, adc_median, adc_iqr, tot_median, tot_iqr, 
    toa_median, toa_iqr, adc_mean, adc_stdd, tot_mean, tot_stdd, 
    toa_mean, toa_stdd, tot_efficiency, tot_efficiency_error, 
    toa_efficiency, toa_efficiency_error, cell
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
cur.execute(insert_query, (
    chips, channels, channeltypes, adc_medians, adc_iqrs, tot_medians, tot_iqrs, 
    toa_medians, toa_iqrs, adc_means, adc_stdds, tot_means, tot_stdds, 
    toa_means, toa_stdds, tot_efficiencies, tot_efficiency_errors, 
    toa_efficiencies, toa_efficiency_errors, cells
))
# # Insert data into database
# for index, row in df.iterrows():
#     cur.execute(insert_query, (
#         row['chip'], row['channel'], row['channeltype'], row['adc_median'], 
#         row['adc_iqr'], row['tot_median'], row['tot_iqr'], row['toa_median'], 
#         row['toa_iqr'], row['adc_mean'], row['adc_stdd'], row['tot_mean'], 
#         row['tot_stdd'], row['toa_mean'], row['toa_stdd'], row['tot_efficiency'], 
#         row['tot_efficiency_error'], row['toa_efficiency'], row['toa_efficiency_error'],
#         row['cell']
#     ))

# Update module_pedestal_test
cur.execute("""
UPDATE module_pedestal_test
SET 
    count_bad_cells = (
        SELECT COUNT(*)
        FROM unnest(adc_stdd) AS stdd
        WHERE stdd < 0.1 OR stdd > 2
    );
""")

cur.execute("""
UPDATE module_pedestal_test
SET
    list_dead_cells = subquery.list_dead_cells,
    list_noisy_cells = subquery.list_noisy_cells
FROM (
    SELECT 
        mod_pedtest_no,  -- Assuming there's a unique identifier column named 'mod_pedtest_no'
        array_agg(CASE WHEN stdd.value < 0.1 THEN c.val END) FILTER (WHERE stdd.value < 0.1) AS list_dead_cells,
        array_agg(CASE WHEN stdd.value > 2 THEN c.val END) FILTER (WHERE stdd.value > 2) AS list_noisy_cells
    FROM module_pedestal_test
    CROSS JOIN LATERAL unnest(adc_stdd) WITH ORDINALITY AS stdd(value, idx)
    CROSS JOIN LATERAL unnest(cell) WITH ORDINALITY AS c(val, idx2)
    WHERE idx = idx2
    GROUP BY mod_pedtest_no
) AS subquery
WHERE module_pedestal_test.mod_pedtest_no = subquery.mod_pedtest_no;
""")

# Insert into module_qc_summary
cur.execute("""
INSERT INTO module_qc_summary (count_bad_chan, list_bad_pad)
SELECT
    count_bad_cells,
    list_dead_cells || list_noisy_cells
FROM module_pedestal_test;
""")
# Commit changes and close connection
conn.commit()
cur.close()
conn.close()
