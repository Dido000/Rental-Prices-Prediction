import os
import csv
import pandas as pd


base_dir = '/Users/T/science/datahelp/ReshapeData/VacantUnit/'
# base_dir = '/Users/T/science/datahelp/data/'
filename = 'VacantUnitOtherl.csv'
fname, fext = os.path.splitext(filename)
fullpath = base_dir + filename
infile = open(fullpath,'rU')
data_list = list(csv.reader(infile, dialect=csv.excel_tab, delimiter=','))

headers = data_list[0]
n_columns = len(headers)

# column idx in the original data
zip_idx = 0
zip_to_rows = dict()

zipcodes = []
n_errors = 0
for r, row in enumerate(data_list):
    if row[zip_idx] != headers[0]:
        zip_to_rows[row[zip_idx]] = r
        zipcodes.append(row[zip_idx])

    if len(row) != n_columns:
        n_errors += 1

if n_errors > 0:
    print str(n_errors) + ' didn\'t match'

unique_zips = sorted(list(set(zipcodes)))

print headers


candidate_years = [str(ty) for ty in range(2000,2020)]

to_delete = candidate_years
to_delete.append('[inyeargeog]') # this is a hack because the 2000 and spaces get replaced in the actual xls string '[in year 2000 geog]'
# to_delete.append('%') # in Other_Pop_Household.csv
to_delete.append('\'') # in Other_Pop_Household.csv
to_delete.insert(0,'[inyear2000geog]')

# get rid of the first column ('Zip')
headers_measures = headers[1:]

# map original header names (with year) to the filtered unique headers
headers_dict = dict()
headers_to_filtered = dict()
headers_to_year = dict()
headers_old_to_idx = dict() # map the original header names to their column in raw data

filtered_headers = [] # measure names with no spaces or years
unique_headers = [] # unique filtered headers
actual_years = [] # years that are used anywhere in the data (subset of candidate_years)

for r, row in enumerate(headers_measures):

    headers_old_to_idx[row] = r
    orig_row = row
    row = row.replace(', ','')
    row = row.replace(' ','')

    # strip out year or any other undesired string
    for delete_string in to_delete:
        if delete_string in row:
            row = row.replace(delete_string,'')
            if delete_string != str('%') and delete_string != '[inyear2000geog]' and delete_string != '\'':

                headers_to_year[orig_row] = delete_string
                actual_years.append(delete_string)

    filtered_headers.append(row)
    headers_to_filtered[orig_row] = row

unique_headers.append(list(set(filtered_headers)))
unique_headers = unique_headers[0]
actual_years = sorted(list(set(actual_years)))

my_current = headers[5]
print my_current + ' / ' + headers_to_filtered[my_current] + ' / ' + str(headers_to_year[my_current]) + ' / ' + str(headers_old_to_idx[my_current])
print actual_years


new_headers = ['Zip','Year']
headers_new_to_idx = dict() # store the column idx of each measure
col_idx = len(new_headers)+1

for uh in unique_headers:
    new_headers.append(uh)
    headers_new_to_idx[uh] = col_idx
    col_idx += 1

df = pd.DataFrame(columns=new_headers)

z_counter = 0
y_counter = 0
zip_year_dict = dict() # map zipcode-year combos to row in dataframe

for zc in zipcodes:
    for y in actual_years:
        df.loc[z_counter,'Zip'] = zc
        df.loc[y_counter, 'Year'] = y
        zip_year_dict[str(zc)+str(y)] = z_counter
        z_counter += 1
        y_counter += 1

# df[0:3]


for c, old_measure in enumerate(data_list[0]):

    if old_measure != 'Zip':

        new_measure = headers_to_filtered[old_measure] # target measure in dataframe
        old_year = headers_to_year[old_measure]

        for r in range(1,len(data_list)): # loop over the data rows
            zc = data_list[r][0]
            old_val = data_list[r][c]

            c_row = zip_year_dict[str(zc)+str(old_year)]

            df.loc[c_row, new_measure] = old_val

print 'finished'


# base_dir = '/Users/T/science/datahelp/data/'
# filename = 'QualityofLifeCrime.csv'

output_name  = base_dir + fname + '_reshaped' + fext

df.to_csv(output_name, sep=',')

print 'saved output ' + output_name
# df[0:3]
