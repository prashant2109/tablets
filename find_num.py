from openpyxl.utils.cell import coordinate_from_string, column_index_from_string
xy = coordinate_from_string('ID3') # returns ('A',4)
col = column_index_from_string(xy[0]) # returns 1
row = xy[1]
print row, col
