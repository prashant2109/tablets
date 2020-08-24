import shelve
shl_path    = '/var/www/html/WorkSpaceBuilder_DB/1604/1/pdata/docs/2/dstructdata/lextree_2.slv'
sh = shelve.open(shl_path, 'r')
data = sh['data']
for grid, rc_dct in data.iteritems():
    if grid != '2#15#7#7':continue
    print grid, rc_dct
