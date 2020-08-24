import sqlite3, json
class Table():
    def read_tables(self, company_id, doc_id, tableid_list=[]):
        db_path = '/mnt/eMB_db/company_management/{0}/table_info.db'.format(company_id)
        con         = sqlite3.connect(db_path, timeout=30000)
        cur         = con.cursor()
        sql         = "select doc_id, page_no, grid_id from table_mgmt where doc_id=%s"%(doc_id)
        cur.execute(sql)
        res         = cur.fetchall()
        done_d  = {}
        for r in res:
            doc_id, page_no, grid_id    = r
            table_id    = '{0}_{1}_{2}'.format(doc_id, page_no, grid_id)    
            if tableid_list:
                if table_id not in tableid_list:continue
            if table_id in done_d:continue
            json_file   = '/mnt/eMB_db/company_management/{0}/json_files/{1}.json'.format(company_id, table_id)
            grid_data   = json.loads(open(json_file, 'r').read())
            done_d[table_id]    = (int(page_no), (int(grid_data['table_boundry'][1]), int(grid_data['table_boundry'][1])+int(grid_data['table_boundry'][3])))
        table_ids   = done_d.keys()
        table_ids.sort(key=lambda x:done_d[x])
        #for r in table_ids:
        #    print r, done_d[r]
        return table_ids

if __name__ == '__main__':
    obj = Table()
    print obj.read_tables(1053724, 3)
        
