import os, sys, json, shelve, ast, subprocess
import sqlite3, json

class Similarity:
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

    def insert_group_info(self, company_id):
        try:
            sh_data = self.doc_group_info(company_id)
        except:sh_data = {}

        similartables = sh_data.get('similartables', {})
        
        grp_insert_rows_dct = {}
        for grp_idx, sim_dct in enumerate(similartables, 1):
            for doc, table_lst in sim_dct.iteritems():
                for table in table_lst:
                    table = '_'.join(table.split('#')[:3])
                    grp_insert_rows_dct.setdefault(grp_idx, {}).setdefault(doc, []).append(table)  
            
         
        insert_rows = []
        for grp, doc_dct in grp_insert_rows_dct.iteritems():
            for doc, table_list in doc_dct.iteritems():
                table_list = self.read_tables(company_id, doc, table_list)
                for tab in table_list:
                    insert_rows.append((grp, doc, tab))
            
        if insert_rows:
            cid_path = '/mnt/eMB_db/company_management/{0}/'.format(company_id)
            if not os.path.exists(cid_path):
                os.system(""" mkdir -p {0} """.format(cid_path))

            db_path = os.path.join(cid_path, 'table_info.db')
            conn, cur   = conn_obj.sqlite_connection(db_path)
            try:
                drop_stmt = """  DROP TABLE group_tables;   """ 
                cur.execute(drop_stmt)
            except:pass
    
            crt_stmt = """ CREATE TABLE IF NOT EXISTS group_tables(row_id INTEGER PRIMARY KEY AUTOINCREMENT, group_id TEXT, doc_id TEXT, table_id TEXT); """
            cur.execute(crt_stmt)
            insert_stmt = """ INSERT INTO group_tables(group_id, doc_id, table_id) VALUES(?, ?, ?); """
            cur.executemany(insert_stmt, insert_rows)
            conn.commit()
            conn.close()
        return
                    
    def doc_group_info(self, company_id):
        f_null = open(os.devnull, 'w')
        source_path = '/var/www/html/WorkSpaceBuilder_DB/{0}/1/pdata/acrossdata/tabsdata/similartables1_{0}.slv'.format(company_id)
        comp_path = '/mnt/eMB_db/company_management/{0}/similartables1_{0}.slv'.format(company_id)
        cid_path = '/mnt/eMB_db/company_management/{0}/'.format(company_id)
        if not os.path.exists(cid_path):
            os.system(""" mkdir -p {0} """.format(cid_path))
        else:
            cmd = """ rm -rf {0} """.format(comp_path)
        try:
            os.system(cmd)
        except:pass
        try:
            cmd1 = """ cp {0} {1}  """.format(source_path, comp_path)
            process = subprocess.Popen(cmd1, stdout=f_null, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
        except:pass
        sh_data = {}
        if comp_path:
            sh = shelve.open(comp_path)
            sh_data = sh['data']
            sh.close()
        return sh_data





