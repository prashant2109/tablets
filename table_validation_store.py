import os, sys, json, sqlite3, logging, datetime 


class ErrorValidation:
    
    def read_json_file(self, table_id, company_id):
        print 'READ ',(company_id, table_id)
        json_file_path = '/mnt/eMB_db/company_management/{0}/json_files/{1}.json'.format(company_id, table_id)
        json_dct = {}
        if not os.path.exists(json_file_path):
            return json_dct
        with open(json_file_path, 'r') as j:
            json_dct = json.load(j)
        return json_dct

    def grid_span_info(self, grid_json):
        ddict = grid_json.get('data', {})

        span_dct = {}
        rc_info_dct = {}  
        for rc, cell_dict in ddict.iteritems():
            stype = cell_dict.get('ldr')
            if stype != 'value':continue
            row, col = rc.split('_')
            col_span = cell_dict.get('colspan')
            row_span = cell_dict.get('rowspan')
            #span_dct.setdefault('row_span', {}).setdefault(col, set()).add(row_span)
            #span_dct.setdefault('col_span', {}).setdefault(row, set()).add(col_span)
            rc_info_dct.setdefault('row_span', {}).setdefault(col, {})[rc] = col_span
            rc_info_dct.setdefault('col_span', {}).setdefault(row, {})[rc] = row_span
        
        #print rc_info_dct
        #sys.exit()
           
        error_info = {}
        for span, rc_dct in rc_info_dct.iteritems():
            for rw_cl, rc_span_dct in rc_dct.iteritems():
                if len(set(rc_span_dct.values())) > 1:
                    for r_c, span_int in rc_span_dct.iteritems():
                        if int(span_int) > 1:
                            er_msg = 'MULTIPLE SPAN -- {0}'.format(span)
                            error_info.setdefault(er_msg, {})[r_c] = span_int 
        
        return error_info  
        
    def validate_col_row_span_error(self, company_id, tables_tup, error_tables):
        for dpg_tup in tables_tup:
            doc, page, grid = dpg_tup
            dpg = '{0}_{1}_{2}'.format(doc, page, grid)
            grid_json = self.read_json_file(dpg, company_id)
            span_error = self.grid_span_info(grid_json) 
            if span_error:
                error_tables.setdefault(str(doc), {}).setdefault(dpg, {})
                error_tables[str(doc)][dpg].update(span_error)
        return 
        
    def table_validation(self, ijson):
        company_id = ijson['company_id']
        doc_id     = ijson['doc_id']
        import numeq_table_lets_DB as ntlb
        t_Obj = ntlb.Table_Lets_DB()
        error_tables, tables_tup  = t_Obj.find_error_tables_map_label_equality(ijson)
        self.validate_col_row_span_error(company_id, tables_tup, error_tables)

        insert_rows = []
        for doc, grid_dct in error_tables.iteritems():
            for grid, error_dct in grid_dct.iteritems():
                if error_dct:
                    insert_rows.append((doc, grid, json.dumps(error_dct)))
     
        #print insert_rows   
        db_path = '/mnt/eMB_db/company_management/{0}/table_info.db'.format(company_id)
        #print db_path 
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()        
        crt_stmt = """ CREATE TABLE IF NOT EXISTS error_tables(row_id INTEGER PRIMARY KEY AUTOINCREMENT, doc_id TEXT, table_id TEXT, error_info TEXT); """
        cur.execute(crt_stmt)
        del_stmt = """ DELETE FROM error_tables WHERE doc_id='%s';  """%(doc_id)
        cur.execute(del_stmt)
        conn.commit()
        if insert_rows:
            insert_stmt = """ INSERT INTO error_tables(doc_id, table_id, error_info) VALUES(?, ?, ?); """
            cur.executemany(insert_stmt, insert_rows)
            conn.commit()
        conn.close()
        return 
        
    def return_table_exception_flg(self, ijson):
        company_id = ijson['company_id']
        doc_str    = ijson['doc_str']
        db_path = '/mnt/eMB_db/company_management/{0}/table_info.db'.format(company_id)
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()        
        crt_stmt = """ CREATE TABLE IF NOT EXISTS error_tables(row_id INTEGER PRIMARY KEY AUTOINCREMENT, doc_id TEXT, table_id TEXT, error_info TEXT); """
        cur.execute(crt_stmt)
        read_qry = """  SELECT doc_id FROM error_tables WHERE doc_id IN (%s); """%(doc_str)
        cur.execute(read_qry)
        t_data = cur.fetchall() 
        conn.close()
        if t_data:
            error_store_path = '/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/error_logs/%s_error.log'%(company_id) 
            logging.basicConfig(filename=error_store_path, filemode='a', format='%(message)s', level=logging.INFO)        
            logging.info('************* ERROR IN DOCUMENT **************')
            dt = str(datetime.datetime.now())
            logging.info(dt)
            logging.info(doc_str)
            logging.info(db_path)
            logging.info('Please check above path SQL-TABLE {error_tables}')
            logging.info('*********************************\n')
            sys.exit('TABLE ERROR')       
        return


if  __name__ == '__main__':
    ev_Obj = ErrorValidation()
    ijson = {"company_id":1053739, "doc_id":13}
    ev_Obj.table_validation(ijson)

