import os, sys, json

import config

import db.get_conn as get_conn
conn_obj    = get_conn.DB()

def disableprint():
    sys.stdout = open(os.devnull, 'w')
    pass

def enableprint():
    sys.stdout = sys.__stdout__

class ErrorValidation:
        
    def __init__(self):
        self.error_msg_map = {
                                'row_span' : 'ROWSPAN',
                                'col_span' : 'COLSPAN',
                                'GRID DOES NOT EXISTS IN LABEL INFO'  : 'GRID MISSING IN LABEL INFO',
                                'XML DOES NOT EXISTS IN LABEL INFO'   : 'XML MISSING IN LABEL INFO',
                                }
        self.err_msg_order = [
                            'ROWSPAN', 
                            'COLSPAN',
                            'GRID MISSING IN LABEL INFO',
                            'XML MISSING IN LABEL INFO',
                            ]


    def error_table_stats(self, ijson):
        company_id = ijson['company_id']
        cid_path = config.Config.comp_path.format(company_id)
        db_path = os.path.join(cid_path, 'table_info.db')
        print db_path 
        try:
            conn, cur   = conn_obj.sqlite_connection(db_path)
            read_qry = """ SELECT doc_id, table_id, error_info FROM error_tables;  """
            cur.execute(read_qry)
            t_data = cur.fetchall()
            conn.close()
        except:t_data = ()
        
        if not t_data:
            res = [{'message':'done', 'data':[]}]
            return res 
                
        res_error_dct = {}
        all_error_tables = {}
        for row_data in t_data:
            doc_id, table_id, error_info = row_data
            error_info_dct = json.loads(error_info) 
            for error_msg, rc_dct in error_info_dct.iteritems():
                res_error_dct.setdefault(error_msg, {})[table_id] = 1
                all_error_tables[table_id] = 1
        
        all_err_tab_cnt = len(all_error_tables)
        all_er_tab_list = all_error_tables.keys()
        res_lst = [{'k':'error_tables', 'n':'Error Tables', 'c':all_err_tab_cnt, 't_list':all_er_tab_list}]
        
        for e_msg, table_dct in res_error_dct.iteritems():
            display_error_msg = self.error_msg_map[e_msg]
            error_msg_tables = table_dct.keys()
            error_no_tables = len(error_msg_tables)
            row_dct = {'k':e_msg, 'n':display_error_msg, 'c':error_no_tables, 't_list':error_msg_tables}         
            res_lst.append(row_dct)
        res_lst.sort(key=lambda x:self.err_msg_order.index[x['n']])
        
        res = [{'message':'done', 'data':res_lst}]
        return

    def read_validation_error_rcs(self, ijson):
        company_id = ijson['company_id']
        table_id   = ijson['table_id']
        err_msg  = ijson['error_message'] 
        cid_path = config.Config.comp_path.format(company_id)
        db_path = os.path.join(cid_path, 'table_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT error_info FROM error_tables WHERE table_id='%s';  """%(table_id)
        cur.execute(read_qry)
        t_data = eval(cur.fetchone()[0])
        conn.close()
        res_lst = []
        for error_msg, rc_dct in   t_data.iteritems():
            if error_msg != err_msg:continue
            if row_dct.keys()[0] == 'ALL RCs':continue
            display_error_msg = self.error_msg_map[error_msg]
            row_dct = {'k':display_error_msg, 'n':rc_dct}
            res_lst.append(row_dct)
    
        res = [{'message':'done', 'data':res_lst}]
        return res
    

if __name__ == '__main__':
    ev_Obj = ErrorValidation() 
    ijson = {"company_id":"1053729", "project_id":5}    
    print ev_Obj.error_table_stats(ijson)

