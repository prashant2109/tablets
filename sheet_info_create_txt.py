import os, sys, json
import config
import db.get_conn as get_conn
conn_obj    = get_conn.DB()
import modules.template_mgmt.model_api as ma 
ma_obj = ma.model_api()


class SheetInfo:
    def __init__(self):
        self.config = config.Config()
    
    def sheet_drop_down(self, ijson):
        print 'Aniket' 
        company_id = ijson.get('company_id', '1604')
        template_id   = ijson.get('template_id', '4')
        project_id   = ijson.get('project_id', '5')
        doc_page_dct = self.read_distinct_doc_ids(company_id)
        db_path = self.config.sheet_path
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT sheet_id, sheet_name FROM sheet_mgmt WHERE template_id=4; """
        print read_qry
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        info_data = {r[0]: r[1] for r in t_data}
        res_lst = []
        for sheet_id, sheet_name in info_data.iteritems():
            print sheet_id, sheet_name
            d_dct = {'k':sheet_id, 'n':sheet_name}
            ijson['template_id'] = '4'
            ijson['sheet_id'] = sheet_id
            ijson['type']  = 'dynamic'
            ijson['WITH_META']  = 'Y'
            res_lst.append(d_dct)
            sheet_res = ma_obj.sheet_gridinfo(ijson)
            sheet_res[0]['bbox'] = doc_page_dct
            sheet_res = json.dumps(sheet_res)
            sheet_text_path = self.config.sheet_txt_path.format(company_id, project_id, template_id, sheet_id)
            sf = open(sheet_text_path, 'w') 
            sf.write(sheet_res)
            sf.close()
        res = [{'message':'done', 'data':res_lst}]
        txt_path = self.config.table_info_v1.format(company_id, project_id)    
        f = open(txt_path, 'w')
        data = json.dumps(res)
        f.write(data)
        f.close()
        print 'Creating Excel....'
        import create_excel
        obj = create_excel.excel()
        tres    = obj.cl_create(ijson)
        tpath   = '/var/www/html/demo_data/{0}/{1}/Reported_Restated/Output_{2}.zip'.format(company_id, project_id, template_id)
        cmd = 'cp "%s" "%s"'%(tres[0]['des'], tpath)
        print cmd
        os.system(cmd)
        return res
        
    def read_distinct_doc_ids(self, company_id):
        cid_path = self.config.comp_path.format(company_id)
        db_path = os.path.join(cid_path, 'document_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT doc_id FROM document_meta_info; """        
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        doc_page_coords = {}
        for row in t_data:
            doc_id = row[0]
            dir_path = self.config.equality_path.format(company_id)    
            db_path = os.path.join(dir_path, '{0}.db'.format(doc_id))
            conn, cur   = conn_obj.sqlite_connection(db_path)
            read_qry = """ SELECT pageno, pagesize FROM pagedet; """
            cur.execute(read_qry)
            t_data = cur.fetchall()
            conn.close()
            for pg, psz in t_data:
                psz = eval(psz)
                doc_page_coords.setdefault(doc_id, {})[pg] = psz    
        return doc_page_coords
   
if __name__ == '__main__':
    s_Obj = SheetInfo()
    ijson = {'company_id':'1604', 'template_id':'4', 'project_id':'5', 'company_name': 'Monroe Capital LLC','template_name': 'CLO Template'} 
    s_Obj.sheet_drop_down(ijson)
