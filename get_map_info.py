import sqlite3
import json
class map_info:
    def __init__(self):
        self.mapping_path        = '/mnt/eMB_db/company_management/{0}/mapping_info/{1}/{2}.db' #{0:company_id, 1:project_id, 2 : template_id}
    

    def make_connection(self, db_path):
        self.conn = sqlite3.connect(db_path)
        self.cur = self.conn.cursor()

    def read_mapping_info(self):
        sql = "select id, taxo_id, doc_id, table_id, hv_flag, r_c, xml_id, user_id, mtext from mapping_info"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        return res

    def get_map_info(self, ijson):
        company_id, project_id, template_id  = ijson['company_id'], ijson['project_id'], ijson['template_id']
        db = self.mapping_path.format(company_id, project_id, template_id)
        self.make_connection(db)
        res = self.read_mapping_info()
        self.conn.close()
        fs = {}
        ldr_sections = {'hch': "HGH", "vch":"VGH", "value": "GV"}
        for dd in res:
            tid, taxo_id, doc_id, table_id, hv_flag, r_c, xml_id, user_id, mtext = dd
            fs.setdefault(taxo_id, []).append({'doc_id': doc_id, 'table_id': table_id, 'hv_flag': ldr_sections.get(hv_flag, hv_flag), 'xml_id': xml_id})
            
        return fs


if __name__ == '__main__':
    obj = map_info()
    ijson = {"company_id":1604, "project_id":5, "template_id":7}
    res = obj.get_map_info(ijson)
    print json.dumps(res)
        
        
