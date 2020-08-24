import os, sys, json

import config

class HtmlStr:
    
    def read_json_file(self, table_id, company_id):
        json_dir = config.Config.table_json_path.format(company_id)
        json_file_path = os.path.join(json_dir, '{0}.json'.format(table_id))        
        json_dct = {}
        with open(json_file_path, 'r') as j:
            json_dct = json.load(j)
        return json_dct
        
    def create_table_wise_html_string(self, company_id, table_id):
        color_dct = {"gh": {"bgcolor":"#e01e5a", "color":"#ffffff"}, "hch": {"bgcolor":"#21aad4", "color":"#ffffff"}, "vch": {"bgcolor":"#f2b737", "color":"#ffffff"}, "value": {"bgcolor":"#2fb67c", "color":"#ffffff"}}
        json_data = self.read_json_file(table_id, company_id)
        grid_data = json_data['data']
        
        rc_cell_dct = {}
        for rc, cell_dict in grid_data.iteritems():
            srow, scol = map(int, rc.split('_'))
            rc_cell_dct.setdefault(srow, {})[scol] = cell_dict
        
        sorted_rows = sorted(rc_cell_dct.keys()) 
        html_table_str = '<table border=1>'
        html_table_str += "<tbody>"
        html_table_str += "<tr>" +"TABLE ID:  "+table_id+"</tr>" 
        for row in sorted_rows:
            html_table_str += "<tr>"
            sorted_cols = sorted(rc_cell_dct[row])
            for col in sorted_cols:
                c_dct = rc_cell_dct[row][col]
                col_span = c_dct['colspan']
                cell_val = c_dct['data']
                stype = c_dct['ldr']
                clr_info = color_dct[stype]
                bgcolor, color = clr_info['bgcolor'], clr_info['color']
                html_table_str += "<td colspan='{0}' style='background-color:{1}; color:{2}'>".format(col_span, bgcolor, color)+str(cell_val)+ "</td>"
            html_table_str += "</tr>" 
        html_table_str += "</tbody>"
        html_table_str += "</table>"
        html_table_str += "<hr>"
        html_table_str += "<br><br>"
        return html_table_str
                
    def given_tables_create_htmls(self, ijson):
        company_id = ijson['company_id']
        table_ids  = ijson['table_ids']
        for idx, tableid_lst in  enumerate(table_ids):
            html_str = "<html><body>"
            for table_id in tableid_lst:
                html_str += self.create_table_wise_html_string(company_id, table_id)
            html_str += "</body></html>"
            html_path_dir = config.Config.html_file_path.format(company_id)
            if not os.path.exists(html_path_dir):
                os.system('mkdir -p {0}'.format(html_path_dir))
            #html_path = os.path.join(html_path_dir, '{0}.html'.format(idx))
            html_path = '/var/www/html/prashant/{0}.html'.format(idx)
            print html_path
            f = open(html_path, 'w')
            f.write(html_str)
            f.close()
        res = [{'message':'done'}]
        return res

if __name__ == '__main__':
    h_Obj = HtmlStr()
    ijson = {"company_id":"1053730", "table_ids":[['1_56_1', '1_57_2'], ['1_50_1', '1_49_2', '168_32_1']]}
    h_Obj.given_tables_create_htmls(ijson)
    

        
 
