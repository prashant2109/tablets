import os, sys
import config
import db.get_conn as get_conn
conn_obj    = get_conn.DB()
    
class ModelInfo:
    def ascii_int(self, cell_str):
        row_lst = []
        col_str_lst = []
        for char in cell_str:
            if char.isalpha():
                ascii_int = ord(char) - 65
                col_str_lst.append(char)
            elif char.isdigit():
                row_lst.append(char)
        row = int(''.join(row_lst))
        col_str = ''.join(col_str_lst)
        col = self.col_to_index(col_str)
        return (row, col)
        
    def col_to_index(self, col):
        return sum((ord(c) - 64) * 26**i for i, c in enumerate(reversed(col))) - 1

    def convert_xl_cell_to_int_rc(self, info_str):
        top_left, bottom_right = info_str.split(':')
        print [top_left, bottom_right]
        tl_rc = self.ascii_int(top_left) 
        br_rc = self.ascii_int(bottom_right)
            
        r1, c1 = tl_rc
        r2, c2 = br_rc
        all_cord_cells = []
        for rw in range(r1, r2+1):
            for cl in range(c1, c2+1):
                all_cord_cells.append((rw, cl))
        row_span = (r2-r1)+1  
        col_span = (c2-c1)+1
        res = {'all_cells':all_cord_cells, 'rs':row_span, 'cs':col_span, 'cell':tl_rc}
        return res 
        
    def read_sheet_info(self, conn, cur, sheet_id):
        read_qry = """ SELECT extra_info FROM sheet_mgmt WHERE sheet_id='{0}' """.format(sheet_id)
        cur.execute(read_qry)
        t_data = cur.fetchone()
        ex_info = eval(t_data[0])
        merge_cell_info = {}
        if ex_info:
            merged_cells = ex_info['mergedCells']
            for cell_cmb in merged_cells:
                cell_info = self.convert_xl_cell_to_int_rc(cell_cmb)
                act_cell = cell_info['cell']
                merge_cell_info[act_cell] = cell_info    
        return merge_cell_info
        
    def insert_info(self, row, col, value_dct, rid):
        row_dct = {'r':row, 'c':col, 'v':{'ht':0, 'vt':0, 'ct':{'ca':'@', 't':'s'}, 'bg':'#fff',
                    'ff':'2', 'ff':10, 'fc':'#253473', 'v':value_dct['value'], 'map':{'id':rid},
                    }
                    } 
        error_flg = value_dct.get('Error_flg')
        if error_flg:
            row_dct['v']['ps'] = {'value':error_flg}
        border_dct = {
                    "rangeType": 'cell',
                    "value": {
                            "row_index": row,
                            "col_index": col,
                            "l": {
                                    "style": 1,
                                    "color": '#fff'
                            },
                            "r": {
                                    "style": 1,
                                    "color": '#fff'
                            },
                            "t": {
                                    "style": 1,
                                    "color": '#fff'
                            },
                            "b": {
                                    "style": 1,
                                    "color": '#fff'
                            }
                    }
                    
                }
        return row_dct, border_dct
        

    def output_model_info_structure(self, ijson):
        company_id    = ijson['company_id']
        project_id    = ijson['project_id']
        template_id   = ijson['template_id']
        sheet_id      = ijson['sheet_id']
        db_path = config.Config.mapping_path.format(company_id, project_id, template_id) 
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT row_id, row, col, value FROM model_info WHERE sheet_id='{0}'; """.format(sheet_id)
        cur.execute(read_qry)
        t_data = cur.fetchall()
        merge_cell_info = self.read_sheet_info(conn, cur, sheet_id)
        conn.close()
        
        main_dct = {}
        for rw_dt in t_data:
            row_id, rw, cl, vl_dct = rw_dt
            #print eval(vl_dct), '\n'
            main_dct[(rw, cl)] = (vl_dct, row_id)

        #sys.exit()
        
        data_lst = [] 
        border_lst = []
        all_mc_dct = {}
        for rd, (val_str, rid) in main_dct.iteritems():
            row, col = rd
            value_dct = eval(val_str)
            if not value_dct.get('value'):continue
            #print value_dct, '\n'
            merge_cell = merge_cell_info.get((row, col), {})
            row_dct, border_dct = self.insert_info(row, col, value_dct, rid)
            cl_type = value_dct.get('cl_t')
            if cl_type:
                border_dct['cl_t'] = cl_type
               
            ctype = value_dct.get('c_type') 
            if ctype:
                border_dct['c_type'] = ctype
            data_lst.append(row_dct)
            border_lst.append(border_dct) 
            # value['cl_t'] == 'PH'  --- PH Header
            # value['c_type'] == 'H' --  column Header
                 
            if merge_cell.get('all_cells', []):
                all_cells, a_cell, rs, cs = merge_cell['all_cells'], merge_cell['cell'], merge_cell['rs'], merge_cell['cs']
                all_mc_dct['{0}_{1}'.format(row, col)] = {"r": row, "c": col, "rs": rs, "cs":cs}
                data_lst[-1]['v']['mc'] = {"r": row, "c": col, "rs":rs, "cs":cs}
                #data_lst[-1]['v']['mc'] = {"r": row, "c": col, "rs":rs, "cs":cs}
                for rc_tp in all_cells:
                    ch_rw, ch_cl = rc_tp
                    ch_vl_dct, chrid = main_dct[rc_tp]
                    ch_vl_dct = eval(ch_vl_dct)
                    m_row_dct, m_border_dct = self.insert_info(ch_rw, ch_cl, ch_vl_dct, chrid)
                    ch_cl_type = ch_vl_dct.get('cl_t')
                    if ch_cl_type:
                        border_dct['cl_t'] = ch_cl_type
                       
                    ch_ctype = ch_vl_dct.get('c_type') 
                    if ch_ctype:
                        border_dct['c_type'] = ctype
 
                    data_lst.append(m_row_dct)
                    border_lst.append(m_border_dct) 
                    all_mc_dct['{0}_{1}'.format(ch_rw, ch_cl)] = {"r": ch_rw, "c":ch_cl, "rs":'', "cs":''}
        res = [{'message':'done', 'data':data_lst, 'border':border_lst, 'mergeCells':all_mc_dct}]
        return res
                    
       

if __name__ == '__main__':
    m_Obj = ModelInfo()    
    #ijson = {"company_id":1053730, "project_id":5, "template_id":4, "sheet_id":8}
    ijson = {"company_id":1053729, "project_id":5, "template_id":4, "sheet_id":13}
    #print m_Obj.output_model_info_structure(ijson)
    #info_str = 'AX1:BD1' 
    #print m_Obj.convert_xl_cell_to_int_rc(info_str)
    print m_Obj.output_model_info_structure(ijson)

 

