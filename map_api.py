import re
import os
import json
import copy
import config
class exe:
    def __init__(self):
        pass
    def DecimalToAlphaConverter(self, dec_val):
        if(dec_val<26):
           return chr(65+dec_val)
        if(dec_val > 25 and dec_val < 52):
            return chr(65)+chr(65+(dec_val-26))
        if(dec_val > 51 and dec_val < 78):
            return chr(66)+chr(65+(dec_val-52))
        if(dec_val > 77 and dec_val < 104):
            return chr(67)+chr(65+(dec_val-78))
        return "@@"

    def find_range(self, colspan, rowspan, col, row):
        srow , scol = row+1, col
        if colspan >1  and rowspan > 1:
            erow = srow + (rowspan - 1)
            ecol = scol + (colspan - 1)
            scola = self.DecimalToAlphaConverter(scol)
            ecola = self.DecimalToAlphaConverter(ecol)
            return  scola+str(srow)+":"+ecola+str(erow)
        elif colspan > 1:
            ecol = scol + (colspan - 1)
            scola = self.DecimalToAlphaConverter(scol)
            ecola = self.DecimalToAlphaConverter(ecol)
            return scola+str(srow)+":"+ecola+str(srow)
        elif rowspan > 1:
            erow = srow + (rowspan - 1)
            scola = self.DecimalToAlphaConverter(scol)
            return scola+str(srow)+":"+scola+str(erow)
    

    def get_row_col(self,all_keys):
        rows    = []
        cols    = []
        for r in all_keys:
            row,col = map(lambda x: int(x),r.split('_'))
            if row not in rows:
                rows.append(row)
            if col not in cols:
                cols.append(col)
        rows.sort()
        cols.sort()
        return [rows,cols]


    def read_doc_wise_data(self, ijson, doc_id, doc_str, flag_info):
        import sqlite_api as sq1
        if doc_id not in flag_info:
            if ijson.get('pre_equality', '') == 'Y':
                db_path = "/mnt/eMB_db/company_management/%s/equality/%s_numeq.db"%(ijson['company_id'], doc_id)
                sq1_obj = sq1.sqlite_api(db_path)
                data = sq1_obj.read_eq_num_info(doc_str) 
                for dd in data:
                    cgg = '_'.join(dd[0].split('#')[:3])
                    flag_info.setdefault(doc_id, {}).setdefault('eq_num', {})[cgg] = 1
                db_path = "/mnt/eMB_db/company_management/%s/equality/%s.db"%(ijson['company_id'], doc_id)
                sq1_obj = sq1.sqlite_api(db_path)
            else:
                db_path = "/mnt/eMB_db/company_management/%s/equality/%s.db"%(ijson['company_id'], doc_id)
                sq1_obj = sq1.sqlite_api(db_path)
                data = sq1_obj.read_eq_info(doc_str) 
                print 'cdata', data
                for dd in data:
                    cgg = '_'.join(dd[0].split('#')[:3])
                    flag_info.setdefault(doc_id, {}).setdefault('eq', {})[cgg] = 1
            rr = sq1_obj.read_raw_info()
            for cc in rr:
                dd = '_'.join(cc[0].split('#')[:3])
                flag_info.setdefault(doc_id, {}).setdefault('raw_db', {})[dd] = 1
            print 'flag', doc_id, flag_info
        if ijson.get('pre_equality', '') == 'Y':
            data  = flag_info[doc_id].get('eq_num', {}).get(doc_str, '')
        else: 
            data  = flag_info[doc_id].get('eq', {}).get(doc_str, '')
        rr = flag_info[doc_id].get('raw_db', {}).get(doc_str, '')
        if data and rr:
            return 'G'
        elif data:
            return 'O'
        elif rr:
            return 'P'
        else:
            return 'R'

    def read_cell_wise_data(self, ijson, doc_id, map_info):
        import sqlite_api as sq1
        '''db_path = "/mnt/eMB_db/company_management/%s/equality/%s.db"%(ijson['company_id'], doc_id)
        sq1_obj = sq1.sqlite_api(db_path)
        data = sq1_obj.read_eq_info_full() 
        for dd in data:
            table_id1, table_id2, r_c1, r_c2,relationship_Type = dd
            map_info.setdefault(table_id1, {}).setdefault(r_c1, {})[relationship_Type] =1
            row, col = r_c1.split('_')
            map_info.setdefault(table_id1, {}).setdefault(row, {})[relationship_Type] = 1
            map_info.setdefault(table_id1, {}).setdefault('%s_eq'%(r_c1), [])[table_id2] = 1'''
        if ijson.get('pre_equality', '') == 'Y':
            db_path = "/mnt/eMB_db/company_management/%s/equality/%s_numeq.db"%(ijson['company_id'], doc_id)
            sq1_obj = sq1.sqlite_api(db_path)
            data = sq1_obj.read_eq_num_info_full() 
            for dd in data:
                gp1,row1, col1, gp2, row2, col2 = dd
                rc1 = '%s_%s'%(row1, col1)
                cgg = '_'.join(gp1.split('#')[:3])
                print cgg, rc1
                relationship_Type = 'DIST'
                map_info.setdefault(cgg, {}).setdefault(rc1, {})[relationship_Type] =1
                map_info.setdefault(cgg, {}).setdefault(str(row1), {})[relationship_Type] = 1
        else:
            db_path = "/mnt/eMB_db/company_management/%s/equality/%s.db"%(ijson['company_id'], doc_id)
            sq1_obj = sq1.sqlite_api(db_path)
            data = sq1_obj.read_eq_info_full() 
            for dd in data:
                table_id1, table_id2, r_c1, r_c2,relationship_Type = dd
                table_id1 = '_'.join(table_id1.split('#')[:3])
                table_id2 = '_'.join(table_id2.split('#')[:3])
                r_c1 = str(r_c1)
                map_info.setdefault(table_id1, {}).setdefault(r_c1, {})[relationship_Type] =1
                row, col = r_c1.split('_')
                map_info.setdefault(table_id1, {}).setdefault(row, {})[relationship_Type] = 1
                map_info.setdefault(table_id1, {}).setdefault('%s_eq'%(r_c1), {})[table_id2] = 1 
        return ''

    def get_grid_info(self, ijson):
        import sqlite_api as sq
        db  = "/mnt/eMB_db/company_management/%s/table_info.db"%(ijson['company_id'])
        sq_obj = sq.sqlite_api(db)
        db_info = sq_obj.get_grids_tb(ijson['doc_id'])
        flag_info = {}
        map_d , data, cols = {}, [], []
        for cid, dd in enumerate(db_info,1):
            import pra_redis_exc as pr
            pr_obj = pr.exe(ijson['company_id'], dd[0])
            header  = pr_obj.make_exec(dd[1], dd[2], 'gh')
            cgrid = '%s_%s_%s'%dd
            temp = {'g': {'v': cgrid}, 'gh': {'v': header}, 'flg': {}, 'cid': cid, 'rid': cid, 'sno': {'v': cid}}
            flgt = self.read_doc_wise_data(ijson, dd[0], cgrid, flag_info)
            temp['flg'] = {'v': flgt}
            map_key = "%s_%s"%(cid, '')
            data.append(temp)
        ff = [('sno', 'SNo'), ('g', 'Grid'), ('gh', 'Gheader'), ('flg', 'Flag')]
        for hh in ff:
            vtemp = {'k': hh[0], 'n': hh[1]}
            if hh[0] == 'flg':
                vtemp['v_opt'] = 2
            cols.append(vtemp)
        return [{'message':'done', 'coldef': cols, 'data': data, 'map': map_d}]

    def read_json_file(self, ijson, doc_str):
        file_path = "/mnt/eMB_db/company_management/%s/json_files/%s.json"%(ijson['company_id'], doc_str)
        f = open(file_path)
        data = json.loads(f.read())
        return data


    def read_forulas_info(self, ijson):
        doc_id = ijson['doc_id']
        import sqlite_api as sq11
        db_path = "/mnt/eMB_db/company_management/%s/doc_builder/%s.db"%(ijson['company_id'], doc_id)
        sq11_obj = sq11.sqlite_api(db_path)
        data = sq11_obj.read_fids()
        return map(lambda x: x[0], data)
        
    def read_grid_info_g(self, ijson):
        doc_str = "%s_%s_%s"%(ijson['d'], ijson['p'], ijson['g'])
        data = self.read_json_file(ijson, doc_str)
        gdata , gcols, gmap = [], [], {}
        rel_type_info = {} 
        self.read_cell_wise_data(ijson, ijson['d'], rel_type_info) 
        print 'rt', rel_type_info
        done_cols={}
        ad_avail_cols = {}
        for k, v in data.items():
            if k != 'data':continue
            all_keys        = v.keys()
            row_ids,col_ids = self.get_row_col(all_keys)
            print row_ids, col_ids
            for r_id in row_ids:
                temp = {'rid': r_id, 'cid': r_id}
                if str(r_id) in rel_type_info.get(doc_str, {}):
                    temp['rt'] = rel_type_info[doc_str][str(r_id)]
                #if ijson.get('pre_equality', '') == 'Y':
                #    temp['rt'] = {'DIST':1}
                for c_id in col_ids:
                    if c_id not in done_cols:
                        scola = self.DecimalToAlphaConverter(c_id)
                        done_cols[c_id] = 1
                        if scola == 'A':
                            scola = "Descrption"
                        gcols.append({'k': c_id, 'n': scola})
                    map_key = "%s_%s"%(r_id, c_id)
                    r_key = "%s_%s"%(r_id, c_id)
                    cell_info = v.get(r_key, {})
                    if not cell_info:continue
                    temp[c_id] = {'v': cell_info.get('data', {})}
                    print 'dddddddd', r_key, rel_type_info.get(doc_str, {}).get(r_key, {}), cell_info
                    if r_key in rel_type_info.get(doc_str, {}):
                        temp[c_id]['rt'] = rel_type_info[doc_str][r_key]
                    if '%s_eq'%(r_key) in rel_type_info.get(doc_str, {}):
                        temp[c_id]['eq'] = rel_type_info[doc_str]['%s_eq'%(r_key)]
                    #if ijson.get('pre_equality', '') == 'Y':
                    #    temp[c_id]['rt'] = {'DIST':1}
                    cbox = []
                    vbox = cell_info.get('bbox', '')
                    vxml = cell_info.get('xml_ids', '') 
                    vbox_lst = filter(lambda x:x, vbox.split('$$'))
                    xml_lst = filter(lambda x:x, vxml.split('$$'))
                    page = ''
                    if vbox_lst:
                        for ii, gg in enumerate(vbox_lst):
                            if len(xml_lst) >= ii:
                                np = xml_lst[ii].split('_')[1]
                                if not page:
                                    page = np
                            else:
                                np = page 
                            chh = map(lambda x:int(x), gg.split('_'))
                            cbox.append({'bbox': [[chh[0], chh[1], chh[2]- chh[0], chh[3] - chh[1]]], 'p': np, 'd': ijson['d']})
                    if temp[c_id].get('rt'):
                        ad_avail_cols[c_id] = 1
                        temp['da'] = 'Y'   
                    gmap[r_key] ={'ref_k': r_key, 'ref':{'values': cbox}, 'vv': cell_info.get('data', {})}
                    print 'uuuuuuuuu', gmap[r_key]
                print temp
                gdata.append(temp)
        for col in gcols:
            if col['k'] in ad_avail_cols:continue
            col['visible'] = False
        gmap['ref_path'] = self.ref_path_info_workspace(ijson['company_id']) 
        gmap.setdefault('page_coords', {})[ijson['d']] = self.read_page_cords(ijson)
        return [{'message':'done', 'data': gdata, 'coldef': gcols, 'map': gmap}]


    def read_page_cords(self, ijson):
        import sqlite_api as sq1
        doc_id  = ijson['d']
        db_path = "/mnt/eMB_db/company_management/%s/equality/%s.db"%(ijson['company_id'], doc_id)
        sq1_obj = sq1.sqlite_api(db_path)
        data = sq1_obj.read_page_cords() 
        res = {}
        for dd in data:
            res[dd[0]] = json.loads(dd[1])
        return res
        
                   
    def get_eq_rows(self, ijson):
        rc = ijson['rc']     
        #doc_id = ijson['d']
        #page = ijson['p']
        #grid = ijson['g']
        tb_str = ijson.get('table_id','')
        doc_id, page, grid = tb_str.split('#') 
        tb1 = tb_str
        #tb1 =  '%s_%s_%s'%(doc_id, page, grid)
        import sqlite_api as sq1
        rc_info = {}
        if ijson.get('pre_equality', '') == 'Y':
                r, c = rc.split('_')
                db_path = "/mnt/eMB_db/company_management/%s/equality/%s_numeq.db"%(ijson['company_id'], doc_id)
                sq1_obj = sq1.sqlite_api(db_path)
                data = sq1_obj.cell_wise_eq(tb1, r, c) 
                fs = []
                done_g = {}
                for dd in data:
                    kt = '#'.join(dd[0].split('#')[0:3]) #need to see
                    print kt
                    rc2 = '%s_%s'%(dd[1], dd[2]) 
                    rc_info.setdefault(kt, {})[rc2] = 1
                    if kt in done_g:continue
                    done_g[kt] = 1
                    fs.append({'k': kt, 'n': kt})
        else:
            db_path = "/mnt/eMB_db/company_management/%s/equality/%s.db"%(ijson['company_id'], doc_id)
            sq1_obj = sq1.sqlite_api(db_path)
            data = sq1_obj.read_eq_info_rc(tb1, rc) 
            fs = []
            done_g = {}
            for dd in data:
                rc2 = dd[1]
                rc_info.setdefault(dd[0], {})[rc2] = 1
                if dd[0] in done_g:continue
                done_g[dd[0]] = 1
                fs.append({'k': dd[0], 'n': dd[0]})
        return [{'message': 'done', 'data':fs, 'rc_info': rc_info}]


    def ref_path_info_workspace(self, company_id):
        if str(company_id) == '1117':
            path1   = '/var_html_path/WorkSpaceBuilder_DB/34/1/pdata/docs/'
        else:
            path1   = '/var_html_path/WorkSpaceBuilder_DB/%s/1/pdata/docs/'%(company_id)
        ref_path    = {
                        'ref_html':'%s/{0}/html_output/{1}.html'%(path1.replace('/var/www/html', '')),
                        'ref_pdf':'/pdf_canvas/viewer.html?file=%s{0}/pages/{1}.pdf'%(path1),
                        }
        return ref_path

    def read_grid_info(self, ijson):
        doc_str1 = ijson['table_id']
        doc_str  = '_'.join(doc_str1.split('#')[0:3]) #need to see
        cr_info = ijson.get('rc', {})
        data = self.read_json_file(ijson, doc_str)
        fs_res = {'rows':{}, 'styles': [], 'merges': [], 'map': {}, 're_map': {}}
        styles = {"gh": {"bgcolor":"#e01e5a", "color":"#ffffff"}, "hch": {"bgcolor":"#21aad4", "color":"#ffffff"}, "vch": {"bgcolor":"#f2b737", "color":"#ffffff"}, "value": {"bgcolor":"#2fb67c", "color":"#ffffff", "align": "right"}, "footnote": {"bgcolor":"#d9925b", "color":"#ffffff"}, 'eq':{"color":"#ffffff","bgcolor":"#ff7f50","border":"2px solid #66608b"}}
        if not data['data']:
            return {'message': 'done', 'spreadsheet_data': fs_res, 'status': 'N'}
        style_map_key = {}
        for ldr, style in styles.items():
            style_map_key[ldr] = len(fs_res['styles'])
            fs_res['styles'].append(style)
            width_ldr = '%s_%s'%(ldr, 'width')
            new_style = copy.deepcopy(style)
            new_style['width'] = "350px"
            style_map_key[width_ldr] = len(fs_res['styles'])
            fs_res['styles'].append(new_style)
        for k, v in data.items():
            if k != 'data':continue
            all_keys        = v.keys()
            row_ids,col_ids = self.get_row_col(all_keys)
            #print [row_ids, col_ids]
            fs_res['row_len'] = row_ids[-1] + 1 
            fs_res['col_len'] = col_ids[-1] + 1
            for r_id in row_ids:
                fs_res['rows'][r_id] = {'cells':{}}
                for c_id in col_ids:
                    map_key = "%s_%s"%(r_id, c_id)
                    r_key = "%s_%s"%(r_id, c_id)
                    if r_key not in v:continue
                    fs_res['map'][map_key] = {}
                    for ck, cv in v[map_key].items():
                        if ck not in fs_res['re_map']:
                            fs_res['re_map'][ck] = len(fs_res['re_map'].keys())+1
                        fs_res['map'][map_key][fs_res['re_map'][ck]] = cv
                        cell_info = v[r_key]
                        temp = {'text': cell_info['data']}
                        colspan, rowspan, ldr = cell_info.get('colspan', 1), cell_info.get('rowspan', 1), cell_info.get('ldr', '')
                        colspan = int(colspan)
                        rowspan = int(rowspan)
                        if colspan > 1 or rowspan > 1:
                            fs_res['merges'].append(self.find_range(colspan, rowspan, c_id, r_id))
                            temp['merge'] = [rowspan - 1, colspan- 1]
                        if ldr and style_map_key.get('ldr', '-1') != -1:
                            #temp['style'] = style_map_key[ldr]
                            if c_id == col_ids[0]:# and ldr == 'hch':
                                temp['style'] = style_map_key[ldr+"_width"]
                            else:
                                temp['style'] = style_map_key[ldr]
                        elif ldr:
                            temp['snf'] = 1
                        if r_key in cr_info:
                            temp['style'] = style_map_key['eq']
                            #print 'rccc', r_key, temp['style']
                        fs_res['rows'][r_id]['cells'][c_id] = temp
        ijson['d'] = ijson['doc_id']
        ref_path = self.ref_path_info_workspace(ijson['company_id']) 
        page_coords  = {}
        page_coords[ijson['d']]  = self.read_page_cords(ijson)
        return [{'message': 'done', 'spreadsheet_data': fs_res, 'status': 'N', 'ref_path': ref_path, 'page_coords': page_coords}]

    def read_table_ids(self, ijson):
        import sqlite_api as sq1
        doc_id = ijson['doc_id']
        db_path = "/mnt/eMB_db/company_management/%s/doc_builder/%s.db"%(ijson['company_id'], doc_id)
        tb_map_info = {}
        if ijson.get('tab', '') == 'Builder':
            db_path = config.Config.databuilder_path_test.format(ijson['company_id'], ijson['project_id']) 
            import sqlite_api as sq
            sq_obj = sq1.sqlite_api("/mnt/eMB_db/company_management/global_info.db")
            vdinfo = sq_obj.read_tb_ids_info()
            for gg in vdinfo:
                tb_map_info[gg[0]] = gg[1]
        sq1_obj = sq1.sqlite_api(db_path)
        print os.path.isfile(db_path), db_path, tb_map_info
        if os.path.isfile(db_path):
            cdata = sq1_obj.read_db_builder1()
        else:
            cdata = []
        ff = [('sno', 'SNo'), ('g', 'Grid'),('flg', 'Flag')]
        done_g = {}
        data = []
        cols  = []    
        map_d = {}
        cid = 1
        for dd in cdata:
            row_id, table_type, taxo_group_id, restated_flag, formula_flag, label_change_flag = dd
            if not table_type:continue
            tt = table_type #'%s_%s'%(table_type, taxo_group_id) 
            if tt in done_g:continue
            done_g[tt] = 1
            flg = 'R'
            if restated_flag == 'Y' or formula_flag == 'Y' and label_change_flag == 'Y':
                flg = 'G' 
            temp = {'sno': {'v': cid}, 'g': {'v': table_type}, 'flg':{'v': flg}, 'cid': cid, 'rid':cid, 't_id': table_type}
            if ijson.get('tab', '') == 'Builder':
                print [row_id, table_type]
                temp['g']['v'] = tb_map_info.get(int(table_type), table_type)     
            data.append(temp)
            cid = cid + 1
        for hh in ff:
            vtemp = {'k': hh[0], 'n': hh[1]}
            vtemp['pin'] = 'pinnedLeft'
            if hh[0] == 'flg':
                vtemp['v_opt'] = 2
            cols.append(vtemp)
        return [{'message':'done', 'coldef': cols, 'data': data, 'map': map_d}]

    def read_group_ids(self, ijson):
        import sqlite_api as sq1
        doc_id = ijson['doc_id']
        db_path = "/mnt/eMB_db/company_management/%s/doc_builder/%s.db"%(ijson['company_id'], doc_id)
        if ijson.get('tab', '') == 'Builder':
            db_path = config.Config.databuilder_path_test.format(ijson['company_id'], ijson['project_id']) 
        sq1_obj = sq1.sqlite_api(db_path)
        cdata = sq1_obj.read_db_builder()
        done_g = {}
        data = []
        cols  = []    
        map_d = {}
        cid = 1
        for dd in cdata:
            if dd[0] != ijson['table_type']:continue
            if dd[1] in done_g:continue
            done_g[dd[1]] = 1
            data.append({'k': dd[1],'n': dd[1]})
        return [{'message':'done', 'data': data}]
            
    def read_databuilder_info(self, ijson):
        import sqlite_api as sq1
        doc_id = ijson['doc_id']
        db_path = "/mnt/eMB_db/company_management/%s/doc_builder/%s.db"%(ijson['company_id'], doc_id)
        if ijson.get('tab', '') == 'Builder':
            db_path = config.Config.databuilder_path_test.format(ijson['company_id'], ijson['project_id']) 
        sq1_obj = sq1.sqlite_api(db_path)
        table_type = ijson['table_type']
        taxo_group_id = ijson['taxo_group_id']
        fids = self.read_forulas_info(ijson)
        cdata = list(sq1_obj.read_rc_info(table_type, taxo_group_id))
        cdata.sort(key = lambda x: (x[0], x[1]))
        ref_info = sq1_obj.read_reference_table()
        ref_dict = {}
        cols = []
        data = []
        map_d = {}
        done_h = {}
        index_map = {}
        for tt in ref_info:
            ref_dict[tt[0]] = tt
        for dd in cdata:
            #print dd
            row, col, value, cell_type, row_id = dd
            if col not in done_h:
                if col == 0:
                    cols.append({'k': col, 'n': 'Descrption',"pin":"pinnedLeft"})
                else:
                    cols.append({'k': col, 'n': col})
                done_h[col] = 1
            if row not in index_map:
                index_map[row] = len(data)
                data.append({'cid': row, 'rid':row})
            cdata = data[index_map[row]]
            #print 'accccccccc', row_id, type(row_id), row_id in fids
            if row_id in fids:
                print 'accccccccc', row_id
                cdata[col] = {'v': value, 'cls': 'resultant-formula'}
            else:
                cdata[col] = {'v': value}
            map_key = '%s_%s'%(row, col)
            cbox = []
            if row_id in ref_dict:
                nrow_id, rawdb_row_id, xml_id, bbox, page_no = ref_dict[row_id]
                try:
                    bbox = eval(bbox)
                except:bbox = [[]]
                cbox.append({'x': xml_id, 'bbox': bbox[0], 'p': page_no, 'd': doc_id, 'cell_type': cell_type})
            map_d[map_key] = {'ref':{'values': cbox}, 'cell_id': row_id, 'ref_k': row_id} 
        map_d['ref_path'] = self.ref_path_info_workspace(ijson['company_id']) 
        ijson['d'] = ijson['doc_id']
        map_d.setdefault('page_coords', {})[ijson['d']] = self.read_page_cords(ijson)
        return [{'message':'done', 'coldef': cols, 'data': data, 'map': map_d}]

    def formula_info(self, ijson):
        import sqlite_api as sq1
        doc_id = ijson['doc_id']
        db_path = "/mnt/eMB_db/company_management/%s/doc_builder/%s.db"%(ijson['company_id'], doc_id)
        sq1_obj = sq1.sqlite_api(db_path)
        rid = ijson['row_id']
        fs_info = sq1_obj.read_formula_info(rid)
        op_rid = []
        op_dict = {}
        ref_ids = [str(rid)]
        for dd in fs_info:
            if str(dd[2]) not in ref_ids:
                ref_ids.append(str(dd[2]))
            if str(dd[4]) not in ref_ids:
                ref_ids.append(str(dd[4]))
            op_rid.append(str(dd[2]))
            op_dict.setdefault(dd[0], []).append(dd)
        des_info = sq1_obj.read_only_rows(','.join(op_rid))
        des_dict = {}
        for dd in des_info:
            des_dict[str(dd[1])] = str(dd[0])
        desc_info_row = sq1_obj.read_only_des(','.join(des_dict.values()))
        #print 'yyy',  desc_info_row
        row_des = {}
        for dd in desc_info_row:
            row_des[str(dd[0])] = dd[1]
        res_info = []
        #print 'tt', ref_ids
        refernece_info = sq1_obj.read_reference_table_rid(','.join(ref_ids))
        ref_dict = {}   
        for vg in refernece_info:
            ref_dict[vg[1]] = vg
        #print refernece_info
        data = []
        cols = [{'k':'desc', 'n':"Descrption"}, {"k": "val", "n":"Value"}, {'k':'op', 'n': 'Operator'}]
        map_d = {}
        fgroup = 1
        rid = 1
        for fid, finfo in op_dict.items():
            data.append({'desc': {'v': '%s_%s'%('Formula', fgroup)}, '$$treeLevel': 0, 'rid': rid, 'cid':rid})
            fgroup =  fgroup + 1
            rid = rid + 1
            for ff in finfo:
                row_id = des_dict[str(ff[2])]
                desc = row_des.get(row_id, '')
                #temp.append([desc, ff[3], ff[1]])
                data.append({'desc': {'v': desc}, 'op': {'v': ff[1]}, 'val': {'v': ff[3]}, 'rid': rid, 'cid': rid, '$$treeLevel': 1})
                map_key = '%s_%s'%(rid, 'desc')
                cbox = []
                if ff[2] in ref_dict:
                    nrow_id, rawdb_row_id, xml_id, bbox, page_no = ref_dict[ff[2]]
                    bbox = eval(bbox)
                    cbox.append({'x': xml_id, 'bbox': bbox[0], 'p': page_no, 'd': doc_id})
                map_d[map_key] = {'ref':{'values': cbox}}
                map_key = '%s_%s'%(rid, 'val')
                cbox1 = []
                if ff[4] in ref_dict:
                    nrow_id, rawdb_row_id, xml_id, bbox, page_no = ref_dict[ff[4]]
                    bbox = eval(bbox)
                    cbox1.append({'x': xml_id, 'bbox': bbox[0], 'p': page_no, 'd': doc_id})
                map_d[map_key] = {'ref':{'values': cbox1}}
                rid = rid + 1
        return [{'message':'done', 'data': data, 'col_def': cols, 'map': map_d}]


    def read_common_table_types(self, ijson):
        import sqlite_api as sq1
        import config
        pid  = ijson['project_id']
        db_path  = config.Config.gl_db 
        print db_path
        sq1_obj = sq1.sqlite_api(db_path)
        data = sq1_obj.read_all_tb()
        tb_types = {}
        for dd in data:
            tb_types[str(dd[0])] = dd[1]
        return tb_types

    def read_company_meta_data(self, ijson):
        #cur, conn = conn_obj.MySQLdb_conn(config.Config.company_info_db) 
        data = []
        cols = [{'k':'sno', 'n':"SNo", "type": "SL", "w":"50"}, {"k": "g", "n":"Sections", "v_opt":1}]
        map_d = {}
        rid = 1
        group_map   = {
                        'Company': ['Basic Information', 'Client Details', 'SEC Information'],
            }
        map_d   = {
                    'Basic Information' : 'basic_info',
                    'Client Details'    : 'client_info',
                    'SEC Information'    : 'sec_info',
                }
        for fg, groups in group_map.items():
            if not fg:continue
            data.append({'g': {'v': fg},'$$treeLevel': 0, 'rid': rid, 'cid':rid, 'sno': {'v': rid}, 'ref_k':[map_d.get(fg, fg), '']})
            rid = rid + 1
            for g in groups:
                if not g:continue
                data.append({'g': {'v': g}, 'rid': rid, 'cid': rid, '$$treeLevel': 1, 'sno': {'v': rid}, 'ref_k': [map_d.get(fg, fg), map_d.get(g, g)]})
                rid = rid + 1
        cols.insert(0, {'k':'check', 'n': 'check', "v_opt":3, 'pin': 'pinnedLeft'}) 
        return [{'message':'done', 'data': data, 'col_def': cols, 'map': map_d}]
        
        pass

    def read_sprimary_cols_data(self, ijson):
        #cur, conn = conn_obj.MySQLdb_conn(config.Config.company_info_db) 
        data = []
        cols = [{'k':'sno', 'n':"SNo", "type": "SL", "w":"50"}, {"k": "g", "n":"Sections", "v_opt":1}]
        map_d = {}
        rid = 1
        group_map   = {
                        'Super Primary Key': [],
            }
        for fg, groups in group_map.items():
            if not fg:continue
            data.append({'g': {'v': fg},'$$treeLevel': 0, 'rid': rid, 'cid':rid, 'sno': {'v': rid}, 'ref_k':[map_d.get(fg, fg), '']})
            rid = rid + 1
            for g in groups:
                if not g:continue
                data.append({'g': {'v': g}, 'rid': rid, 'cid': rid, '$$treeLevel': 1, 'sno': {'v': rid}, 'ref_k': [map_d.get(fg, fg), map_d.get(g, g)]})
                rid = rid + 1
        cols.insert(0, {'k':'check', 'n': 'check', "v_opt":3, 'pin': 'pinnedLeft'}) 
        return [{'message':'done', 'data': data, 'col_def': cols, 'map': map_d}]

    def read_grouped_primary_keys(self, ijson):
        company_id  = ijson['company_id']
        project_id  =ijson['project_id']
        db_path     = config.Config.databuilder_path.format(company_id, project_id)
        import sqlite_api as sq1
        sq1_obj = sq1.sqlite_api(db_path)
        res = sq1_obj.read_grouped_primary_keys()
        grp_map_d   = {}
        for r in res:
            primary_key, other_primary_key  = r #map(lambda x: ''.join(x.lower().split()), r)
            grp_map_d.setdefault(primary_key, {})[other_primary_key]    = 1
            if other_primary_key != primary_key:
                grp_map_d[('REV', other_primary_key)]   = primary_key
            
        return grp_map_d
        

    def read_all_primary_keys(self, ijson, ret_flg=None):
        company_id  = ijson['company_id']
        project_id  =ijson['project_id']
        db_path     = config.Config.databuilder_path.format(company_id, project_id)
        import sqlite_api as sq1
        sq1_obj = sq1.sqlite_api(db_path)
        res = sq1_obj.read_all_primary_keys()
        #sq1_obj.close()
        grp_map_d   = self.read_grouped_primary_keys(ijson)
        f   = {}
        tt_d    = {}
        for r in res:
            table_type, taxo_group_id   = r
            try:
                xxx = int(taxo_group_id)
                continue
            except:pass
            f.setdefault(taxo_group_id, {})[table_type] = 1
            if ('REV', taxo_group_id) in grp_map_d:
                taxo_group_id   = grp_map_d[('REV', taxo_group_id)]
            if taxo_group_id:
                tt_d.setdefault(table_type, {})[taxo_group_id]  = 1
            f.setdefault(taxo_group_id, {})[table_type] = 1
        morethan_one    = filter(lambda x:len(tt_d[x].keys())> 1, tt_d.keys())
        for k in morethan_one:
            tgrps    = tt_d[k].keys()
            tgrps.sort()
            f.setdefault('^'.join(tgrps), {})[table_type]   = 1
        data    = []
        grps    = f.keys()
        if ret_flg == 'Y':
            return f
        grps.sort(key=lambda x:(len(x.split('^')), len(f[x].keys())), reverse=True)
        rid = 1
        for grp in grps:
            if ijson.get('ALL') != 'Y' and ('REV', grp) in grp_map_d:continue
            row = {'sn':{'v':rid}, 'rid':rid, 'cid':rid, 'ref_k':[grp, ''], 'ttypes':f[grp].keys()}
            row['desc']  = {'v':grp} #, 'ref_k':[grp, '']}
            row['c']  = {'v':len(grp.split('^'))}
            rid += 1
            data.append(row)
        col_def = [{'k':'sn', 'type':'SL', 'n':'Sl No'}, {'k':'desc', 'n':'Group Name'}, {'k':'c', 'n':'Count'}]
        col_def.insert(0, {'k':'check', 'n': 'check', "v_opt":3, 'pin': 'pinnedLeft'}) 
        res = [{'message':'done', 'data':data, 'col_def':col_def, 'map':{}}]
        return res   
        
    def read_group_grid(self, ijson):
        import sqlite_api as sq1
        import config
        pid  = ijson['project_id']
        if ijson.get('tab') == 'Meta Data':
            return self.read_company_meta_data(ijson)
        elif ijson.get('tab') == 'Primary Key':
            return self.read_all_primary_keys(ijson)
        elif ijson.get('tab') == 'Super Primary Key':
            return self.read_sprimary_cols_data(ijson)
        db_path  = config.Config.databuilder_path.format(ijson['company_id'], pid)   
        sq1_obj = sq1.sqlite_api(db_path)
        data = sq1_obj.read_grid_info()
        tb_type = self.read_common_table_types(ijson)
        group_map = {}
        for dd in data:
            group_map.setdefault(dd[0], {})[dd[1]] = 1 
        data = []
        cols = [{'k':'sno', 'n':"SNo", "type": "SL", "w":"50"}, {"k": "g", "n":"Table Class", "v_opt":1}]
        map_d = {}
        rid = 1
        fgs  = filter(lambda x:x, group_map.keys())
        fgs.sort(key=lambda x:int(x))
        for fg in fgs:
            groups  = group_map[fg]
            data.append({'g': {'v': tb_type.get(fg, fg)},'$$treeLevel': 0, 'rid': rid, 'cid':rid, 'sno': {'v': rid}, 'ref_k':[fg, '']})
            rid = rid + 1
            for g in groups:
                if not g:continue
                data.append({'g': {'v': g}, 'rid': rid, 'cid': rid, '$$treeLevel': 1, 'sno': {'v': rid}, 'ref_k': [fg, g]})
                rid = rid + 1
        cols.insert(0, {'k':'check', 'n': 'check', "v_opt":3, 'pin': 'pinnedLeft'}) 
        return [{'message':'done', 'data': data, 'col_def': cols, 'map': map_d}]

    def read_db_muthu_info(self, ijson):
        import report_year_sort
        import modules.databuilder.taxo_builder as tx
        tx_obj = tx.TaxoBuilder()
        import modules.databuilder.form_builder_from_template as f_builder
        db_obj = f_builder.TaxoBuilder()
        import copy
        company_id = ijson['company_id']
        project_id = ijson['project_id']
        tab = ijson.get('tab', 'FE')
        if tab == 'Primary Key':
            if ijson.get('m_grps'):
                ijson['grpname']   = map(lambda x:x[0], ijson['m_grps'])
            else:
                ijson['grpname']   = [ijson['ref_k'][0]]
            import modules.databuilder.form_builder_from_template as taxo_builder
            obj = taxo_builder.TaxoBuilder()
            return obj.form_builder(ijson)
        elif tab == 'Meta Data':
            import update_company_config as pyf
            s_Obj = pyf.CompanyInfo()
            ijson['type']   = ijson['ref_k'][1]
            return s_Obj.get_company_data(ijson)
        elif tab == 'Super Primary Key':
            ijson['from_model'] = 'Y'
            res = db_obj.read_super_key_builder_data(ijson)
            return res
            
        elif tab == 'FE':
            ty, tg =  ijson['ref_k']
            if 1:#ijson['ref_k'][1] == '':
                ijson['table_type'] = ty
                ijson['from_model'] = 'Y'
                res = db_obj.read_builder_data(ijson)
                return res
            print len(tx_obj.read_db_data(ijson['company_id'], ijson['project_id'], [ty]))
            db_info, db_row_d, rev_db_d,  page_coords_dct, target_column, formula_d  = tx_obj.read_db_data(ijson['company_id'], ijson['project_id'], [ty]) 
            table_type_d = tx_obj.read_all_table_types(company_id)
            #print [db_info]
            table_path  = '/var/www/html/demo_data/V1/%s/%s/Table_info/'%(company_id, project_id)
            doc_mdata   = tx_obj.read_document_meta_data(ijson)
            ph_order    = map(lambda x:doc_mdata[x]['ph'], doc_mdata.keys())
            ph_order    = report_year_sort.year_sort(ph_order)
            all_docs    = map(lambda x:str(x), doc_mdata.keys())
            all_docs.sort(key=lambda x:ph_order.index(doc_mdata[str(x)]['ph']), reverse=True)
            res = tx_obj.create_final_db_output(db_info, company_id, project_id, table_type_d, page_coords_dct, table_path, target_column, formula_d, db_row_d, all_docs, 'N')
            if ijson['ref_k'][1] == '':
                ijson['ref_k'][1] = 'ALL'
            consider_key = tuple(ijson['ref_k'])
            grid_info = res[consider_key][0] 
        else:
            import modules.databuilder.taxo_builder as taxo_builder
            obj = taxo_builder.TaxoBuilder()
            ijson['ref_k'] = ijson['ref_k'].split('~')
            consider_key = tuple(ijson['ref_k'])
            res = obj.form_db_data(ijson)[0]
            print 'ttt', res
            grid_info = res #res[consider_key][0]
        map_dic = {}
        rid = 1
        for row in grid_info['data']:
            row['rid'] = rid
            row['cid'] = rid
            #if tab == 'FE':
            #    row['ID'] = {'v': row.get('ID', '')}
            for col in grid_info['phs']:
                if col['k'] not in row:continue
                map_key = "%s_%s"%(rid, col['k'])
                print row #[col['k']]
                map_dic[map_key] = copy.deepcopy(row[col['k']])
                map_dic[map_key]['p'] = map_dic[map_key].get('pno', '')
                row['cid'] = row['sn']
                row['rid'] = row['sn']
                row[col['k']] = {'v': row[col['k']]['v'], 'rid':row[col['k']].get('rid', '')}  
            rid = rid  + 1
        if len(grid_info['phs']) > 1:
            grid_info['phs'][0]['pin'] = 'pinnedLeft'
            grid_info['phs'][0]['w'] = '200'
        #for ph in grid_info['phs']:
        #    ph['v_opt'] = 3
        #if tab == 'FE':
        #    grid_info['phs'].insert(0, {'k':'ID', 'n': 'ID', 'pin': 'pinnedLeft'})
        grid_info['phs'].insert(0, {'k':'check', 'n': 'check', "v_opt":3, 'pin': 'pinnedLeft'})
        map_dic['ref_path'] = self.ref_path_info_workspace(ijson['company_id']) 
        return [{'message': 'done', 'data': grid_info['data'], 'col_def': grid_info['phs'], 'page_cords': grid_info.get('bbox', {}), 'map': map_dic, 'group': grid_info.get('grp_flg', 'N')}]

    def read_row_info(self, ijson):
        company_id = ijson['company_id']
        project_id = ijson['project_id']
        rid = ijson['rid']
        db_path = config.Config.databuilder_path.format(company_id, project_id)
        import sqlite_api as sq1
        sq1_obj = sq1.sqlite_api(db_path)
        data = sq1_obj.read_db_builder_v2(rid)
        ref_info = sq1_obj.read_reference_table_rid_v1(rid)
        return [data, ref_info]
        
        
    
if __name__ == '__main__':
    obj = exe()
    ijson = {"cmd_id":71,"company_id":1604,"doc_id":1,"project_id":5,"template_id":1,"user":"madan", 'd':4, 'p':11, 'g':1, "rid": 422015}
    res = obj.read_row_info(ijson)
    print json.dumps(res)
