import Search_module.Autocomplete_new.redis_config as redis_search
import re
import json
class exe:
    def __init__(self):
        self.redsis_info = "172.16.20.7##6384##0"
        self.escape1 = re.compile(r'&#\d+;')
        self.escape2 = re.compile(r',|\.|<|>|{|}|[|]|"|\'|:|;|!|@|#|\$|%|\^|&|\*|\(|\)|-|\+|=|~')
        self.escape3 = re.compile(r'\s+')

    def escape_special_charatcers(self, search_str, v = ''):
        search_str = re.sub(self.escape1, '', search_str)
        search_str = re.sub(self.escape2, '', search_str)
        search_str = re.sub(self.escape3, ' ', search_str)
        search_str = search_str.strip()   
        if not v:
            return search_str #+"|"+search_str+"*"
        else:
            return search_str #+"|"+search_str+"*"

    def get_common_terms(self, res_dic):
        if not res_dic:
            return []
        return reduce(set.intersection, [set(l_) for l_ in res_dic.values()])
 
    def update_bbox(self, hl, ht, hw, hh, bboxs):
        for bbox in bboxs.split('$$'):
            if not bbox:continue
            x0, y0, x1, y1 =  map(lambda x: int(x), bbox.split('_'))
            new_height = (y1 - y0)
            hh = hh+new_height 

        return hh

    def consolidated_bbox(self, bbox):
        bboxs = filter(lambda x:x, bbox.split('$$'))
        bboxs =  map(lambda x: map(lambda y:int(y), x.split('_')), bboxs)
        new_bbox = []
        if len(bboxs) == 1:
            new_bbox = [bboxs[0][0], bboxs[0][1], bboxs[0][2], bboxs[0][3]]
        else:
            nl, nt, nw, nh = 0, 0 , 0, 0
            for bbox in bboxs:
                if nl == 0 or nl > bbox[0]:
                    nl = bbox[0]
                if nt ==0 or nt > bbox[1]:
                    nt = bbox[1]
                if nw == 0 or nw < bbox[2]:
                    nw = bbox[2]
                if nh == 0 or nh < bbox[3]:
                    nh = bbox[3]
            new_bbox = [nl, nt, nw , nh]
            #print 'neww', new_bbox
        return [new_bbox[0], new_bbox[1], (new_bbox[2] - new_bbox[0]), (new_bbox[3] - new_bbox[1])]
            
    def row_wise_top_height(self, bbox, res, row):
        bboxs = filter(lambda x:x, bbox.split('$$'))
        bboxs =  map(lambda x: map(lambda y:int(y), x.split('_')), bboxs)
        for bbox in bboxs:
            if res.get(row):
                break
            res[row] = [bbox[1], (bbox[3]- bbox[1]), bbox]
        
            

    def get_cords_info(self, rc_info, cell_info, gridinfo):
        doc, page, grid = gridinfo
        tb_area = rc_info['bbox'][int(page)]
        cell_bbox = {}
        row_wise_info = {}
        row_wise_cord = {}
        for cell in cell_info:
            r, c = cell[0].split('_')
            row_wise_info.setdefault(r, {})[cell[0]] = cell
            cell_bbox[cell[0]] = cell[1]
        temp  = []
        print 'tb_area', tb_area
        done_rheight = {}
        hl, ht, hw, hh = tb_area[0], tb_area[1], tb_area[2],0 
        for row, rinfo in rc_info['rc_d'].items():
            for cell , cinfo in rinfo.items():
                new_rc = '%s_%s'%(row, cell)
                stype = cinfo.get('ldr')
                bbox = cinfo.get('bbox')    
                if not bbox:continue
                self.row_wise_top_height(bbox, row_wise_cord, row)
                if stype.lower() in ['vch', 'gh']:
                    if not done_rheight.get(row):
                        done_rheight[row] = 1
                        hh = self.update_bbox(hl, ht, hw, hh, bbox)
        print 'row_wise_cord', row_wise_cord
        print 'header_bbox', [hl, ht, hw, hh]
        #print row_wise_cord
        vtemp = {'slices':["S1"], "d": doc, "p": page, "g": grid, "S1": {"bbox":[hl, ht, hw, hh], "cells":[]}}
        for row, cellinfo in row_wise_info.items():
            print 'crow', row
            sl_len = len(vtemp['slices'])+1
            skey = 'S%s'%(sl_len)
            cells = {}
            for rc, rcinfo in cellinfo.items():
                r,c  = rc.split('_')
                rcord_info = row_wise_cord[int(r)]
                cbbox = self.consolidated_bbox(rcinfo[1])
                print 'row bbox', [tb_area[0], rcord_info[0], tb_area[2], rcord_info[1]]
                if not cells.get('bbox'):
                    print 'cell bbox', [cbbox]
                    cells = {'bbox':[tb_area[0], rcord_info[0], tb_area[2], rcord_info[1]], 'cells':{}}
                
                cells['cells'][rc] = {'ref_k':[doc, page, grid, '', rcinfo[0]], 'bbox': cbbox, 'cls':rcinfo[3], 'cls-d':'hch-d'}
                #cells['cells'].append({'ref_k':[doc, page, grid, '', rcinfo[0]], 'bbox': cbbox, 'cls':rcinfo[3]})
            #print 'cfff', cells, 
            if cells.get('bbox'):
                vtemp['slices'].append(skey)
                vtemp[skey] = cells
        return vtemp

    def get_page_cords(self, doc_info, company_id, page_dic):
        doc, page, grid = doc_info
        if page_dic.get(doc, ''):
            return
        db_path = '/mnt/eMB_db/company_management/{0}/equality/{1}.db'.format(company_id, doc)
        import sqlite_api as sqlapi
        sqlapi_obj = sqlapi.sqlite_api(db_path)
        page_info  = sqlapi_obj.read_page_cords()
        sqlapi_obj.cur.close()
        for dd in page_info:
            page_dic.setdefault(doc, {})[str(dd[0])] = eval(dd[1])

    def get_only_grids(self, sres):
        res = []
        for grid, elm in sres.items():
            doc, page , grid = grid.split('_')
            res.append({'k': grid, 'n': '%s_%s'%(page, grid)})
        return [{'message':'done', 'data': res}]
        

    def start_grid_wise(self, sres, ijson, cmp_n):
        cm = ijson['cmp_id']
        import modules.tablets.tablets as tb_api
        tb_obj  = tb_api.Tablets()
        page_cords_dic = {}
        fs_info = []
        for grid, cellinfo in sres.items():
            #if '5131_101_2' not  in grid:continue
            self.get_page_cords(grid.split('_'), cm, page_cords_dic)
            grid_info = tb_obj.create_inc_table_data(cm, grid, '')
            if not grid_info:continue
            doc, page, bgrid = grid.split('_')
            cc = self.get_cords_info(grid_info, cellinfo, grid.split('_'))
            if len(cc['slices']):
                #fs_info.append(cc)
                cpage_cord = {}
                cpage_cord.setdefault(doc, {})[page] = page_cords_dic.get(doc, {}).get(page, [0,0,0,0])
                fs_info.append({"ref_path": {"ref_image": "/var_html_path/WorkSpaceBuilder_DB/34/1/pdata/docs/{0}/html_pages/{0}-page-{1}.png","ref_html": "/var_html_path/WorkSpaceBuilder_DB/34/1/pdata/docs/{0}/html_output/{1}.html","ref_pdf": "/var_html_path/WorkSpaceBuilder_DB/34/1/pdata/docs/{0}/pages/{1}.pdf", "ref_svg": "/var_html_path/WorkSpaceBuilder_DB/34/1/pdata/docs/{0}/html_pages/{0}-page-{1}.svg"},"cards": cc, "page_cord": cpage_cord})
        return [{'message':'done', 'data': fs_info}]

    def search_elm(self, ijson):
	search_result = {}
        querys = []
        mquery = []
        docs = ijson['data']
        #company = ijson['company']
        data           = ijson['data']
        #project = ijson['Project']
        m_data         = ijson.get('m_dict', {})
        dest            = ''
        company          = ijson.get('company','')
        table_type =  ijson.get('tablename','')
        texts       = ijson.get('search_dict',{})
        flag_v  = ijson.get('all_flg', 'N')
        cmp_id = ijson['cmp_id']
        for qk, qv in texts.items(): 
            vv = '|'.join(map(lambda x: self.escape_special_charatcers(x,1),qv))
            cque = "@DATA:"+vv+" @SECTION_TYPE:"+qk
            if qk in m_data:
                mquery.append(cque)
            else:
                querys.append(cque)
        order_result  = {}
        for ddoc in docs:
            ddoc_key     = "%s_%s"%(cmp_id, ddoc)# project+"_GRID_"+str(ddoc)
            si,sp,sdb = self.redsis_info.split('##')
            dredis_obj = redis_search.TAS_AutoCompleter(si,sp,sdb, ddoc_key)
            if not mquery: 
                for query in querys:
                    try:
                        get_alltext  = dredis_obj.search_query_convert_docs_wise_v2(query,search_result, ddoc)
                    except Exception as e:
                        pass
            else:
                mt_res_flg = False
                search_result_m = {}
                query_wise_res  = {}
                for query in mquery:
                    try:
                        get_alltext  = dredis_obj.search_query_convert_docs_wise_v2_mquery(query,search_result_m, ddoc, query_wise_res)
                    except Exception as e:
                        pass
                union_m_res = self.get_common_terms(query_wise_res)
                for doc_info in union_m_res:
                    mt_res_flg = True
                    search_result[doc_info] = search_result_m[doc_info]
                if mt_res_flg:
                    for query in querys:
                        try:
                            get_alltext  = dredis_obj.search_query_convert_docs_wise_v2_order(query, order_result, ddoc)
                        except Exception as e:
                            pass
                                    
        order_keys  = sorted(search_result.keys(), key=lambda x:order_result.get(x, 9999))
        if flag_v == 'Y':
            fs = []
            doc_id_index = {}
            for kk in order_keys:
                doc_id , page, grid = kk.split('_')
                hh = "%s_%s"%(page, grid)
                if doc_id not in doc_id_index:
                    keys_len = len(doc_id_index.keys())
                    doc_id_index[doc_id] = keys_len
                    fs.append([doc_id, [[hh, 1]]]) 
                else:
                    fs[doc_id_index[doc_id]][1].append([hh, 1])
            #return fs, {}
            return self.start_grid_wise(fs, ijson, company) 
        return self.start_grid_wise(search_result, ijson, company) 
        return search_result, order_keys

if  __name__ == '__main__':
    obj =  exe()
    #ijson = {"cmd_id":67,"company":"OFGBancorp","data":[5131],"search_dict":{"hch":["interest income"]},"m_dict":{"hch":"Y"},"user":"demo","cmp_id":1117}
    ijson = {"cmd_id":67,"company":"OFG Bancorp","data":[5131],"search_dict":{"hch":["income"]},"m_dict":{"value":"Y"},"user":"demo","cmp_id":1117, "grid":"Y"} 
    #{"c_flag":323,"company":"OFGBancorp","db_string":"AECN_INC","data":[5131,4862,4863,4864,4866,4868,4869,4871,4872,4875,4876,4877,4878,4879,4880,4881,4882,4883,4884,4887,4889,4892,4893,4894,4896,4897,4899,4901,4902,4903,4906,4907,4908,4909,4910,4911,4912,4913,4914,4915,4916,5129,5130,5132,5133,5134,5135,5136,5163,5380],"search_dict":{"value":["143"]},"m_dict":{"value":"Y"},"user":"harsha", "cmp_id":"1117"}
    res = obj.search_elm(ijson)
    print json.dumps(res)

    
