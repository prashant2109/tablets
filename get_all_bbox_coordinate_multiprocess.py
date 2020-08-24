#!/usr/bin/python
# -*- coding:utf-8 -*-
import multiprocessing
from contextlib import contextmanager
import os, sys, json
import common_func as common_func
import common.baseobj as bobj
import common.datastore as datastore
import common.GlobalData as GlobalData
import common.filePathAdj as fileabspath
import common.getconfig as getconfig
idbkey = 'QngxaW1pcGZGeWwxNUFBNmshqVLICs12'
idbkeysize = len(idbkey)
# Global data object
gobj = GlobalData.GlobalData()
if idbkey is not None: gobj.add('dbkey', idbkey)
if idbkeysize is not None: gobj.add('dbkeysize', idbkeysize)


def disableprint():
    sys.stdout = open(os.devnull, 'w')

def enableprint():
    sys.stdout = sys.__stdout__
def single_page_bbox(doc_id, pageno, i_ipath):
    #print doc_id, pageno
    page_cord = ''
    if not os.path.exists(os.path.join(i_ipath, doc_id, 'CID', '%s.sh'%pageno)): 
        path    = os.path.join(i_ipath, doc_id, 'html_pages', '%s-page-%s.json'%(doc_id, pageno))
        
        if os.path.exists(path):
            cord_data   = json.loads(open(path, 'r').read())
            page_xywh   = map(lambda x:int(x), cord_data['page_xywh'].split('_'))
            return (pageno, page_xywh)
        path    = os.path.join(i_ipath, doc_id, 'html', '%s-page-%s.json'%(doc_id, pageno))
        
        if os.path.exists(path):
            cord_data   = json.loads(open(path, 'r').read())
            page_xywh   = map(lambda x:int(x), cord_data['page_xywh'].split('_'))
            return (pageno, page_xywh)
        return (pageno, [0, 0, 0, 0])
    try:
        cell_dict = common_func.get_cell_info_dict(doc_id, pageno)
    except:
        cell_dict = {}
    for key, val in cell_dict.items():
        page_cord = val.get('page_rect', '')
        if page_cord: return (pageno, map(float, page_cord.split('_')))
    return (pageno, [0, 0, 0, 0])
    
def get_page_bbox(doc_id, i_ipath):
    #disableprint()
    common_func.ipath = i_ipath
    common_func.opath = i_ipath
    common_func.isdb = 1
    common_func.isenc = 1
    page_dict = {}
    #print i_ipath,doc_id
    t_p_no = open(os.path.join(i_ipath, doc_id, 'pdf_total_pages')).read()
    #print "os.path.join(i_ipath, doc_id, 'pdf_total_pages')", os.path.join(i_ipath, doc_id, 'pdf_total_pages')
    inp_info = [(doc_id, pageno, i_ipath) for pageno in range(1, int(t_p_no)+1)]
    with poolcontext(processes=5) as pool:
        page_dict = dict(pool.map(merge_names_unpack, inp_info))
    
    #print page_dict
    #enableprint()
    return page_dict

def merge_names_unpack(args):
    return single_page_bbox(*args)

@contextmanager
def poolcontext(*args, **kwargs):
    pool = multiprocessing.Pool(*args, **kwargs)
    yield pool
    pool.terminate()

if __name__ == '__main__':
    docid = '11'
    #ipath = '/var/www/html/WorkSpaceBuilder_DB/34/1/pdata/docs/'
    d = {"c_flag":15649,"doc_id":"1940","pageno":"11","groupid":4,"db_string":"AECN_INC","ProjectID":"1053735","workspace_id":"1"}
    path = "/var/www/html/WorkSpaceBuilder_DB/"+str(d['ProjectID'])+"/"+str(d['workspace_id'])+"/pdata/docs/"
    enableprint()
    print get_page_bbox(docid, path)
