import json, sys, os
from pyapi import PYAPI
import db.get_conn as get_conn
conn_obj    = get_conn.DB()


def disableprint():
    sys.stdout = open(os.devnull, 'w')
    pass

def enableprint():
    sys.stdout = sys.__stdout__

class WebAPI(PYAPI):
    def __init__(self):
        PYAPI.__init__(self)
    def process(self, cmd_id, ijson):
        res     = []
        if 1 == cmd_id:
            res = self.validate_login(ijson)
        elif 2 == cmd_id:
            res = self.get_company_info_cid(ijson)
        elif 3 == cmd_id:
            res = self.read_docs(ijson)
        elif 4 == cmd_id:
            import tree_view_data_builder as pyf 
            t_Obj = pyf.GridModel() 
            res = t_Obj.read_rawdb_tree(ijson)
        elif 5 == cmd_id:
            import tree_view_data_builder as pyf 
            t_Obj = pyf.GridModel() 
            res = t_Obj.cell_id_reference(ijson)

        elif cmd_id == 6: #insert Model
            import model_api as ma 
            ma_obj = ma.model_api()
            res = ma_obj.insert_model(ijson)

        elif cmd_id == 7: #read template 
            import model_api as ma 
            ma_obj = ma.model_api()
            res = ma_obj.read_templates(ijson)

        elif cmd_id == 8: #read sheets 
            import model_api as ma 
            ma_obj = ma.model_api()
            res = ma_obj.read_sheets(ijson)
             
        elif cmd_id == 9: #read sheet data 
            import model_api as ma 
            ma_obj = ma.model_api()
            res = ma_obj.sheet_gridinfo(ijson)

        elif 10 == cmd_id:
            import tree_view_data_builder as pyf 
            t_Obj = pyf.GridModel() 
            res = t_Obj.read_formula_cell_id(ijson)

        elif 11 == cmd_id:
            import child_parent as pyf 
            t_Obj = pyf.GridModel() 
            res = t_Obj.search_operation_data_flgs(ijson)

        elif 12 == cmd_id:
            import databuilder_info as pyf 
            db_Obj = pyf.INC_DataBuilder() 
            res = db_Obj.read_distinct_table_types(ijson)

        elif 13 == cmd_id:
            import databuilder_info as pyf 
            db_Obj = pyf.INC_DataBuilder() 
            res = db_Obj.save_classification_info(ijson)

        elif 14 == cmd_id:
            import databuilder_info as pyf 
            db_Obj = pyf.INC_DataBuilder() 
            res = db_Obj.multi_table_module_info(ijson)

        elif 15 == cmd_id:
            import databuilder_info as pyf 
            db_Obj = pyf.INC_DataBuilder() 
            res = db_Obj.insert_table_type_global(ijson)

        elif 16 == cmd_id:
            import databuilder_info as pyf 
            db_obj = pyf.INC_DataBuilder() 
            res = db_obj.doc_wise_well_connected_tables_ib(ijson)

        elif 17 == cmd_id:
            import databuilder_info as pyf 
            db_obj = pyf.INC_DataBuilder() 
            res = db_obj.data_according_to_table_id(ijson)

        elif 18 == cmd_id:
            import databuilder_info as pyf 
            db_obj = pyf.INC_DataBuilder() 
            res = db_obj.table_id_poss_with_across_docs(ijson)

        elif 19 == cmd_id:
            import databuilder_info as pyf 
            db_obj = pyf.INC_DataBuilder() 
            res = db_obj.read_well_collected_table_info(ijson)

        elif 20 == cmd_id:
            import databuilder_info as pyf 
            db_obj = pyf.INC_DataBuilder() 
            res = db_obj.read_realtion_between_tables(ijson)

        elif 21 == cmd_id:
            res = self.read_companies_from_company_mgmt()

        elif 22 == cmd_id:
            res = self.read_data_builder_companies()

        elif 23 == cmd_id:
            res = self.read_all_table_types(ijson)
        
        elif 24 == cmd_id:
            res = self.read_all_industry(ijson)

        elif 25 == cmd_id:
            import spreadheet_builder as pyf
            s_Obj = pyf.SpreadSheet()
            res = s_Obj.read_project_details(ijson)

        elif 26 == cmd_id:
            import spreadheet_builder as pyf
            s_Obj = pyf.SpreadSheet()
            res = s_Obj.read_taxo_data(ijson)

        elif 27 == cmd_id:
            import spreadheet_builder as pyf
            s_Obj = pyf.SpreadSheet()
            res = s_Obj.read_data_builder_data(ijson)

        elif 28 == cmd_id:
            import spreadheet_builder as pyf
            s_Obj = pyf.SpreadSheet()
            res = s_Obj.read_reference_info(ijson)

        elif 29 == cmd_id:
            #import spreadheet_builder as pyf
            #s_Obj = pyf.SpreadSheet()
            res = self.read_kpi_data_cgi(ijson)

        elif 30 == cmd_id:
            import spreadheet_builder as pyf
            s_Obj = pyf.SpreadSheet()
            res = s_Obj.read_raw_DB(ijson)

        elif 31 == cmd_id:
            import spreadheet_builder as pyf
            s_Obj = pyf.SpreadSheet()
            res = s_Obj.data_builder_reference(ijson)

        elif 32 == cmd_id:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.read_company_list()

        elif 33 == cmd_id:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.read_distinct_table_types_data(ijson)

        elif 34 == cmd_id:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.read_document_meta_data(ijson)

        elif 35 == cmd_id:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.read_grid_information(ijson)

        elif 36 == cmd_id:
            import modules.tablets.tablets as tablets
            s_Obj = tablets.Tablets()
            res = s_Obj.get_tablet_cell_ref(ijson)

        elif 37 == cmd_id:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.save_hgh_scoping(ijson)

        elif cmd_id == 38:
            if ijson.get('user', '') == 'prashant':
                import modules.tablets.chain_tablet_v2 as v2
            else:
                import modules.tablets.chain_tablet_v3 as v2
            v2_obj = v2.chain()
            res = v2_obj.get_equlity_rows(ijson)

        elif cmd_id == 39:
            import modules.tablets.scoping_info as v2
            v2_obj = v2.classify()
            res = v2_obj.get_classify_info(ijson)

        elif 40 == cmd_id:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.read_all_tt(ijson)

        elif 41 == cmd_id:
            import company_mgmt_data as pyf
            s_obj = pyf.INC_Company_Mgmt()
            res = s_obj.save_gv_scoping(ijson)

        elif 42 == cmd_id:
            import company_mgmt_data as pyf
            s_obj = pyf.INC_Company_Mgmt()
            res = s_obj.add_table_types(ijson)

        elif 43 == cmd_id:
            import company_mgmt_data as pyf
            s_obj = pyf.INC_Company_Mgmt()
            #res = s_obj.read_scoped_gvs(ijson)
            res = s_obj.read_scoped_gv_information(ijson)

        elif 4003 == cmd_id:
            import t_company_mgmt_data as pyf
            s_obj = pyf.INC_Company_Mgmt()
            #res = s_obj.read_scoped_gvs(ijson)
            res = s_obj.read_scoped_gv_information(ijson)

        elif cmd_id == 44:
            import modules.tablets.chain_tablet_v5 as v2
            v2_obj = v2.chain()
            res = v2_obj.get_equlity_rows(ijson)

        elif cmd_id == 45:
            import modules.databuilder.form_builder as v2
            v2_obj = v2.DataBuilder()
            res = v2_obj.form_triplet_data(ijson)

        elif 46 == cmd_id:
            import company_mgmt_data as pyf
            s_obj = pyf.INC_Company_Mgmt()
            res = s_obj.read_scoped_gv_information(ijson)

        elif 47 == cmd_id:
            res = self.get_search_result(ijson)

        elif 48 == cmd_id:
            import company_mgmt_data as pyf
            s_obj = pyf.INC_Company_Mgmt()
            res = s_obj.all_scoped_table_types(ijson)

        elif 49 == cmd_id:
            res = self.form_auto_db(ijson)

        elif 50 == cmd_id:
            import company_mgmt_data as pyf
            s_obj = pyf.INC_Company_Mgmt()
            res = s_obj.across_doc_DB(ijson)

        elif 1000 == cmd_id:
            import search_info as pyf
            s_obj = pyf.Search_Information()
            res = s_obj.create_search_infos_table_lets(ijson)

        elif 51 == cmd_id:
            import display_search_info as pyf
            s_obj = pyf.Searched_Data()
            res = s_obj.basic_search(ijson)
        
        elif 510 == cmd_id:
            import test_company_mgmt_data as pyf
            s_obj = pyf.INC_Company_Mgmt()
            res = s_obj.test_read_grid_information(ijson)

        elif 52 == cmd_id:
            res = self.hardcoded_coldef(ijson)

        elif 53 == cmd_id:
            res = self.get_navigation_hops(ijson)
        
        elif 54 == cmd_id:
            #res = [{"message": "done", "data": [{"ref_k": ["5131_137_1", "", "3_1"], "k": 1, "data": [["ROOT", "5131_137_1", "3_1"], ["624", "10_1", ""]], "n": 1}, {"ref_k": ["5131_137_1", "", "3_1"], "k": 2, "data": [["ROOT", "5131_137_1", "3_1"], ["623", "10_1", ""]], "n": 2}, {"ref_k": ["5131_137_1", "", "3_1"], "k": 3, "data": [["ROOT", "5131_137_1", "3_1"], ["624", "10_1", ""]], "n": 3}, {"ref_k": ["5131_137_1", "", "3_1"], "k": 4, "data": [["ROOT", "5131_137_1", "3_1"], ["623", "10_1", ""]], "n": 4}, {"ref_k": ["5131_137_1", "", "3_1"], "k": 5, "data": [["ROOT", "5131_137_1", "3_1"], ["624", "10_1", ""]], "n": 5}, {"ref_k": ["5131_137_1", "", "3_1"], "k": 6, "data": [["ROOT", "5131_137_1", "3_1"], ["624", "10_1", ""]], "n": 6}, {"ref_k": ["5131_137_1", "", "3_1"], "k": 7, "data": [["ROOT", "5131_137_1", "3_1"], ["623", "10_1", ""]], "n": 7}, {"ref_k": ["5131_137_1", "", "3_1"], "k": 8, "data": [["ROOT", "5131_137_1", "3_1"], ["623", "10_1", ""]], "n": 8}, {"ref_k": ["5131_137_1", "", "3_1"], "k": 9, "data": [["ROOT", "5131_137_1", "3_1"], ["624", "10_1", ""]], "n": 9}, {"ref_k": ["5131_137_1", "", "3_1"], "k": 10, "data": [["ROOT", "5131_137_1", "3_1"], ["623", "10_1", ""]], "n": 10}, {"ref_k": ["5131_137_1", "", "3_1"], "k": 11, "data": [["ROOT", "5131_137_1", "3_1"], ["624", "10_1", ""]], "n": 11}, {"ref_k": ["5131_137_1", "", "3_1"], "k": 12, "data": [["ROOT", "5131_137_1", "3_1"], ["623", "10_1", ""]], "n": 12}, {"ref_k": ["5131_137_1", "", "3_1"], "k": 13, "data": [["ROOT", "5131_137_1", "3_1"], ["624", "10_1", ""]], "n": 13}, {"ref_k": ["5131_137_1", "", "3_1"], "k": 14, "data": [["ROOT", "5131_137_1", "3_1"], ["624", "10_1", ""]], "n": 14}, {"ref_k": ["5131_137_1", "", "3_1"], "k": 15, "data": [["ROOT", "5131_137_1", "3_1"], ["623", "10_1", ""]], "n": 15}, {"ref_k": ["5131_137_1", "", "3_1"], "k": 16, "data": [["ROOT", "5131_137_1", "3_1"], ["624", "10_1", ""]], "n": 16}, {"ref_k": ["5131_137_1", "", "3_1"], "k": 17, "data": [["ROOT", "5131_137_1", "3_1"], ["623", "10_1", ""]], "n": 17}, {"ref_k": ["5131_137_1", "", "3_1"], "k": 18, "data": [["ROOT", "5131_137_1", "3_1"], ["624", "10_1", ""]], "n": 18}, {"ref_k": ["5131_137_1", "", "3_1"], "k": 19, "data": [["ROOT", "5131_137_1", "3_1"], ["624", "10_1", ""]], "n": 19}, {"ref_k": ["5131_137_1", "", "3_1"], "k": 20, "data": [["ROOT", "5131_137_1", "3_1"], ["623", "10_1", ""]], "n": 20}, {"ref_k": ["5131_137_1", "", "3_1"], "k": 21, "data": [["ROOT", "5131_137_1", "3_1"], ["623", "10_1", ""]], "n": 21}, {"ref_k": ["5131_137_1", "", "3_1"], "k": 22, "data": [["ROOT", "5131_137_1", "3_1"], ["624", "10_1", ""]], "n": 22}, {"ref_k": ["5131_137_1", "", "3_1"], "k": 23, "data": [["ROOT", "5131_137_1", "3_1"], ["623", "10_1", ""]], "n": 23}, {"ref_k": ["5131_137_1", "", "3_1"], "k": 24, "data": [["ROOT", "5131_137_1", "3_1"], ["624", "10_1", ""]], "n": 24}, {"ref_k": ["5131_137_1", "", "3_1"], "k": 25, "data": [["ROOT", "5131_137_1", "3_1"], ["623", "10_1", ""]], "n": 25}, {"ref_k": ["5131_137_1", "", "3_1"], "k": 26, "data": [["ROOT", "5131_137_1", "3_1"], ["624", "10_1", ""]], "n": 26}]}]
            import warnings 
            warnings.filterwarnings("ignore")
            import search_info as pyf
            s_obj = pyf.Search_Information()
            res = s_obj.create_search_final_results_between(ijson)

        elif 55 == cmd_id:
            import company_mgmt_data as pyf
            s_obj = pyf.INC_Company_Mgmt()
            res = s_obj.delete_scoped_gv(ijson)

        elif 56 == cmd_id:
            import company_mgmt_data as pyf
            s_obj = pyf.INC_Company_Mgmt()
            res = s_obj.read_tt_wise_docs(ijson)

        elif 57 == cmd_id:
            import company_mgmt_data as pyf
            s_obj = pyf.INC_Company_Mgmt()
            res = s_obj.read_most_ref_grids(ijson)

        elif 58 == cmd_id:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.read_grid_information(ijson)

        elif 59 == cmd_id:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.read_project_info(ijson)

        elif 60 == cmd_id:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.get_traversed_details(ijson)

        elif 61 == cmd_id:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.scoped_predictor_info(ijson)
        elif 62 == cmd_id:
            res = self.read_templates(ijson)
        elif 63 == cmd_id:
            res = self.read_template_sheets(ijson)
        elif 64 == cmd_id:
            res = self.read_template_sheet_data(ijson)
        elif 65 == cmd_id:
            res = self.insert_new_template(ijson)
        elif 66 == cmd_id:
            import company_mgmt_data as pyf
            s_obj = pyf.INC_Company_Mgmt()
            res = s_obj.across_doc_DB(ijson)
        elif cmd_id == 67:
            import n_exc_api as ex
            ex_obj = ex.exe()
            res = ex_obj.search_elm(ijson) 
        elif cmd_id == 68:
            res = self.read_all_grids(ijson)
        elif cmd_id == 69:
            res = self.insert_taxo_row(ijson)
        elif 70 == cmd_id:
            if '_' in ijson['table_id']:
                ijson['table_id']   = '#'.join(ijson['table_id'].split('_'))
            import test_company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            #res = s_Obj.read_grid_information_spread_sheet(ijson)
            res = s_Obj.spread_sheet_table_data(ijson)
        elif 71 == cmd_id:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.read_all_grids_doc_wise(ijson)

        elif cmd_id == 72:
            res = self.delete_map_ids(ijson)
        elif cmd_id == 73:
            res = self.get_lookup_value(ijson)

        elif 74 == cmd_id:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.read_rawdb_tree(ijson)
        elif cmd_id == 101:
            import map_api
            ma_obj = map_api.exe()
            res = ma_obj.get_grid_info(ijson)
        elif cmd_id == 102:
            import map_api
            ma_obj = map_api.exe()
            res = ma_obj.read_grid_info_g(ijson)
        elif cmd_id == 103:
            import map_api
            ma_obj = map_api.exe()
            res = ma_obj.read_grid_info(ijson)
        elif cmd_id == 104:
            res = self.read_table_lets(ijson)
        elif cmd_id == 105:
            import map_api
            ma_obj = map_api.exe()
            res = ma_obj.get_eq_rows(ijson)
        elif cmd_id == 106:
            import map_api
            ma_obj = map_api.exe()
            res = ma_obj.read_table_ids(ijson)
        elif cmd_id == 107:
            import map_api
            ma_obj = map_api.exe()
            res = ma_obj.read_group_ids(ijson)
        elif cmd_id == 108:
            res = self.read_scope_taxo_info(ijson)
        elif cmd_id == 109:
            import map_api
            ma_obj = map_api.exe()
            res = ma_obj.read_databuilder_info(ijson)
        elif cmd_id == 110:
            import map_api
            ma_obj = map_api.exe()
            res = ma_obj.formula_info(ijson)
        elif cmd_id == 111:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            #res = s_Obj.get_count_tables(ijson)
            res = s_Obj.table_stats(ijson)
        elif cmd_id == 112:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.read_all_grids_doc_wise_no_grid_header(ijson)
        elif cmd_id == 113:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.read_data_builder_info(ijson)
        elif cmd_id == 114:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.read_db_ref_info(ijson)
        elif cmd_id == 115:
            res = self.con_insert_taxo_row(ijson)
        elif cmd_id == 116:
            import map_api
            ma_obj = map_api.exe()
            res = ma_obj.read_group_grid(ijson)
        elif 117 == cmd_id:
            res = self.read_template_grids(ijson)
            #res = self.read_template_taxo(ijson)
        elif cmd_id == 118:
            import map_api
            ma_obj = map_api.exe()
            res = ma_obj.read_db_muthu_info(ijson)
        elif cmd_id == 119:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.data_builder_stats(ijson)
        elif cmd_id == 120:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            #res = s_Obj.get_doc_table_stats(ijson)
            res = s_Obj.grids_get_doc_table_stats(ijson)
        elif cmd_id == 121:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.delete_scoped(ijson)
        elif cmd_id == 122:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.read_rem_rc_table(ijson)
        elif cmd_id == 123:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.read_global_table_type_info(ijson)
        elif cmd_id == 124:
            import template_configuration as pyf
            tc_Obj = pyf.TemplateConfig()
            res = tc_Obj.insert_output_config(ijson)
        elif cmd_id == 125:
            import template_configuration as pyf
            tc_Obj = pyf.TemplateConfig()
            res = tc_Obj.read_template_info(ijson)
        elif cmd_id == 126:
            import create_excel as pyf
            tc_Obj = pyf.excel()
            res = tc_Obj.cl_create(ijson)
        elif cmd_id == 127:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.read_doc_meta_data(ijson)
        elif cmd_id == 128:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.read_index_info(ijson)
        elif cmd_id == 129:
            import company_config as pyf
            s_Obj = pyf.CompanyInfo()
            res = s_Obj.get_company_data(ijson)
        elif cmd_id == 130:
            import t_company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.grid_color_info_label_taxo_check(ijson)
        elif cmd_id == 131:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.update_table_type(ijson)
        elif cmd_id == 132:
            import insert_rule_information as pyf
            s_Obj = pyf.RuleInfo()
            res = s_Obj.insert_rule_populate_info(ijson)
        elif cmd_id == 133:
            res = self.read_model_default(ijson)
        elif cmd_id == 134:
            import dashboard_populate_wrapper as pyf
            d_Obj = pyf.DashBoard()
            res = d_Obj.get_running_stats()
        elif cmd_id == 135:
            res = self.read_all_primary_keys(ijson)
        elif cmd_id == 136:
            res = self.write_grouped_primary_keys(ijson)
        elif cmd_id == 137:
            import modules.template_mgmt.model_api as ma 
            ma_obj = ma.model_api()
            res = ma_obj.update_sheet_data(ijson)

        elif cmd_id == 138:
            import modules.template_mgmt.model_api as ma 
            ma_obj = ma.model_api()
            res = ma_obj.read_sheet_col_data(ijson)
        elif cmd_id == 139:
            res = self.get_all_column_for_sheet(ijson)
        elif cmd_id == 140:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.read_data_type_info(ijson)
        elif cmd_id == 141:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.add_data_type_from_interface(ijson)
        elif cmd_id == 142:
            import sheet_info_create_txt as pyf
            s_Obj = pyf.SheetInfo()
            res = s_Obj.sheet_drop_down(ijson)

        elif cmd_id == 143:
            res = self.update_lookup_sheet_info(ijson)
        elif cmd_id == 144:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.update_document_meta_info(ijson)
        elif cmd_id == 145:
            import insert_rule_information as pyf
            s_Obj = pyf.RuleInfo()
            #res = s_Obj.insert_doc_populate_info(ijson)
            #res = s_Obj.insert_company_doc_mgmt(ijson)
            res = s_Obj.insert_company_doc_mgmt_page(ijson)
        elif cmd_id == 146:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.create_groups_tables(ijson)
        elif cmd_id == 147:
            import dashboard_populate_wrapper as pyf
            d_Obj = pyf.DashBoard()
            res = d_Obj.rule_stats(ijson)
        elif cmd_id == 148:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.generate_rules(ijson)
        elif cmd_id == 149:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.table_type_flag_grid_module(ijson)
        elif cmd_id == 150:
            import sheet_info_create_txt_external
            obj = sheet_info_create_txt_external.SheetInfo()
            res = obj.sheet_drop_down(ijson)
        elif cmd_id == 151:
            import dashboard_populate_wrapper as pyf
            d_Obj = pyf.DashBoard()
            res = d_Obj.doc_stats(ijson)
        elif cmd_id == 152:
            import modules.tablets.tablets as tablets
            s_Obj   = tablets.Tablets()
            res     = s_Obj.read_table_bbox(ijson) 
        elif cmd_id == 153:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.read_group_tables(ijson)
        elif cmd_id == 154:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.stats_gh_hgh_vgh(ijson)
        elif cmd_id == 155:
            import modules.databuilder.scope_info as v2
            v2_obj = v2.Scope()
            res = v2_obj.read_scoped(ijson)
        elif cmd_id == 156:
            import warnings 
            warnings.filterwarnings("ignore")
            import modules.databuilder.scope_info as v2
            v2_obj = v2.Scope()
            res = v2_obj.add_tables_to_scoped_group(ijson)

        elif cmd_id == 157:
            import modules.databuilder.scope_info as v2
            v2_obj = v2.Scope()
            res = v2_obj.add_sugg_to_scoped_tables(ijson)

        elif cmd_id == 158:
            import warnings 
            warnings.filterwarnings("ignore")
            import modules.databuilder.scope_info as v2
            v2_obj = v2.Scope()
            res = v2_obj.delete_from_scoped_group(ijson)
        elif cmd_id == 159:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.non_scoped_groups(ijson)

        elif cmd_id == 160:
            import modules.databuilder.scope_info as v2
            v2_obj = v2.Scope()
            res = v2_obj.run_applicator(ijson)

        elif 161 == cmd_id:
            res = self.read_INC_json_multi_tables(ijson)
        elif 162 == cmd_id:
            res = self.filter_groups(ijson)
        elif 163 == cmd_id:
            import modules.databuilder.scope_info as v2
            v2_obj = v2.Scope()
            res = v2_obj.run_app_for_all(ijson)

        elif 164 == cmd_id:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.delete_taxo_tagged_tt(ijson)
            #import modules.databuilder.scope_info as v2
            #v2_obj = v2.Scope()
            #res = v2_obj.reset_tagging(ijson)
        
        elif 165 == cmd_id:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.read_class_doc_type_wise(ijson)

        elif 166 == cmd_id:
            import modules.databuilder.form_builder_from_template as f_builder
            db_obj = f_builder.TaxoBuilder()
            res = db_obj.read_super_key_sugg(ijson)
        elif 167 == cmd_id:
            res = self.update_super_key(ijson)

        elif 168 == cmd_id:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.read_tt_wise_taxo_group(ijson)

        elif cmd_id == 169:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.read_db_ref_info_skey(ijson)

        elif 170 == cmd_id:
            res = self.update_super_key_builder(ijson)

        elif 171 == cmd_id:
            res = self.read_poss_builder(ijson)
        elif 172 == cmd_id:
            res = self.read_super_keymerge_poss(ijson)
        elif cmd_id == 173:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.table_column_data(ijson)
        elif 174 == cmd_id:
            res = self.read_super_key_poss(ijson)
        elif 175 == cmd_id:
            res = self.update_super_key_to_db_row(ijson)
        elif 176 == cmd_id:
            import modules.template_mgmt.model_mapping as pyf
            s_Obj = pyf.Mapping()
            res = s_Obj.map_group(ijson)
        elif 177 == cmd_id:
            import modules.databuilder.form_builder_from_template as f_builder
            db_obj = f_builder.TaxoBuilder()
            res = db_obj.read_distinct_values(ijson)
        elif 178 == cmd_id:
            import modules.template_mgmt.model_mapping as pyf
            s_Obj = pyf.Mapping()
            res = s_Obj.link_sheets(ijson)
        elif 179 == cmd_id:
            import modules.template_mgmt.model_mapping as pyf
            s_Obj = pyf.Mapping()
            res = s_Obj.read_header_map_grp(ijson)
        elif cmd_id == 180:
            import altered_table_store as ats
            a_Obj = ats.AlteredTables()
            res = a_Obj.save_altered_tables(ijson)
        elif cmd_id == 181:
            import altered_table_store as ats
            a_Obj = ats.AlteredTables()
            res = a_Obj.read_modified_tables(ijson)

        elif 182 == cmd_id:
            import modules.databuilder.form_builder_from_template as f_builder
            db_obj = f_builder.TaxoBuilder()
            res = db_obj.insert_exclude_rows(ijson)

        elif 183 == cmd_id:
            import modules.template_mgmt.model_mapping as pyf
            s_Obj = pyf.Mapping()
            res = s_Obj.map_preview(ijson)
        elif 184 == cmd_id:
            import modules.template_mgmt.model_mapping as pyf
            s_Obj = pyf.Mapping()
            res = s_Obj.read_lookup_value(ijson)
        elif cmd_id == 185:
            import error_table_stats as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.error_table_stats(ijson)
        elif cmd_id == 186:
            import error_table_stats as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.read_validation_error_rcs(ijson)
        elif cmd_id == 187:
            import super_builder_info as pyf
            s_Obj = pyf.SuperBuilder()
            res = s_Obj.superkey_data_builder_info(ijson)

        elif 188 == cmd_id:
            import modules.template_mgmt.model_mapping as pyf
            s_Obj = pyf.Mapping()
            res = s_Obj.read_Error_Info(ijson)

        elif cmd_id == 189:
            import super_builder_info as pyf
            s_Obj = pyf.SuperBuilder()
            res = s_Obj.read_distinct_taxo_group_id(ijson)

        elif cmd_id == 190:
            import super_builder_info as pyf
            s_Obj = pyf.SuperBuilder()
            res = s_Obj.give_sk_poss_builder_info(ijson)
        elif cmd_id == 191:
            import company_mgmt_data as pyf
            s_Obj = pyf.INC_Company_Mgmt()
            res = s_Obj.tab_module_format(ijson)
        elif cmd_id == 192:
            import super_builder_info as pyf
            s_Obj = pyf.SuperBuilder()
            res = s_Obj.read_fe_signature(ijson)

        elif 193 == cmd_id:
            import modules.databuilder.form_builder_from_template as f_builder
            db_obj = f_builder.TaxoBuilder()
            res = db_obj.primary_key_match(ijson)

        elif 194 == cmd_id:
            import modules.databuilder.form_builder_from_template as f_builder
            db_obj = f_builder.TaxoBuilder()
            res = db_obj.update_primary_key_match(ijson)

        elif cmd_id == 195:
            import super_builder_info as pyf
            s_Obj = pyf.SuperBuilder()
            res = s_Obj.read_column_signature_stats(ijson)

        elif cmd_id == 196:
            import super_builder_info as pyf
            s_Obj = pyf.SuperBuilder()
            res = s_Obj.create_save_db_row_ids_info(ijson)

        elif cmd_id == 197:
            import super_builder_info as pyf
            s_Obj = pyf.SuperBuilder()
            res = s_Obj.read_multi_DB_data_builder(ijson)
        elif cmd_id == 198:
            import read_populate_error_stats
            s_Obj   = read_populate_error_stats.READSTATS()
            res = s_Obj.read_data(ijson)

        elif cmd_id == 199:
            import read_populate_error_stats
            s_Obj   = read_populate_error_stats.READSTATS()
            res = s_Obj.err_highlight(ijson)

        elif cmd_id == 200:
            import modules.template_mgmt.model_api as ma 
            ma_obj = ma.model_api()
            res = [{"message":"done"}] #ma_obj.delete_model_data_cell_wise(ijson)

        elif cmd_id == 201:
            import modules.template_mgmt.model_api as ma 
            ma_obj = ma.model_api()
            res = ma_obj.insert_sheet_old_template(ijson)
            pass  

        elif cmd_id == 202:
            import modules.template_mgmt.model_api as ma 
            ma_obj = ma.model_api()
            res = ma_obj.check_avail_name(ijson)

        elif cmd_id == 203:
            import modules.template_mgmt.model_api as ma 
            ma_obj = ma.model_api()
            res = ma_obj.check_avail_sname(ijson)

        elif cmd_id == 204: #read sheets 
            import modules.template_mgmt.model_api as ma 
            ma_obj = ma.model_api()
            res = ma_obj.read_sheets(ijson)

        elif cmd_id == 205: #delete sheet 
            import modules.template_mgmt.model_api as ma 
            ma_obj = ma.model_api()
            res = ma_obj.delete_sheets(ijson)
 
        elif cmd_id == 206: #sheet insert
            import modules.template_mgmt.model_api as ma 
            ma_obj = ma.model_api()
            res = ma_obj.insert_sheet_old_template_new_229(ijson)

        elif cmd_id == 207: #sheet insert
            import modules.template_mgmt.model_api as ma 
            ma_obj = ma.model_api()
            res = ma_obj.sheet_gridinfo(ijson)

        elif 208 == cmd_id:
            res = self.read_template_grids_info(ijson)

        elif cmd_id == 209: #read ref info
            import modules.template_mgmt.model_api as ma 
            ma_obj = ma.model_api()
            res = ma_obj.read_ref_info(ijson)

        elif cmd_id == 210: #read ref info
            import modules.template_mgmt.model_api as ma 
            ma_obj = ma.model_api()
            res = ma_obj.insert_lk_value_mgmt(ijson)

        elif cmd_id == 212: #read ref info
            import modules.template_mgmt.model_api as ma 
            ma_obj = ma.model_api()
            res = ma_obj.import_from_global(ijson)

        elif cmd_id == 213:
            import modules.databuilder.form_builder_from_traverse_path as ma 
            ma_obj = ma.DataBuilder()
            res = ma_obj.form_builder_from_files(ijson)
    
        elif cmd_id == 214:
            import modules.template_mgmt.store_re as ma 
            ma_obj = ma.restore_api()
            res = ma_obj.read_info(ijson)

        elif cmd_id == 215:
            import modules.template_mgmt.store_re as ma 
            ma_obj = ma.restore_api()
            res = ma_obj.insert_row(ijson)
                    
        elif cmd_id == 216:
            import modules.template_mgmt.store_re as ma 
            ma_obj = ma.restore_api()
            res = ma_obj.restore_data(ijson)

        elif cmd_id == 217:
            import modules.template_mgmt.store_re as ma 
            ma_obj = ma.restore_api()
            res = ma_obj.delete_row(ijson)

        elif cmd_id == 218:
            import create_excel_v1 as pyf
            tc_Obj = pyf.excel()
            res = tc_Obj.cl_create(ijson)

        elif cmd_id == 219:
            import modules.template_mgmt.read_info as ma 
            ma_obj = ma.model_api()
            res = ma_obj.sheet_gridinfo(ijson)
        elif 220 == cmd_id:
            res = self.read_super_keymerge_poss_all(ijson)
        elif 221 == cmd_id:
            res = self.read_builder_data_row_ids(ijson)

        elif cmd_id == 222:
            import create_model_info as pyf
            s_Obj = pyf.ModelInfo()
            res = s_Obj.output_model_info_structure(ijson)

        elif 223 == cmd_id:
            import modules.databuilder.form_builder_from_template as f_builder
            db_obj = f_builder.TaxoBuilder()
            res = db_obj.read_super_key_builder(ijson)

        elif cmd_id == 2061: #sheet insert
            import modules.template_mgmt.tmodel_api as ma 
            ma_obj = ma.model_api()
            res = ma_obj.insert_sheet_old_template_new_229(ijson)


        return json.dumps(res)


        

if __name__ == '__main__':
    obj = WebAPI()

    try:
        ijson   = json.loads(sys.argv[1])
        if not isinstance(ijson, dict):
                print xxxx
    except:
        cmd_id  = int(sys.argv[1])
        ijson   =  {"cmd_id":cmd_id}
        if len(sys.argv) > 2:
            tmpjson = json.loads(sys.argv[2])
            ijson.update(tmpjson)
    cmd_id  = int(ijson['cmd_id'])
    if ijson.get('PRINT') != 'Y':
        disableprint()
    res = obj.process(cmd_id, ijson)
    enableprint()
    print res
