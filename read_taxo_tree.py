import modules.databuilder.taxo_builder as taxo_builder
obj = taxo_builder.TaxoBuilder()
#res = obj.form_db_data({'company_id':1604, 'project_id':5, 'template_id':1, 'shee_id':1,"ref_k":['744e7822f78f34a04b96a15edc62e362', '83']})
res = obj.read_taxo_tree({'company_id':1604, 'project_id':5, 'template_id':1, 'shee_id':1})
print res
