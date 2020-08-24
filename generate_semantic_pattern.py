import os
class generate_semantic_pattern:
     def generate_semantic_patt(self,chr_list):
           semantic_type=''
           semantic_ls=chr_list
           for semantic_tup in semantic_ls: 
               for k in range(0,len(semantic_tup[0])):
                     semantic_type+=semantic_tup[0][k]+str(len(semantic_tup[1][k]))
               semantic_type+='_'
           semantic_type=semantic_type[:-1]
           return semantic_type

if __name__=="__main__":
    obj=generate_semantic_pattern()
    chr_list=[('Cc', ['P', 'ortfolionumber'], [(0, 1), (1, 15)])]
    obj.generate_semantic_patt(chr_list)
  
