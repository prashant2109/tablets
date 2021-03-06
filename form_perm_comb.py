import itertools
class form_perm_comb():
    def form_perm_comb(self, ar):
        idx_d ={}
        rev_d   = {}
        for i, grp in enumerate(ar):
            grp.sort()
            idx_d[tuple(grp)] =  i
            for elm in grp:
                rev_d.setdefault(elm, {})[tuple(grp)]   = 1
        for i, grp in enumerate(ar):
            other_grp   = {}
            for elm in grp:
                other_grp.update(rev_d.get(elm, {}))
            #print '\n========================'
            #print grp
            #for elm in other_grp.keys():
            #    print '\t', elm
            ftup    = ()
            for tup in other_grp.keys():
                ftup    += tup
                if tup in idx_d:
                    del idx_d[tup] 
            far    = list(set(ftup))
            far.sort()
            idx_d[tuple(far)]   = 1
            for elm in far:
                rev_d.setdefault(elm, {})[tuple(far)]   = 1
        return idx_d.keys()

    def form_all_pos_comb(self, ar):
        final_d = {}
        for L in range(0, len(ar)+1):
            for subset in itertools.combinations(ar, L):
                final_d[subset] = 1
        return final_d.keys()
if __name__ == '__main__':
    obj = form_perm_comb()
    print obj.form_perm_comb([['6_6_3', '6_6_5'], ['6_8_3', '6_9_1'], ['6_9_1', '6_10_2'], ['6_10_2', '6_11_3'], ['6_11_3', '6_12_1'], ['6_12_1', '6_13_3'], ['6_15_1', '6_16_1'], ['6_16_1', '6_17_1'], ['6_17_1', '6_18_1'], ['6_18_1', '6_19_1'], ['6_19_1', '6_20_1'], ['6_20_1', '6_21_1'], ['6_21_1', '6_22_1'], ['6_22_1', '6_23_1'], ['6_23_1', '6_24_1'], ['6_24_1', '6_25_1'], ['6_26_3', '6_26_4'], ['6_28_3', '6_28_4'], ['6_29_3', '6_29_4'], ['6_30_1', '6_31_1'], ['6_34_1', '6_35_1'], ['6_35_1', '6_36_1'], ['6_36_1', '6_37_1'], ['6_37_1', '6_38_1'], ['6_38_1', '6_39_3'], ['6_41_1', '6_42_1'], ['6_42_1', '6_43_1'], ['6_43_1', '6_44_1'], ['6_44_1', '6_45_1'], ['6_45_1', '6_46_3'], ['6_47_1', '6_48_1'], ['6_48_1', '6_49_1'], ['6_49_1', '6_50_1'], ['6_50_1', '6_51_2'], ['6_53_1', '6_54_1'], ['6_54_1', '6_55_1'], ['6_55_1', '6_56_1'], ['6_56_1', '6_57_2'], ['6_59_1', '6_60_1'], ['6_60_1', '6_61_1'], ['6_61_1', '6_62_1'], ['6_62_1', '6_63_1'], ['6_64_2', '6_65_2'], ['6_65_2', '6_66_2'], ['6_66_2', '6_67_2'], ['6_67_2', '6_68_1'], ['6_68_1', '6_69_2'], ['6_69_2', '6_70_2'], ['6_70_2', '6_71_2'], ['6_71_2', '6_72_2'], ['6_73_1', '6_74_1'], ['6_74_1', '6_75_1'], ['6_75_1', '6_76_1'], ['6_76_1', '6_77_1'], ['6_77_1', '6_78_1'], ['6_78_1', '6_79_1'], ['6_79_1', '6_80_1'], ['6_80_1', '6_81_1'], ['6_81_1', '6_82_1'], ['6_93_1', '6_94_3'], ['6_98_1', '6_99_2'], ['6_100_1', '6_101_1'], ['6_101_1', '6_102_1'], ['6_102_1', '6_103_1'], ['6_103_1', '6_104_1'], ['6_106_1', '6_107_1'], ['6_109_1', '6_110_2'], ['6_111_1', '6_112_1'], ['6_112_1', '6_113_1'], ['6_113_1', '6_114_1'], ['6_114_1', '6_115_1'], ['6_116_1', '6_117_1'], ['6_117_1', '6_118_1'], ['6_118_1', '6_119_1'], ['6_119_1', '6_120_1']])
                
            
                
        
        
