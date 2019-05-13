from data_analysis import *
import pandas as pd
from forecast_einsman import *
########Variables
description = False
tso_filter = False
redispatch_filter = False
merge = False
prepare_forecast_einsman = True
#################
bag = pd.read_csv('/Users/benni/Desktop/Uni/Paper/Einsman/bag.csv', low_memory = False)
edi = pd.read_csv('/Users/benni/Desktop/Uni/Paper/Einsman/edi.csv', low_memory = False)
shn = pd.read_csv('/Users/benni/Desktop/Uni/Paper/Einsman/shn.csv', low_memory = False)
ava = pd.read_csv('/Users/benni/Desktop/Uni/Paper/Einsman/ava.csv', low_memory = False)

if tso_filter:
    edi = filter_tso(edi, name = 'edi', tso = '50 Hertz')
    edi_binned = binarize_einsman(edi, name = '50Hertz_edi')
    ava = filter_tso(ava, name = 'ava', tso = '50 Hertz')
    ava_binned = binarize_einsman(ava, name = '50Hertz_ava')
    shn = filter_tso(shn,  name = 'shn', tso = '50 Hertz')
    shn_binned = binarize_einsman(shn, name = '50Hertz_shn')
    bag = filter_tso(bag, name = 'bag', tso = '50 Hertz')
    bag_binned = binarize_einsman(bag, name = '50Hertz_bag')

if description:
    descriptor_einsman(bag, name = 'Bayernwerke AG')
    print('---------------------------------------')
    descriptor_einsman(edi, name = 'EDI.S Netz')
    print('---------------------------------------')
    descriptor_einsman(shn, name = 'Schleswig-Holstein Netz')
    print('---------------------------------------')
    descriptor_einsman(ava, name = 'Avacon Netz')


##################

if redispatch_filter:
    redispatch_data = read_in_redispatch_data(filter = '50Hertz')
    bin_redispatch = binarize_redispatch(redispatch_data, name = '50Hertz')

if merge:
    merged = merger()

if prepare_forecast_einsman:
    dataset = dataset_forecast()
    prep_merge = pd.read_csv('merged_all_cong_data_IMP.csv')
    dataset_edi = dataset.join(prep_merge['50Hertz_edi_einsman'], on = 'Time (CET)', how = 'inner')
    dataset_ava = dataset.join(prep_merge['50Hertz_ava_einsman'], on = 'Time (CET)', how = 'inner')
    dataset_edi.to_csv('dataset_edi.csv')
    dataset_ava.to_csv('dataset_ava.csv')