from data_analysis import *
import pandas as pd
import os
from prepare_features import *
from einsman_forecast import model
########Variables
description = False
tso_filter = False
redispatch_filter = False
merge = True
prepare_forecast_einsman = False
forecast = False
#################
bag = pd.read_csv('/Users/benni/Desktop/Uni/Paper/Einsman/bag.csv', low_memory = False)
edi = pd.read_csv('/Users/benni/Desktop/Uni/Paper/Einsman/edi.csv', low_memory = False)
shn = pd.read_csv('/Users/benni/Desktop/Uni/Paper/Einsman/shn.csv', low_memory = False)
ava = pd.read_csv('/Users/benni/Desktop/Uni/Paper/Einsman/ava.csv', low_memory = False)

if tso_filter:
    edi = filter_tso(edi, name = 'edi', tso = '50 Hertz')
    edi_binned = binarize_einsman(edi, name = '50Hertz_edi', granularity=200)
    ava = filter_tso(ava, name = 'ava', tso = '50 Hertz')
    ava_binned = binarize_einsman(ava, name = '50Hertz_ava', granularity=200)

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
    dataset = pd.read_csv('features.csv')
    dataset['Time (CET)'] = pd.to_datetime(dataset['Time (CET)'])
    del dataset['Unnamed: 0']
    prep_merge = pd.read_csv('merged_all_cong_data_IMP.csv')
    prep_merge['Time (CET)'] = pd.to_datetime(prep_merge['Time (CET)'])
    prep_merge = prep_merge.set_index('Time (CET)', drop = True)
    dataset_edi = dataset.join(prep_merge['50Hertz_edi_einsman'], on = 'Time (CET)', how = 'inner')
    dataset_ava = dataset.join(prep_merge['50Hertz_ava_einsman'], on = 'Time (CET)', how = 'inner')
    dataset_edi.to_csv('dataset_edi.csv')
    dataset_ava.to_csv('dataset_ava.csv')

if forecast:
    dataset_edi = pd.read_csv('dataset_edi.csv')
    print('EDI ###############################')
    model(dataset_edi)
