import pandas as pd

def descriptor_einsman(df, name = "no name defined", limited = True):
    df['Time (CET)'] = pd.to_datetime(df['Time (CET)'], utc = True)
    df['Time (CET)'] = df['Time (CET)']
    df.sort_values(by = 'Time (CET)', inplace = True)
    if limited:
        df = df[(df['Time (CET)'].dt.year < 2018)]
    col_list = ['ID', 'Einsatz-ID', 'Dauer(min)', 'Gebiet',
                'Ort Engpass', 'Stufe(%)', 'Ursache', 'Anlagenschluessel', 'Anforderer',
                'Netzbetreiber', 'Anlagen-ID', 'Entschaedigungspflicht', 'Time (CET)']
    for i in col_list:
        if i == 'Time (CET)':
            print('Starting date', df[i].head(1))
            print('Ending date', df[i].tail(1))
        elif i in ['Ursache', 'Stufe(%)', 'Anforderer',
                   'Netzbetreiber', 'Entschaedigungspflicht']:
            print('Unique values of {}'.format(i), df[i].unique())
        elif i == "Dauer(min)":
            print('Durchschnittliche Dauer', df[i].mean())
            print('Median Dauer', df[i].median())
            print('Std Dauer', df[i].std())
            print('Max Dauer', df[i].max())
            print('Min Dauer', df[i].min())
        else:
            print('Length of {}'.format(i), len(df[i].unique()))
    print("finished processing of {} \n".format(name))
    return


def filter_tso(df, tso = 'none', limited = True, output = True, name = 'no_name'):
    df['Time (CET)'] = pd.to_datetime(df['Time (CET)'], utc = True)
    df['Time (CET)'] = df['Time (CET)']
    df.sort_values(by = 'Time (CET)', inplace = True)
    if limited:
        df = df[(df['Time (CET)'].dt.year < 2018)]
    if tso not in ['50 Hertz','50Hertz', 'TenneT', 'tennet']:
        print('No tso name given - no processing')
    elif tso in ['50 Hertz','50Hertz']:
        df.Anforderer = df.Anforderer.apply(str)
        df = df[df.Anforderer.str.contains('Hertz')]
        if output:
            df.to_csv('/Users/benni/PycharmProjects/CongDataAnalysis/Filtered EinsManData/'+
                      name + '_filtered_for_' + tso + '.csv', index = False)
    elif tso == ['TenneT','tennet']:
        df.Anforderer = df.Anforderer.apply(str)
        df.Anforderer = df.Anforderer.replace(['4033872000058','4050404000003'], 'TenneT')
        df = df[df.Anforderer.str.contains(tso)]
        if output:
            df.to_csv('/Users/benni/PycharmProjects/CongDataAnalysis/Filtered EinsManData/'+
                      name + '_filtered_for_' + tso + '.csv', index = False)
    print('finished filtering of dataframe {name} for TSO {tso}'.format(name = name,tso = tso))
    return df


def read_in_redispatch_data(filter = 'none', split = True, translate = True, output = True):
    time_frame = pd.DataFrame()
    time_frame['Begin Time (CET)'] = pd.date_range(start='2015-01-01', end='2017-12-31 23:59:59', freq='H')
    redispatch = pd.read_csv('/Users/benni/PycharmProjects/CongDataAnalysis/redispatch_data/redispatch_netztranparenz.csv',
                        encoding='ISO-8859-1', sep=None, engine='python')
    redispatch = redispatch.loc[:, ~redispatch.columns.str.contains('^Unnamed')]
    redispatch['Beginn Zeit (CET)'] = pd.to_datetime(redispatch['BEGINN_DATUM'] + ' ' + redispatch['BEGINN_UHRZEIT'],
                                                    errors = 'coerce', dayfirst = True)
    redispatch['Ende Zeit (CET)'] = pd.to_datetime(redispatch['ENDE_DATUM'] + ' ' + redispatch['ENDE_UHRZEIT'],
                                                  errors = 'coerce', dayfirst = True)
    redispatch.drop(columns = ['BEGINN_DATUM','BEGINN_UHRZEIT','ENDE_DATUM', 'ENDE_UHRZEIT'])
    redispatch['Dauer'] = redispatch['Ende Zeit (CET)'] - redispatch['Beginn Zeit (CET)']
    if translate:
        redispatch.rename(columns = {
       'NETZREGION':'Net_region', 'GRUND_DER_MASSNAHME':'Reason_for_measure', 'RICHTUNG':'Direction',
        'MITTLERE_LEISTUNG_MW':'Mean_power_MW', 'MAXIMALE_LEISTUNG_MW':'MAX_POWER_MW',
       'GESAMTE_ARBEIT_MWH':'TOTAL_WORK_MWH', 'ANWEISENDER_UENB':'DIRECTING_TSO',
       'ANFORDERNDER_UENB':'REQUESTING_TSO', 'BETROFFENE_ANLAGE':'AFFECTED_PS', 'Redispatch_KW':'Redispatch_KW',
        'Beginn Zeit (CET)':'Begin Time (CET)', 'Ende Zeit (CET)': 'End Time (CET)', 'Dauer':'Duration' }, inplace = True)
    if split:
        redispatch['Direction'] = redispatch['Direction'].replace('Wirkleistungseinspeisung reduzieren',
                                                            'decrease')
        redispatch['Direction'] = redispatch['Direction'].str.replace(r'(^.*Wirk.*)',
                                                                'increase')
    if filter in ['50Hertz', 'TenneT', 'TransnetBW', 'Amprion']:
        redispatch = redispatch[redispatch['REQUESTING_TSO'].str.contains(filter)]
        print('Data has been filtered for TSO {}'.format(filter))
    else:
        print('Data has not been filtered or does not match needed filter')
    result = pd.merge(time_frame, redispatch,how='left', on=['Begin Time (CET)', 'Begin Time (CET)'])
    if output:
        result.to_csv('redispatch_data/Transformed_redispatch_data/redispatch_filtered_for_TSO_{}'.format(filter) + '.csv',
                      index = False)
        if split:
            result[result['Direction'] == 'increase'].to_csv(
                'redispatch_data/Transformed_redispatch_data/redispatch_filtered_for_TSO_{}_increase'.format(filter) + '.csv',
                index=False)
            result[result['Direction'] == 'decrease'].to_csv(
                'redispatch_data/Transformed_redispatch_data/redispatch_filtered_for_TSO_{}_decrease'.format(
                    filter) + '.csv', index=False)
    return result

