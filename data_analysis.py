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