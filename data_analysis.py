def descriptor_einsman(df, name = "no name defined"):
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

