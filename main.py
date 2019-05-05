from data_analysis import *
import pandas as pd

bag = pd.read_csv('/Users/benni/Desktop/Uni/Paper/Einsman/bag.csv', low_memory = False)
edi = pd.read_csv('/Users/benni/Desktop/Uni/Paper/Einsman/edi.csv', low_memory = False)
shn = pd.read_csv('/Users/benni/Desktop/Uni/Paper/Einsman/shn.csv', low_memory = False)
ava = pd.read_csv('/Users/benni/Desktop/Uni/Paper/Einsman/ava.csv', low_memory = False)


descriptor_einsman(bag, name = 'Bayernwerke AG')
print('---------------------------------------')
descriptor_einsman(edi, name = 'EDI.S Netz')
print('---------------------------------------')
descriptor_einsman(shn, name = 'Schleswig-Holstein Netz')
print('---------------------------------------')
descriptor_einsman(ava, name = 'Avacon Netz')
