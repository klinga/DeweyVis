import pandas as pd
import numpy as np


from dewey import DEWEY_MAIN, DEWEY_HUNDREDS, DEWEY_THOUSANDS


def find_main_lbl(value):
    try:
        n = int(value[0] + '00')
        if n in DEWEY_MAIN:
            return DEWEY_MAIN[n]
        else:
            return np.NaN
    except TypeError:
        return np.NaN


def find_main(value):
    try:
        return value[0] + '00'
    except TypeError:
        return np.NaN


def find_hundreds(value):
    try:
        return value[:2] + '0'
    except TypeError:
        return np.NaN


def find_hundreds_lbl(value):
    try:
        n = int(value[:2] + '0')
        if n in DEWEY_HUNDREDS:
            return DEWEY_HUNDREDS[n]
        else:
            return np.NaN
    except TypeError:
        return np.NaN


def find_thousands(value):
    try:
        n = int(value)
        if n in DEWEY_THOUSANDS:
            return DEWEY_THOUSANDS[n]
        else:
            return np.NaN
    except ValueError:
        return np.NaN


fh = './files/bpl_171220_raw.csv'

reader = pd.read_csv(
    fh, header=0,
    parse_dates=[
        'bDateCreated', 'bDateCataloged',
        'iDateCreated', 'iLastCheckoutDate'],
        dtype={'pDate': str},
        iterator=True,
        na_values=['  -  -  ', ],
        chunksize=10**4)
# df_test = reader.next()
# print df_test.head(10)

for chunk in reader:
    # original cataloging agency
    df_cat = chunk['catAgent']
    print df_cat.head()


# df['thousands'] = df['callno'].str.extract(r'(\d{3})', expand=False)
# df['thousands_lbl'] = df['thousands'].apply(find_thousands)
# df['main'] = df['thousands'].apply(find_main)
# df['main_lbl'] = df['thousands'].apply(find_main_lbl)
# df['hundreds'] = df['thousands'].apply(find_hundreds)
# df['hundreds_lbl'] = df['thousands'].apply(find_hundreds_lbl)

# df = df[df['main'].notnull()]
# df.to_csv('./files/bpl_dewey_cat.csv')
# df_main = df[['main', 'main_lbl']]
# df_main = df_main.groupby('main')['main_lbl'].value_counts()
# df_main.to_csv('./files/bpl_main_classes.csv')

