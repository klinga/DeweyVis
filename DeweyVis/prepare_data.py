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


fh = './files/callno.csv'

df = pd.read_csv(fh, header=None, names=['id', 'callno'])
df['thousands'] = df['callno'].str.extract(r'(\d{3})', expand=False)
df['thousands_lbl'] = df['thousands'].apply(find_thousands)
df['main'] = df['thousands'].apply(find_main)
df['main_lbl'] = df['thousands'].apply(find_main_lbl)
df['hundreds'] = df['thousands'].apply(find_hundreds)
df['hundreds_lbl'] = df['thousands'].apply(find_hundreds_lbl)

df = df[df['main'].notnull()]
print df.head(10)