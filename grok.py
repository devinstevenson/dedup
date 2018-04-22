import os
import pandas as pd

bkup = {'hash': '57a83ef1', 'path': 'W:/Z Drive Backup 4-14-18'}
main = {'hash': 'e7c3ebbe', 'path': 'Z:/Dropbox (G Family)'}

datapath = '/Users/devinstevenson/Dropbox/tech_dedup'

deletable = ['.DS_Store', '._.DS_Store', '.ds_store', '.dropbox.attr', '.dropbox',
             '.dropbox.cache', 'Thumbs.db' '.dropbox.device']


def add_location(df):

    split = df.path.str.split('\\')
    location = split.apply(lambda x: x[:-1]).str.join('/')
    location = location.str.replace('W:/Z Drive Backup 4-14-18/Dropbox', 'root')
    location = location.str.replace('Z:/Dropbox (G Family)', 'root')
    folder = location.str.split('/').apply(lambda x: x[-2] if len(x) >= 2 else x[-1])
    return df.assign(location=location, folder=folder)


def filter_fail(df):
    df = df[~df.filename.isin(deletable)]
    df = df[~df.filename.str.startswith('._')]
    return df


def filetyper(f):
    if f.startswith('.'):
        tt = 'dot'
        if f.startswith('._'):
            tt = 'dot_'
    elif '.' in f:
        tt = f.split('.')[-1]
    else:
        tt = 'blank'
    if f in deletable or f.startswith('.smbdelete'):
        tt = 'dontsync'
    return tt


def assignft(df):
    return df.assign(filetype=df.filename.apply(filetyper))


def process(df):
    df = assignft(df)
    df = add_location(df)
    df = df.assign(iscache=is_cache(df.location))
    if df.path.iloc[0].startswith('W'):
        df = df[df.location.str.startswith('root')]
    df = df[df.filetype != 'dontsync']
    df = df[~df.location.str.contains('.dropbox.cache') | ~df.iscache]
    df = df[~df.filename.str.contains('www.dropbox.com')]
    return df


def is_cache(x):
    return x.str.split('/').apply(lambda x: '.dropbox.cache' in x)


def print_sets(dfw, dfz):
    set_w = set(dfw.digest)
    set_z = set(dfz.digest)

    joint = set_w.intersection(set_z)
    disjoint_w = set_w.difference(set_z)
    disjoint_z = set_z.difference(set_w)
    print(dfw.shape, dfz.shape)
    print('joint', len(joint))
    print('disjoint_z', len(disjoint_z))
    print('disjoint_w', len(disjoint_w))


if __name__ == "__main__":
    dfw0 = pd.read_pickle(os.path.join(datapath, 'index 57a83ef1_2018-04-21-10-44-36.p'))
    dfz0 = pd.read_pickle(os.path.join(datapath, 'index e7c3ebbe_2018-04-21-10-39-49.p'))

    fw = pd.read_pickle(os.path.join(datapath, 'failed_recs_2018-04-21-10-44-36.p'))
    fz = pd.read_pickle(os.path.join(datapath, 'failed_recs_2018-04-21-10-39-49.p'))

    mz = pd.concat([dfz0, fz])
    mw = pd.concat([dfw0, fw])

    dfw = process(dfw0)
    dfz = process(dfz0)

    set_w = set(dfw.digest)
    set_z = set(dfz.digest)

    joint = set_w.intersection(set_z)
    disjoint_w = set_w.difference(set_z)
    disjoint_z = set_z.difference(set_w)
    print_sets(dfw, dfz)

    _dfw = dfw[dfw.filename.str.startswith('._')]
    _dfz = dfz[dfz.filename.str.startswith('._')]
    print_sets(_dfw, _dfz)

    _dfwn = dfw[~dfw.filename.str.startswith('._')]
    _dfzn = dfz[~dfz.filename.str.startswith('._')]
    print_sets(_dfwn, _dfzn)
