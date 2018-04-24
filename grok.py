import os
import hashlib
import logging
import shutil
import pandas as pd

# from fuzzywuzzy import fuzz

# PY3
bkup = [{'hash': '57a83ef1', 'path': 'W:/Z Drive Backup 4-14-18'},
        {'hash': '0f7577f2', 'path': 'W:/Z Drive Backup 4-14-18/Dropbox/'}]
main = [{'hash': 'e7c3ebbe', 'path': 'Z:/Dropbox (G Family)'},
        {'hash': 'fd7fa1b0', 'path': 'Z:/Dropbox (G Family)/'}]

datapath = '/Users/devinstevenson/Dropbox/tech_dedup'

deletable = ['.DS_Store', '._.DS_Store', '.ds_store', '.dropbox.attr', '.dropbox',
             '.dropbox.cache', 'Thumbs.db' '.dropbox.device']
delete_filename = ['desktop.ini']

FORMAT = ('%(levelname)s: %(asctime)s: lineno=%(lineno)s: '
          'function=%(funcName)s: %(message)s')
formatter = logging.Formatter(FORMAT)
logging.basicConfig(format=FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logfilename = 'grok_log.log'
filelog = logging.FileHandler(logfilename)
filelog.setFormatter(formatter)
logging.root.addHandler(filelog)


def add_location(df):
    splitjoin = df.path.str.split('\\').str.join('/')
    location = splitjoin.str.replace('W:/Z Drive Backup 4-14-18/Dropbox', 'root')
    location = location.str.replace('Z:/Dropbox \(G Family\)', 'root')
    folder = location.str.split('/').apply(lambda x: x[:-1]).str.join('/')
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


def set_source(df):
    iserror = False
    if 'error' in df.columns:
        iserror = True
    if df.path.iloc[0].startswith('W'):
        key = 'W'
    else:
        key = 'Z'
    # if iserror:
    #     key += 'e'
    return df.assign(source=key)


def process(df):
    df = add_location(df)
    df = df.assign(iscache=is_cache(df.location))
    df = assignft(df)
    df = df[df.filetype != 'dontsync']
    df = df[~df.location.str.contains('.dropbox.cache') | ~df.iscache]
    df = df[~df.filename.str.contains('www.dropbox.com')]
    df = set_source(df)
    df = df[df.filetype != 'lck']
    df = df.assign(
        lochash=df.location.apply(lambda x: hashlib.md5(x.encode('utf-8')).hexdigest()))
    return df


def filterdown(df):
    filehash = df.lochash.unique()
    backup_rename = []
    for fh in filehash:
        sub = df[df.lochash == fh]
        if len(sub) == 1:
            digest = sub.digest.iloc[0]
            dig_sub = df[df.digest == digest]
            if len(dig_sub.source.unique()) == 2:
                truth = dig_sub[dig_sub.source == 'Z'].index[0]
                target = dig_sub[dig_sub.source == 'W'].index[0]


def test_joint2(df, joint):
    lochash = df[df.digest.isin(joint)].lochash  # all loc_hash for joint
    loc = df[df.lochash.isin(lochash)]

    digest_per_lochash = loc.groupby('lochash').digest.unique().to_frame()
    digest_per_lochash['length'] = digest_per_lochash.digest.apply(len)
    sub_digest_per_lochash = digest_per_lochash[digest_per_lochash.length > 1]
    sub_digest_per_lochash['try_nan'] = sub_digest_per_lochash.digest.apply(
        lambda x: x[1])
    # failure to read, unsure if changed
    sub_dig_has_nan = sub_digest_per_lochash[sub_digest_per_lochash.try_nan.isnull()]
    # files are different between location, determine which to keep
    sub_dig_not_nan = sub_digest_per_lochash[sub_digest_per_lochash.try_nan.notnull()]
    return sub_dig_not_nan, sub_dig_has_nan  # pass 1st digest col to determine latest


def test_disjoint(df, disjoint):
    lochash = df[df.digest.isin(disjoint)].lochash  # all loc_hash for joint
    loc = df[df.lochash.isin(lochash)]

    digest_per_lochash = loc.groupby('lochash').digest.unique().to_frame()
    digest_per_lochash['length'] = digest_per_lochash.digest.apply(len)
    sub_digest_per_lochash = digest_per_lochash[digest_per_lochash.length == 1]
    return sub_digest_per_lochash


"""
Plan
For Joint - run test_joint2 on joint. This creates a record of copy from W to Z. Can
    ignore lochash == 1
For Disjoint - run test_joint2 to get matches >1
    For disjoint_w - copy W to Z, if mtime on W is greater than Z.
    
    run test_disjoint to get matches == 1. These need to be copied to other destination.
"""


def determine_latest(df, digest_pairs):
    """Takes output of test_joint2"""
    data = []
    for pair in digest_pairs:
        sub = df[df.digest.isin(pair)]
        # print(sub.digest.nunique(), sub.lochash.nunique())
        # 2 files per sub, up to 6 locations. Plan, get latest file and copy to all Z locations
        from_rec = sub[sub.mtime == sub.mtime.max()].head(1)
        to_locs = sub[(sub.source == 'Z') & (sub.digest != from_rec.digest.iloc[0])]
        # print(sub)
        for ix in to_locs.index:
            data.append((from_rec.path.iloc[0], to_locs.loc[ix].path))
    return data


def make_copy_from_to_since_source(df, test_disjoint_out):
    sub = df[df.lochash.isin(test_disjoint_out.index)]
    base_w = 'W:/Z Drive Backup 4-14-18/Dropbox'
    base_z = r'Z:/Dropbox (G Family)'
    if sub.path.iloc[0].startswith('W'):
        base = base_z
        from_path = sub.location.str.replace('root', base_w)
    elif sub.path.iloc[0].startswith('Z'):
        base = base_w
        from_path = sub.location.str.replace('root', base_z)
    to_path = sub.location.str.replace('root', base)
    return list(zip(from_path, to_path))


def run_make_dataset(df):
    # disjoint_w - missing in Z
    x = test_disjoint(df, disjoint_w)
    files_to_add = make_copy_from_to_since_source(df, x)

    a, b = test_joint2(df, disjoint_w)
    files_to_update = determine_latest(df, a.digest)

    c, d = test_joint2(df, joint)
    files_to_update_jnt = determine_latest(df, c.digest)


def fix_slash(path):
    return '/'.join(path.split('\\'))


def update_files(pairs):
    for source, dest in pairs:
        source = fix_slash(source)
        dest = fix_slash(dest)
        logger.info("Source %s", source)
        logger.info("Dest %s", dest)
        back_file(dest)
        shutil.copyfile(source, dest)


def add_files(pairs):
    for source, dest in pairs:
        make_path(dest)
        logger.info("Source %s", source)
        logger.info("Dest %s", dest)
        shutil.copyfile(source, dest)


def back_file(filepath):
    bkp_path = 'Z:/Dropbox (G Family)/tech_dedup/tmp'
    if not os.path.exists(bkp_path):
        logger.info("Make Dir %s", bkp_path)
        os.makedirs(bkp_path)
    parts = filepath.split('/')
    # fn = parts[-1]
    dest = '/'.join([bkp_path] + parts[1:])
    if not os.path.exists(dest):
        shutil.copyfile(filepath, dest)
    else:
        shutil.copyfile(filepath, dest + '1')
    logger.info("Source %s", filepath)
    logger.info("Dest %s", dest)


def make_path(path):
    """assumes path has a file attached"""

    def get_path(*items):
        return '/'.join(items)

    parts = path.split('/')
    fn = parts[-1]
    parts = parts[:-1]
    logger.info("make path %s", get_path(*parts))
    os.makedirs(get_path(*parts), exist_ok=True)
    # base = get_path(*parts[:2])
    # parts = parts[2:]
    # for i, p in enumerate(parts):
    #     new_path = get_path(base, *parts[:(i+1)])
    #     if not os.path.exists(new_path):
    #         os.mkdir(new_path)


# def string_similarity(a, b):
#     return fuzz.token_set_ratio(a, b)


def crawl(source,
          src_root='W:/Z Drive Backup 4-14-18/Dropbox',
          dst_root='Z:/Dropbox (G Family)',
          dry=True):
    roots = ['/', 'W', 'W:', 'Z Drive Backup 4-14-18', 'Dropbox',
             'Z', 'Z:', 'Dropbox (G Family)', 'Users', 'devinstevenson', 'PycharmProjects', 'deduper']

    same_time = []
    new_time = []
    copy = []
    fail = []
    skip = []
    for fullfolder, _, files in os.walk(source):
        for f in files:
            if f in deletable or f.startswith('.'):
                skip.append(f)
                continue
            fullfolder = fix_slash(fullfolder)
            split = fullfolder.split('/')
            filt = [s for s in split if s not in roots]
            if filt[0] == '':
                filt.pop(0)
            folder = '/'.join(filt)
            # dst_folder = '/'.join([dst_root, folder])
            print(folder)
            print(src_root)
            print(dst_root)
            src_full_file = '/'.join([src_root, folder, f])
            dst_full_file = '/'.join([dst_root, folder, f])
            try:
                src_stat = os.stat(src_full_file)
                if os.path.exists(dst_full_file):
                    dst_stat = os.stat(dst_full_file)
                    if src_stat.st_mtime == dst_stat.st_mtime:
                        same_time.append(src_full_file)
                        logger.info("Same Time %s", src_full_file)
                    elif src_stat.st_mtime > dst_stat.st_mtime:
                        # backup, then copy src to dst
                        logger.info("New Time:")
                        logger.info("Source %s", src_full_file)
                        logger.info("Dest %s", dst_full_file)
                        if not dry:
                            back_file(dst_full_file)
                            shutil.copyfile(src_full_file, dst_full_file)

                        new_time.append(src_full_file)
                    else:
                        skip.append(f)
                        pass  # ignore, nothing needs to be done
                else:
                    logger.info("Copy:")
                    logger.info("Source %s", src_full_file)
                    logger.info("Dest %s", dst_full_file)
                    if not dry:
                        make_path(dst_full_file)
                        shutil.copyfile(source, dst_full_file)
                    copy.append(src_full_file)
            except FileNotFoundError:
                fail.append(src_full_file)
    print("same: ", len(same_time))
    print("new: ", len(new_time))
    print("copy: ", len(copy))
    print("fail: ", len(fail))
    print("skip: ", len(skip))
    return same_time, new_time, copy, fail, skip


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
    print('disjoint_w', len(disjoint_w), '\n')


if __name__ == "__main__":
    # dfw0 = pd.read_pickle(os.path.join(datapath, 'index 57a83ef1_2018-04-21-10-44-36.p'))
    # dfz0 = pd.read_pickle(os.path.join(datapath, 'index e7c3ebbe_2018-04-21-10-39-49.p'))
    #
    # fw = pd.read_pickle(os.path.join(datapath, 'failed_recs_2018-04-21-10-44-36.p'))
    # fz = pd.read_pickle(os.path.join(datapath, 'failed_recs_2018-04-21-10-39-49.p'))

    dfw0 = pd.read_pickle(os.path.join(datapath, 'index 0f7577f2_2018-04-21-21-36-48.p'))
    dfz0 = pd.read_pickle(os.path.join(datapath, 'index fd7fa1b0_2018-04-21-21-40-25.p'))

    fw0 = pd.read_pickle(os.path.join(datapath, 'failed_recs_2018-04-21-21-36-48.p'))
    fz0 = pd.read_pickle(os.path.join(datapath, 'failed_recs_2018-04-21-21-40-25.p'))

    dfw = process(dfw0)
    dfz = process(dfz0)
    fw = process(fw0)
    fz = process(fz0)

    df = pd.concat([dfw, dfz, fw, fz]).reset_index(drop=True)

    set_w = set(dfw.digest)
    set_z = set(dfz.digest)

    print('full sets')
    joint = set_w.intersection(set_z)
    disjoint_w = set_w.difference(set_z)
    disjoint_z = set_z.difference(set_w)
    print_sets(dfw, dfz)
    print('dot underscore')
    _dfw = dfw[dfw.filename.str.startswith('._')]
    _dfz = dfz[dfz.filename.str.startswith('._')]
    print_sets(_dfw, _dfz)
    print('not dot underscore')
    _dfwn = dfw[~dfw.filename.str.startswith('._')]
    _dfzn = dfz[~dfz.filename.str.startswith('._')]
    print_sets(_dfwn, _dfzn)
