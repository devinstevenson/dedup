import argparse
import datetime as dt
import hashlib
import io
import os
import re
import sys
import logging
import pandas as pd
if sys.version_info.major == 2:
    import cPickle as pickle
else:
    import pickle
# PY3
REG_NOT_ASCII = r'[^\x00-\x7f]'
BYTES_CHUNK = 4096
if sys.version_info.major == 2:
    RMODE = 'rb' if sys.platform == 'nt' else 'rb'
    WMODE = 'wb' if sys.platform == 'nt' else 'wb'
else:
    RMODE = 'rb'
    WMODE = 'wb'

n = dt.datetime.now()
now = '-'.join(map(lambda x: str(x).zfill(2), [n.year, n.month, n.day, n.hour, n.minute,
                                               n.second]))
FORMAT = ('%(levelname)s: %(asctime)s: lineno=%(lineno)s: '
          'function=%(funcName)s: %(message)s')
formatter = logging.Formatter(FORMAT)
logging.basicConfig(format=FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

skipfiles_starts = ['.dropb', '.DS', '.ds', '.Ds', '.dS', '.smbd', '._']


def pdump(data, name):
    if not name.endswith('.p'):
        name += '.p'
    with io.open(name, WMODE) as f:
        pickle.dump(data, f, pickle.HIGHEST_PROTOCOL)


def pload(name):
    if not name.endswith('.p'):
        name += '.p'
    with io.open(name, RMODE) as f:
        data = pickle.load(f)
    return data


def hash_reader(fullname):
    h = hashlib.md5()
    try:
        with io.open(fullname, RMODE) as f:
            while True:
                data = f.read(BYTES_CHUNK)
                if data:
                    h.update(data)
                else:
                    break
    except IOError:
        logger.exception(fullname)
        return 'IOError'
    return h.hexdigest()


def is_excluded(path, exclude):
    spath = os.path.split(path)
    if spath[-1].startswith('.'):
        return True
    for ex in exclude:
        if path.startswith(ex):
            return True
    return False


def get_record(filenamepath):
    digest = hash_reader(filenamepath)
    stat = os.stat(filenamepath)
    return {'digest': digest,
            'ctime': stat.st_ctime,
            'atime': stat.st_atime,
            'mtime': stat.st_mtime,
            'size': stat.st_size,
            }


def create_index(dataset, hashing=False):
    logger.info("started")
    index_name = hash_string(dataset)
    datarecord = []
    failed_rec = []
    recap = datarecord.append  # alias
    g = os.walk(dataset)

    try:
        for folder, _, files in g:
            for filename in files:
                if any([filename.startswith(sw) for sw in skipfiles_starts]):
                    continue
                path = os.path.join(folder, filename)
                rec = {'filename': filename,
                       'path': path}
                try:
                    if hashing:
                        rec.update(get_record(path))
                    recap(rec)
                except Exception as e:
                    rec['error'] = str(e)
                    failed_rec.append(rec)
    except:
        # only error iterating
        pdump(datarecord, 'indexpartial {}_{}'.format(index_name, now))
        logger.exception("run fail complete:{} fail_cnt:{}".format(len(datarecord),
                                                                   len(failed_rec)))
    finally:
        if failed_rec:
            dff = pd.DataFrame(failed_rec)
            dff.to_pickle('failed_recs_{}.p'.format(now))
            dff.to_csv('failed_recs_{}.csv'.format(now))
    df = pd.DataFrame(datarecord)
    df.to_pickle('index {}_{}.p'.format(index_name, now))
    df.to_csv('index {}_{}.csv'.format(index_name, now))
    logger.info("length of dataset {}".format(len(datarecord)))


def hash_string(s, n=8):
    try:
        h = hashlib.md5(str(s))
    except TypeError:
        h = hashlib.md5(str(s).encode('utf-8'))
    return h.hexdigest()[:n]


def get_files_matching(root, name, exact=True, startswith=False):
    matches = []
    matchap = matches.append
    for folder, _, files in os.walk(root):
        for f in files:
            if exact:
                if f == name:
                    matchap(os.path.join(folder, f))
            if startswith:
                if f.startswith(name):
                    matchap(os.path.join(folder, f))
    return matches


def count_files(root):
    c = 0
    for folder, _, files in os.walk(root):
        for f in files:
            c += 1
    print(c)


def delete_files(files):
    for f in files:
        os.remove(f)


def rename_bad(bad):
    for folder in bad:
        new = re.sub(REG_NOT_ASCII, ' ', folder)
        if os.path.exists(new):
            new = re.sub(REG_NOT_ASCII, ' -', folder)
            if os.path.exists(new):
                new = re.sub(REG_NOT_ASCII, ' --', folder)
        try:
            os.rename(folder, new)
        except:
            print(folder)


def find_bad_names(root):
    bad = []
    bfile = []
    for folder, _, files in os.walk(root):
        if re.sub(REG_NOT_ASCII, 'XXXXXXXX', folder) != folder:
            bad.append(folder)
        for f in files:
            if re.sub(REG_NOT_ASCII, 'XXXXXXXX', f) != f:
                bfile.append((folder, f))
    print(len(bad), len(bfile))
    return bad, bfile


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--path')
    parser.add_argument('--hash', action='store_true')
    args = parser.parse_args()
    dataset = args.path
    h = hash_string(dataset)
    logfilename = 'debug-{}-{}.log'.format(h[:8], now)
    filelog = logging.FileHandler(logfilename)
    filelog.setFormatter(formatter)
    logging.root.addHandler(filelog)
    logger.info("Hash={} Args={}".format(h, args))

    create_index(dataset, hashing=args.hash)
