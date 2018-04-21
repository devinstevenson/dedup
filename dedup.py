import argparse
import datetime as dt
import hashlib
import os
import sys
import logging
import pandas as pd
if sys.version_info.major == 2:
    import cPickle as pickle
else:
    import pickle

BYTES_CHUNK = 4096
if sys.version_info.major == 2:
    RMODE = 'r' if sys.platform == 'nt' else 'rb'
    WMODE = 'w' if sys.platform == 'nt' else 'wb'
else:
    RMODE = 'r'
    WMODE = 'w'

n = dt.datetime.now()
now = '-'.join(map(lambda x: str(x).zfill(2), [n.year, n.month, n.day, n.hour, n.minute,
                                               n.second]))
FORMAT = ('%(levelname)s: %(asctime)s: lineno=%(lineno)s: '
          'function=%(funcName)s: %(message)s')
formatter = logging.Formatter(FORMAT)
logging.basicConfig(format=FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def pdump(data, name):
    if not name.endswith('.p'):
        name += '.p'
    with open(name, WMODE) as f:
        pickle.dump(data, f, pickle.HIGHEST_PROTOCOL)


def pload(name):
    if not name.endswith('.p'):
        name += '.p'
    with open(name) as f:
        data = pickle.load(f)
    return data


def hash_reader(fullname):
    h = hashlib.md5()
    try:
        with open(fullname) as f:
            while True:
                data = f.read(BYTES_CHUNK)
                if data:
                    h.update(data)
                else:
                    break
    except IOError:
        logger.exception(fullname)
        h = hashlib.md5()
        return h.update(fullname)
    return h


def is_excluded(path, exclude):
    spath = os.path.split(path)
    if spath[-1].startswith('.'):
        return True
    for ex in exclude:
        if path.startswith(ex):
            return True
    return False


def get_record(filenamepath):
    h = hash_reader(filenamepath)
    stat = os.stat(filenamepath)
    return {'digest': h.hexdigest(),
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
                path = os.path.join(folder, filename)
                rec = {'filename': filename,
                       'path': path}
                try:
                    if hashing:
                        rec.update(get_record(path))
                    recap(rec)
                except:
                    failed_rec.append(path)
    except:
        # only error iterating
        pdump(datarecord, 'indexpartial {}_{}'.format(index_name, now))
        logger.exception("run fail complete:{} fail_cnt:{}".format(len(datarecord),
                                                                   len(failed_rec)))
    finally:
        if failed_rec:
            pdump(failed_rec, 'failed_recs_{}'.format(now))
    pdump(datarecord, 'index {}_{}'.format(index_name, now))
    logger.info("length of dataset {}".format(len(datarecord)))


def hash_string(s, n=8):
    h = hashlib.md5(str(s))
    return h.hexdigest()[:n]


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
    logger.info("Hash={} Args={}".format(dataset, h, args))

    create_index(dataset, hashing=args.hash)
