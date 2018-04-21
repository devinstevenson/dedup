import datetime as dt
import hashlib
import os, sys
import logging
import cPickle as pickle

BYTES_CHUNK = 4096
dataset_a = ''
dataset_b = ''

n = dt.datetime.now()
now = '-'.join(map(lambda x: str(x).zfill(2), [n.year, n.month, n.day, n.hour, n.minute, n.second]))
FORMAT = '%(levelname)s: %(asctime)s: lineno=%(lineno)s: function=%(funcName)s: %(message)s'
formatter = logging.Formatter(FORMAT)
logging.basicConfig(format=FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def pdump(data, name):
    if not name.endswith('.p'):
        name += '.p'
    with open(name, 'wb') as f:
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
        with open(fullname, 'rb') as f:
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
    return h.hexdigest(), stat


def create_index(dataset):
    logger.info("started")
    datarecord = []
    failed_rec = []
    recap = datarecord.append  # alias
    g = os.walk(dataset)

    try:
        for folder, _, files in g:
            for filename in files:
                path = os.path.join(folder, filename)
                try:
                    digest, stat = get_record(path)
                    recap((filename, path, digest, stat))
                except:
                    failed_rec.append(path)
    except:
        pdump(datarecord, 'record_failed_{}'.format(now))
        logger.exception("run fail complete:{} fail_cnt:{}".format(len(datarecord),
                                                                   len(failed_rec)))
    finally:
        if failed_rec:
            pdump(failed_rec, 'failed_recs_{}'.format(now))
    index_name = dataset.replace(os.path.sep, '--')
    pdump(datarecord, 'index {}_{}'.format(index_name, now))
    logger.info("length of dataset {}".format(len(datarecord)))


if __name__ == "__main__":
    filelog = logging.FileHandler('debug {}.log'.format(now))
    filelog.setFormatter(formatter)
    logging.root.addHandler(filelog)
    dataset = sys.argv[1]
    create_index(dataset)
