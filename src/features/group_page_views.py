import time
import glob
import pandas as pd
import logging
from multiprocessing import Pool

CORES = 1
LOG_FILE = '../../logs/group_page_views3.log'

logging.basicConfig(format='%(asctime)s %(message)s', filename=LOG_FILE,
                    filemode='w', level=logging.INFO)
log = logging.getLogger(__name__)


def read_data(path):
    df = pd.read_csv(path, chunksize = 100000000)
    return df


def group_data(df):
    n = 0
    for chunk in df:
        start_time = time.time()
        log.info("grouping chunk {0}".format(n))
        pv_per_user = chunk.groupby('uuid')['document_id'].count().reset_index()
        pv_per_user = pv_per_user.rename(columns={'document_id': 'page_views_per_user'})
        log.info("writing chunk {0}".format(n))
        pv_per_user.to_csv('pv_per_user_{0}.csv.gz'.format(n), compression='gzip')
        log.info("time taken for one loop", time.time() - start_time)
        n += 1


def get_num(s):
     str_ls = s.replace('.', '_').split('_')
     return "{0}_{1}".format(str_ls[7], str_ls[8])


# @profile [this took a while to run]
def combine_n_group(files_ls):
    print files_ls
    log.info("Processing these files, {0}".format(files_ls))
    df = pd.DataFrame()
    for filename in files_ls:
        log.info("reading file, {}".format(filename))
        new_df = pd.read_csv(filename)
        log.info("concatanating data")
        df = pd.concat([df, new_df]) # reduce
        # free up some memory by deleting unnecessary df
        del new_df

    log.info("done with loop")
    log.info("grouping data for, {0}".format(files_ls))
    pv_per_user = df.groupby('uuid')['page_views_per_user'].count().reset_index()
    log.info("writing file for, {0}".format(files_ls))
    current_time = str(time.time())
    filenums = map(get_num, [files_ls[i] for i in range(len(files_ls))])
    pv_per_user.to_csv('../../data/interim/pv_per_user_{0}_{1}.csv.gz'.format(
    filenums[0], filenums[1]), compression='gzip')


def get_files_ls():
    log.info("creating files ls")
    files_ls = glob.glob('../../data/interim/pv_per_user_*_*.csv')
    interim_ls = []
    processed_ls = []
    for index, filename in enumerate(files_ls):
        interim_ls.append(filename)
        if index != 0 and index % 2 == 0:
            processed_ls.append(interim_ls)
            interim_ls = []
    print processed_ls
    # import ipdb; ipdb.set_trace()
    return processed_ls


if __name__ == "__main__":
    # path = '../../data/raw/page_views.csv.zip'
    start_time = time.time()
    # print "reading df in chunks"
    # df = read_data(path)
    # print "done reading file, time taken:", time.time()- start_time
    # group_data(df)
    #
    pool = Pool(CORES)
    processed_ls = get_files_ls()
    pool.map(combine_n_group, processed_ls)
    # combine_n_group(processed_ls[0])

    pool.close()
    pool.join()

    log.info("total time taken {}, ".format(time.time() - start_time))
