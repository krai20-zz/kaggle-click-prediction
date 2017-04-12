import pandas as pd
from extract_more_features import read_data

def drop_columns(df, col_list):
    df = df.drop(col_list, axis=1)
    return df

def to_int(df, col_list):
    for col in col_list:
        df[col] = df[col].astype('int')
    return df

def run(filename, write_file, drop_col_list, int_col_list):
    print "reading data"
    df = read_data(train_file)
    print "dropping columns"
    drop_col_list = []
    df = drop_columns(df, drop_col_list)
    print "converting to int"
    int_col_list = []
    df = to_int(df, int_col_list)
    print "writing file"
    df.to_csv(write_file, compression='gzip')


if __name__ == "__main__":
    # training data
    train_file = '../../data/interim/merged3.csv.gz'
    train_write_file = '../../data/processed/training_data.csv.gz'
    drop_col_list = [u'Unnamed: 0', u'Unnamed: 0.1', u'Unnamed: 0.1.1',
                     u'uuid', u'publish_time', u'country']
    int_col_list = [u'display_id', u'ad_id', u'clicked', u'document_id',
                    u'platform', u'ad_document_id', u'ad_campaign_id',
                    u'ad_advetiser_id', u'ad_document_source', u'publisher_id',
                    u'publish_time', u'doc_impressions', u'e_country',
                    u'day_of_week', u'hour_of_day']
    run(train_file, train_write_file, drop_col_list, int_col_list)

    # test data
    test_file = '../../data/interim/test_merged3.csv.gz'
    test_write_file = '../../data/processed/kaggle_test_data.csv.gz'
    run(train_file, test_write_file, drop_col_list, int_col_list)
