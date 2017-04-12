import pandas as pd
import time


def read_data(filename):
    df = pd.read_csv(filename, compression='gzip')
    return df


def get_country(df):
    df.geo_location = df.geo_location.astype('str')
    df['country'] = df.geo_location.apply(lambda x: x.split('>')[0])
    df['e_country'] = pd.factorize(df['country'])[0]
    return df


def get_time_features(df):
    df['readable_ts'] = df.timestamp.apply(lambda x: x+1465876799998)
    df['readable_ts'] = pd.to_datetime(df.readable_ts, unit='ms')
    df['day_of_week'] = df.readable_ts.apply(lambda ts: ts.weekday())
    df['hour_of_day'] = df.readable_ts.apply(lambda ts: ts.hour)
    df.drop('readable_ts', axis=1, inplace=True)
    return df


def run(filename, write_file):
    print "reading data"
    df = read_data(filename)
    print "getting country name"
    df = get_country(df)
    print "getting time features"
    df = get_time_features(df)
    print "writing file"
    df.to_csv(write_file, compression='gzip')


if __name__ == "__main__":
    start_time = time.time()
    train_file = '../../data/interim/merged2.csv.gz'
    train_write_file = '../../data/interim/merged3.csv.gz'
    run(train_file, train_write_file)
    print "total time taken, ", start_time - time.time()

    start_time = time.time()
    test_file = '../../data/interim/test_merged2.csv.gz'
    test_write_file = '../../data/interim/test_merged3.csv.gz'
    run(test_file, test_write_file)
    print "total time taken, ", start_time - time.time()
