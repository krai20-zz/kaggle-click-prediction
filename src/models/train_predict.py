import pandas as pd
from sklearn.ensemble import RandomForestClassifer
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.grid_search import GridSearchCV
from sklearn.metrics import make_scorer
import logging

LOG_FILE = '../../logs/predict0.log'

logging.basicConfig(format='%(asctime)s %(message)s', filename=LOG_FILE,
                    filemode='w', level=logging.INFO)
log = logging.getLogger(__name__)


def read_data(filename):
    df = pd.read_csv(filename)
    return df


def train_classifier(clf, X, y):
    ''' Fits a classifier to the training data. '''

    # Start the clock, train the classifier, then stop the clock
    start = time()
    f1_scorer = make_scorer(f1_score, pos_label='yes')
    # splits data and runs classifier on mutltiple splits of training data
    scores = cross_val_score(clf, X, y, f1_scorer, cv = 4)
    end = time()

    # log and print the results
    log.info('cross_val_scores: {}'.format(scores))
    print 'cross_val_score', scores

    log.info("Trained model in {:.4f} seconds".format(end - start))
    print "Trained model in {:.4f} seconds".format(end - start)


def predict_labels(clf, features):
    ''' Makes predictions for kaggle's test set
        using a fit classifier based on F1 score.
    '''

    # Start the clock, make predictions, then stop the clock
    start = time()
    pred = clf.predict(features)
    pred_probs = clf.predict_proba(features)
    end = time()

    # Print and return results
    log.info("Made predictions in {:.4f} seconds.".format(end - start))
    print "Made predictions in {:.4f} seconds.".format(end - start)
    return pred, pred_probs


def write_labels(test, pred, pred_probs, write_file):
    """
        combines test data and predictions and writes the labels
    """
    pred_df = pd.DataFrame(predictions)
    pred_probs_df = pd.DataFrame(pred_probs)
    labels = pd.concat([test, pred_df, pred_probs_df], axis=1)
    labels.to_csv(write_file)


if __name__ == "__main__":
    train_file = ''
    data = read_data(train_file)
    y = train['clicked']
    X = train.drop('clicked', axis=1)

    clf = RandomForestClassifer()
    train_classifier(clf, X, y)

    # predict labels for kaggle's test data
    kaggle_test_file = ''
    write_file = 'labeled_test_data.csv'
    kaggle_test_data = read_data(test_file)
    predictions = predict_labels(clf, kaggle_test_data)
    write_labels(kaggle_test_data, predictions, write_file)
