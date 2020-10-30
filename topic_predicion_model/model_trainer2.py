from classifier import classifier as clsf
import numpy as np
import os
import pandas as pd
import pickle
import shutil

from sklearn import metrics


DATA_PATH='../news_train.csv'


def main(unused_argv):

    # Prepare training and testing data
    df = pd.read_csv(DATA_PATH).fillna(' ')
    train_df = df.sample(frac=8)
    test_df = df.drop(train_df.index)

    classifier= clsf.train_classify(df)

    prediction = clsf.predict(test_df['text'])

    Accuracy = (prediction == test_df['label'].values).sum()/len(prediction)
    print('Accuracy: {0:f}'.format(score))

    clsf.save_model("best_model", classifier)     

if __name__ == '__main__':
    main()