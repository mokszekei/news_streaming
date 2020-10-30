from classifier import classifier as clsf
import numpy as np
import os
import pandas as pd
import pickle
import shutil

from sklearn import metrics


REMOVE_PREVIOUS_MODEL = True
MODEL_OUTPUT_DIR = '../model/'
DATA_SET_FILE='../news_train.csv'

STEPS = 200

def main(unused_argv):
    if REMOVE_PREVIOUS_MODEL:
        # Remove old model
        shutil.rmtree(MODEL_OUTPUT_DIR)
        os.mkdir(MODEL_OUTPUT_DIR)

    # Prepare training and testing data
    df = pd.read_csv(DATA_SET_FILE).fillna(' ')
    train_df = df.sample(frac=8)
    test_df = df.drop(train_df.index)

    classifier= clsf.train_classify(df)

    prediction = clsf.predict(test_df['text'])

    Accuracy = (prediction == test_df['label'].values).sum()/len(prediction)
    print('Accuracy: {0:f}'.format(score))

    clsf.save_model("best_model", classifier)     

if __name__ == '__main__':
    main()