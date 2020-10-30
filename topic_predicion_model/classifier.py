# Important
import pickle
import os

# SK Learn
from sklearn.model_selection import GridSearchCV
from sklearn.pipeline import Pipeline, FeatureUnion
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import TruncatedSVD

# XGBoost separate package
import xgboost as xgb
from xgboost.sklearn import XGBClassifier


class classifier:
    @staticmethod
    def train_classify(df):
        # Tfidf to convert words to a numeric format based on term frequency
        tfidf = TfidfVectorizer(ngram_range=(1, 1), stop_words='english')

        # tsvd for dimensionality reduction
        tsvd = TruncatedSVD(algorithm='randomized', n_components=200)

        # XGBoost classifier
        model = XGBClassifier()
        # model = xgb.XGBRegressor(learning_rate=0.1, subsample=0.8)

        # Generate pipeline with combined features
        clsf = Pipeline([
                ('text', Pipeline([
                    ('tfidf', tfidf),
                    ('svd', tsvd)])),
                ('clf', model),
        ])
        # Train classifier
        clf = GridSearchCV(clsf,
                   {'text__svd__n_components': [100, 200, 300],
                   'clf__max_depth': [4, 6, 8, 10],
                    'clf__n_estimators': [150, 200, 300]}, verbose=1)


        clf.fit(df['text'], df['label'].values)
        print(clf.best_score_)
        print(clf.best_params_)

        # for i in ['mean_test_score', 'std_test_score', 'param_n_estimators']:
        #     print(i," : ",grid.cv_results_[i])
        return clf
        


    @staticmethod
    def save_model(name, model):
        model_path = "./model"
        if not os.path.exists(model_path):
            os.makedirs(model_path)
        # Save models as pickle files
        pickle.dump(model, open(model_path + '/' + name + '.pickle', 'wb'))

    @staticmethod
    def read_model(path):
        clf = pickle.load(open(path, 'rb'))
        return clf