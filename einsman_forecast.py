import os
import numpy as np
import pandas as pd

import sys
from keras.models import Sequential
from keras.layers import Dense
from sklearn import linear_model
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.metrics import precision_score, recall_score
from sklearn.model_selection import StratifiedKFold, train_test_split

feature_imp = []
precision_ann = []
precision_tree = []
precision_lreg = []
recall_ann = []
recall_tree = []
recall_lreg = []

precision_ann_var = []
precision_tree_var = []
precision_lreg_var = []
recall_ann_var = []
recall_tree_var = []
recall_lreg_var = []

mape = []
mse = []

precision_ann = []
precision_tree = []
precision_lreg = []
recall_ann = []
recall_tree = []
recall_lreg = []
assesedlines = []
def model(dataset):
    try:
        n_items = 7
        dataset = dataset.dropna()  # dropnans to allow good learning
        del dataset['Unnamed: 0']
        del dataset['Time (CET)']
        dataset.drop(columns = ['Balance_ImpExp_DEPL','Balance_ImpExp_DEDK',
                                'Balance_ImpExp_DECZ','Day-ahead Total Load Forecast [MW] - Germany (DE)',
                                'Scheduled Generation [MW] (D) - CTA|DE(50Hertz)'], inplace = True)
        # setting up numpy array
        dataset = dataset.values
        X = dataset[:, 0:n_items]
        Y = dataset[:, -1]
        np.random.seed(8)
        splits = 10
        kfold = StratifiedKFold(n_splits=splits, shuffle=True, random_state=8)
        X_remain, X_test, Y_remain, Y_test = train_test_split(X, Y, test_size=0.3)

        s = 0
        for train, test in kfold.split(X_remain, Y_remain):
            s = s + 1
            clf = linear_model.LogisticRegression(C=1e5, solver='lbfgs')
            clf.fit(X[train], Y[train])
            featureimp = ExtraTreesClassifier()
            featureimp.fit(X[train], Y[train])
            feature_imp.append(featureimp.feature_importances_)

            print('+ Starting neural net +')
            '''
            model = Sequential()
            model.add(Dense(100, input_dim=n_items, activation='relu', kernel_initializer='normal', use_bias=True))
            model.add(Dense(50, activation='relu', kernel_initializer='normal'))
            model.add(Dense(25, activation='relu', kernel_initializer='normal'))
            model.add(Dense(1, activation='sigmoid', kernel_initializer='normal'))
            model.compile(loss='binary_crossentropy', optimizer='adam')

            model.fit(X[train], Y[train], epochs=100, batch_size=25, validation_data=(X[test], Y[test]),
                             verbose=0)

            Y_pred = model.predict_classes(X_test)
            '''
            Y_pred1 = clf.predict(X_test)
            Y_pred3 = featureimp.predict(X_test)

            #precision_ann.append(precision_score(Y_test, Y_pred).astype(float))
            #recall_ann.append(recall_score(Y_test, Y_pred).astype(float))
            precision_lreg.append(precision_score(Y_test, Y_pred1).astype(float))
            recall_lreg.append(recall_score(Y_test, Y_pred1).astype(float))
            precision_tree.append(precision_score(Y_test, Y_pred3).astype(float))
            print(precision_tree)
            recall_tree.append(recall_score(Y_test, Y_pred3).astype(float))
            print(recall_tree)
        #precision_ann_var.append(np.std(precision_ann[0:10 - 1], ddof = 1))
        #recall_ann_var.append(np.std(recall_ann[0:10 - 1], ddof = 1))
        precision_lreg_var.append(np.std(precision_lreg[0:10 - 1], ddof = 1))
        recall_lreg_var.append(np.std(recall_lreg[0:10 - 1], ddof = 1))
        precision_tree_var.append(np.std(precision_tree[0:10 - 1], ddof = 1))
        recall_tree_var.append(np.std(recall_tree[0:10 - 1], ddof = 1))

        # print(interim2-interim)
    except (IndexError, ValueError) as e:
        print('Check', sys.exc_info()[0])

    print('Feature Importance',np.mean(feature_imp, axis = 0))
    #print('ANN recall/precision', np.mean(recall_ann), np.mean(precision_ann))
    #print('ANN Mean of Var K-Fold', np.mean(recall_ann_var), np.mean(precision_ann_var))
    print('ExtraTree recall/precision', np.mean(recall_tree), np.mean(precision_tree))
    print('ExtraTree Mean of Var K-Fold', np.mean(recall_tree_var), np.mean(precision_tree_var))