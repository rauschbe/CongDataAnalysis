import os
import numpy as np
import pandas as pd

import sys
from keras.models import Sequential
from keras.layers import Dense
from sklearn import linear_model
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.metrics import precision_recall_fscore_support, precision_score, recall_score
from sklearn.model_selection import StratifiedKFold


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
        n_items = 12
        dataset = dataset.dropna()  # dropnans to allow good learning
        del dataset['Time (CET)']
        # setting up numpy array
        dataset = dataset.values
        X = dataset[:, 0:n_items]
        Y = dataset[:, -1]
        np.random.seed(8)
        splits = 10
        kfold = StratifiedKFold(n_splits=splits, shuffle=True, random_state=8)
        cvscores = []

        s = 0
        for train, test in kfold.split(X, Y):
            s = s + 1
            clf = linear_model.LogisticRegression(C=1e5, solver='lbfgs')
            clf.fit(X[train], Y[train])
            featureimp = ExtraTreesClassifier()
            featureimp.fit(X[train], Y[train])

            print('+ Starting neural net +')
            model = Sequential()
            model.add(Dense(100, input_dim=n_items, activation='relu', kernel_initializer='normal', use_bias=True))
            model.add(Dense(50, activation='relu', kernel_initializer='normal'))
            model.add(Dense(25, activation='relu', kernel_initializer='normal'))
            model.add(Dense(1, activation='sigmoid', kernel_initializer='normal'))
            model.compile(loss='binary_crossentropy', optimizer='adam')

            hist = model.fit(X[train], Y[train], epochs=100, batch_size=25, validation_data=(X[test], Y[test]),
                             verbose=0)

            Y_pred = model.predict_classes(X[test])

            Y_pred1 = clf.predict(X[test])
            Y_pred3 = featureimp.predict(X[test])

            precision_ann.append(precision_score(Y[test], Y_pred).astype(float))
            recall_ann.append(recall_score(Y[test], Y_pred).astype(float))
            precision_lreg.append(precision_score(Y[test], Y_pred1).astype(float))
            recall_lreg.append(recall_score(Y[test], Y_pred1).astype(float))
            precision_tree.append(precision_score(Y[test], Y_pred3).astype(float))
            recall_tree.append(recall_score(Y[test], Y_pred3).astype(float))

        precision_ann_var.append(np.std(precision_ann[0:10 - 1]))
        recall_ann_var.append(np.std(recall_ann[0:10 - 1]))
        precision_lreg_var.append(np.std(precision_lreg[0:10 - 1]))
        recall_lreg_var.append(np.std(recall_lreg[0:10 - 1]))
        precision_tree_var.append(np.std(precision_tree[0:10 - 1]))
        recall_tree_var.append(np.std(recall_tree[0:10 - 1]))

        # print(interim2-interim)
    except (IndexError, ValueError) as e:
        print('Check', sys.exc_info()[0])

        # valids = np.logical_or.reduce(featureimpv != 0, axis=2)
        # print(np.mean(featureimpv[valids],axis = 0))


    set = np.column_stack((assesedlines, precision_ann))
    set = np.column_stack((set, recall_ann))
    set = np.column_stack((set, precision_lreg))
    set = np.column_stack((set, recall_lreg))
    set = np.column_stack((set, precision_tree))
    set = np.column_stack((set, recall_tree))
    col_names = ['Leitung', 'Precision_ANN', 'Recall_ANN', 'Precision_LReg', 'Recall_LReg',
                 'Precision_Tree', 'Recall_Tree']
    os.chdir('/Users/benni/Desktop/Uni/Paper/KFoldValues')
    set = pd.DataFrame(set, columns=col_names)
    set.to_csv('KFoldValuesLeitungen2.csv', index=False)

    # print('Cross Validation Score',np.mean(CVScore[valids],axis=0))


    # CVSscore_mean = np.mean(CVSscore[valids], axis = 0)


    print('ANN', np.mean(recall_ann_var), np.mean(precision_ann_var))
    print('Logistical Regression', np.mean(recall_lreg_var), np.mean(precision_lreg_var))
    print('ExtraTree', np.mean(recall_tree_var), np.mean(precision_tree_var))