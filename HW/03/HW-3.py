import pandas as pd
import numpy as np
import math, sys, pickle
import scipy.misc as smp
from sklearn.preprocessing import MinMaxScaler
import matplotlib.pyplot as plt


def printProgress(iteration, total, prefix='', suffix='', decimals=1, barLength=100):
    '''

    :param iteration: Required  : current iteration (Int)
    :param total: Required  : total iterations (Int)
    :param prefix: Optional  : prefix string (Str)
    :param suffix: Optional  : suffix string (Str)
    :param decimals: Optional  : positive number of decimals in percent complete (Int)
    :param barLength: Optional  : character length of bar (Int)
    :return:
    '''

    formatStr = "{0:." + str(decimals) + "f}"
    percents = formatStr.format(100 * (iteration / float(total)))
    filledLength = int(round(barLength * iteration / float(total)))
    bar = '=' * filledLength + '-' * (barLength - filledLength)
    sys.stdout.write('\r%s |%s| %s%s %s' % (prefix, bar, percents, '%', suffix)),
    if iteration == total:
        sys.stdout.write('\n')
    sys.stdout.flush()

observations = pd.read_csv('mnist_data.txt', header=None, sep=' ')
labels = pd.read_csv('mnist_labels.txt', header=None)

labels.loc[labels[0]!=8,0]=0
labels.loc[labels[0]==8,0]=1

n_features = len(observations.columns)
n_observations = len(observations)

def train(runId, eta, stepInterval):
    w=pd.Series([0]*nFeatures)
    convergence=0
    # eta=0.5
    delta=0

    iteration=1


    while iteration<50:
        gradientVector = pd.Series([0]*nFeatures)
        for index in range(nObservations):
            try:
                p = 1/(1+np.exp(-1*(observations.loc[index].dot(w))))
            except OverflowError:
                p=0
            error = labels.loc[index,0]-p
            gradientVector=gradientVector.add(observations.loc[index].multiply(error))
        w=w.add(gradientVector.multiply(eta*np.exp(-iteration/stepInterval)))
    
        printProgress(iteration, 50, prefix='Training :', suffix='Iteration '+str(iteration), barLength=50)
        iteration+=1

    with open('lr_weight_vector_'+runId+'.pkl','wb') as outFile:
        pickle.dump(w,outFile)

def test(runId):
    with open('lr_weight_vector_'+runId+'.pkl','rb') as inFile:
        w=pickle.load(inFile)
    correctPredictions=0.0
    prediction=1
    for index in range(nObservations):
        try:
            p = 1/(1+np.exp(-1*(observations.loc[index].dot(w))))
        except OverflowError:
            p=0
        if p>=0.5: prediction=1
        else: prediction=0
        if labels.loc[index,0]-prediction==0: correctPredictions+=1
        
    print('Run ID "'+runId+'" : '+str(correctPredictions/nObservations))
    
def paintWeightVector(runId, imageInterpolationType='nearest'):
    with open('lr_weight_vector_'+runId+'.pkl','rb') as inFile:
        w=pickle.load(inFile)
    w=w.multiply(-1)
    w=w.values.reshape(-1,1)
    
    scaler = MinMaxScaler(feature_range=(0,255), copy=True)
    wScaled = scaler.fit_transform(w) 
    
    pixelMap = wScaled.reshape((28,28))

    fig, ax =plt.subplots(figsize=(2,2))
    ax.imshow(pixelMap, cmap='gray', interpolation=imageInterpolationType)

    plt.savefig('weightVector_pixelMap_'+imageInterpolationType+'.png')
    plt.show()

train_new('eta0.1_step50', 0.1, 50)
test('eta0.1_step50')
paintWeightVector('eta0.1_step50', 'nearest')
paintWeightVector('eta0.1_step50', 'bilinear')

