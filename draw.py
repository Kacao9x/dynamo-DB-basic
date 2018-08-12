# import matplotlib as plt
import matplotlib.pyplot as plt
import numpy as np
import os
import glob
import pandas as pd
from scipy.signal import filtfilt, firwin, upfirdn

# from __future__ import division

path = r"/media/kacao/479E26136B58509D/Titan_AES/Python-script/data/"
file_name = pd.DataFrame()
for filename in glob.glob(os.path.join(path, "*.dat")):
    my_file = open(filename)
    y_str = my_file.read()
    y_str = y_str.splitlines()
    y = []
    for i, num in enumerate(y_str):
        if i < len (y_str) - 1:
            y.append(float(num))
    y = pd.DataFrame(y)
    file_name = pd.concat([file_name, y], axis=1, ignore_index=True)
# with 0s rather than NaNs
file_name = file_name.fillna(0)
print(file_name)
[row, column] = file_name.shape

# fs = 7200000*4
# nyq_rate = fs*0.5
# filterlen = 301
# b = firwin(filterlen, 100000.0/fs, window="hamming", pass_zero=False)
# i = 0
# while i < column:
#     file_name.loc[:, i] = filtfilt(b, 1.0, file_name.loc[:, i])
#     i = i+1
# amp_upsample = pd.DataFrame()
# upsample_rate = 4
# fs = fs*4
# nyq_rate = fs*0.5
# b = firwin(101, 1.0 / upsample_rate)
# i = 0
# while i < column:
#     y = pd.DataFrame(upfirdn(b, file_name.loc[:, i], up=upsample_rate))
#     amp_upsample = pd.concat([amp_upsample, y], axis=1, ignore_index=True)
#     i = i+1
#
# print(amp_upsample.shape)
#
# N = len(amp_upsample.loc[:, 0])
# dt = float(1/fs)
# print (dt)
#
# x = np.arange(0, dt*N, 3.47222222e-8)

i = 1
dt = float(1/7200000)
x = np.arange(0, 3.47222222e-8*row, 3.47222222e-8)

fig = plt.figure()
plt.interactive(False)
while i < column+1:
    #plt.subplot(column/2, 2, i)
    #change the integers inside this routine as (number of rows, number of columns, plotnumber)
    plt.plot(x, file_name.loc[:, i-1])
    plt.xlim((0, 0.00005))
    i = i+1
plt.show()