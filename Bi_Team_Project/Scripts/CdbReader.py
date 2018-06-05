# coding=utf-8
import pandas as pd
read_cdb = pd.read_csv("C://BiUiGit//data//mapping//commission//cdbIoDetails.csv")
print(read_cdb[["IO Id","Currency Exchange Rate","Currency Type"]])