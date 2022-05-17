using CSV, DataFrames, Dates, Statistics, Plots, StatsPlots, PyCall 
using GLM, MLJ, ShapML, MLJModels

include("WranglingUtils/Wrangle.jl")

df = Wrangle.wrangle()

df = Wrangle.onehot_holiday(df)

df_train, df_test = partition(df, 0.7, rng=123)

const ytrain, Xtrain = reshape(df_train.TimeDiff, (nrows(df_train),)), select(df_train, Not([:RequestServiceTime,
																							:StartServiceTime,
																							:StartServiceHr,
																							:StartServiceMin,
																							:StartServiceSec,
																							:TimeDiff]))
const ytest, Xtest = reshape(df_test.TimeDiff, (nrows(df_test),)), select(df_test, Not([:RequestServiceTime,
																						:StartServiceTime,
																						:StartServiceHr,
																						:StartServiceMin,
																						:StartServiceSec,
																						:TimeDiff]))