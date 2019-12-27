# boston-crimes-map

*Spark application that aggregates some analytics from Boston crimes data set.*
Data set can be downloaded from [Kaggle](https://www.kaggle.com/AnalyzeBoston/crimes-in-boston).

## How to launch
1. Built uber-jar with `sbt assembly`
2. `spark-submit --master local[*] --class com.github.oguseynov.boston.crimes.map.BostonCrimesMap \
/path/to/jar {path/to/crime.csv} {path/to/offense_codes.csv} {path/to/output_folder}`