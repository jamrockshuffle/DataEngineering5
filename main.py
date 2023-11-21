import datetime
from pyspark.sql import SparkSession, functions
import glob
import os
import csv
import zipfile
import io

file_names = []
for i in glob.glob("data/**/*.zip", recursive=True):
    # file_names.append(os.path.basename(i))
    file_names.append(i)

JAVA_HOME = "/Users/usr/.jdks/corretto-1.8.0_382"
os.environ["JAVA_HOME"] = JAVA_HOME
spark = SparkSession.builder.appName("Exercise5").enableHiveSupport().getOrCreate()

def zip_to_df(zip_name):
    zip_ref = zipfile.ZipFile(zip_name)
    for name in zip_ref.namelist():
        if not name.startswith("Divvy"):
            continue
        with zip_ref.open(name) as file_contents:
            reader = csv.DictReader(io.TextIOWrapper(file_contents, 'utf-8'), delimiter=',')
            for row in reader:
                with open(zip_name + '.csv', 'w', newline='') as csvfile:
                    writer = csv.DictWriter(csvfile, reader.fieldnames)
                    writer.writeheader()
                    writer.writerows(reader)
            df = spark.read.csv(zip_name + '.csv', header='true')
    return df

def average_trip_length(df, start_time, end_time, filename):
    timeDiff = (functions.unix_timestamp(functions.col(end_time), "yyyy-MM-dd HH:mm:ss") -
                functions.unix_timestamp(functions.col(start_time), "yyyy-MM-dd HH:mm:ss"))/60
    df = df.withColumn("average_trip_length_per_day", timeDiff)
    df = (df.groupBy(functions.to_date(start_time).alias("day"))
          .agg(functions.mean("average_trip_length_per_day").alias("avg trip length per day (minutes)")))

    create_csv(df, "average_trip_length_per_day", "length-", filename)

def number_of_trips_in_a_day(df, start_time, filename):
    df = df.withColumn("date", functions.to_date(start_time))
    df = (df.groupBy('date').count()
          .select(functions.col("date").alias("day"),
                  functions.col("count").alias("trips")))

    create_csv(df, "number_of_trips_per_day", "trips-", filename)


def best_start_station_per_month(df, station_name, start_time, filename):
    df = df.groupBy(functions.month(start_time).alias("month"),
                    functions.year(start_time).alias("year"),
                    functions.col(station_name).alias("station")).agg(functions.count(station_name).alias("trips"))

    df = df.groupBy("month", "year").agg(functions.max_by("station", "trips").alias("best start station"),
                                         functions.max("trips").alias("trips"))

    create_csv(df, "best_station_per_month", "beststat-", filename)


def gender_average_trip_length(df, start_time, end_time, gender, filename):
    timeDiff = (functions.unix_timestamp(functions.col(end_time), "yyyy-MM-dd HH:mm:ss") -
                functions.unix_timestamp(functions.col(start_time), "yyyy-MM-dd HH:mm:ss")) / 60
    df = df.withColumn("average_trip_length_per_day", timeDiff)
    df = df.groupBy(gender).agg(functions.mean("average_trip_length_per_day").alias("avg trip length per gender (minutes)"))
    df = df.na.drop(subset=["gender"])

    create_csv(df, "average_trip_length_per_gender", "avggender-", filename)

def ages_by_trip_lengths(df, start_time, end_time, birthyear, filename):
    timeDiff = (functions.unix_timestamp(functions.col(end_time), "yyyy-MM-dd HH:mm:ss") -
                functions.unix_timestamp(functions.col(start_time), "yyyy-MM-dd HH:mm:ss")) / 60
    ageDiff = functions.year(functions.col(start_time)) - functions.col(birthyear)
    df = df.withColumn("average_trip_length_per_day", timeDiff).withColumn("person_age", ageDiff)
    df = df.na.drop(subset=["person_age"])

    dfMax = (df.orderBy(functions.desc("average_trip_length_per_day")).limit(10)
             .select(functions.col("person_age").alias("age"),
                     functions.col("average_trip_length_per_day").alias("10 longest trips (minutes)")))

    create_csv(dfMax, "longest_and_shortest_trips_by_age", "agelongest-", filename)

    # for some reason, in multiple rows End Time value is earlier than Start Time
    # which results in negative time
    # that is a mistake that was originally presented in the source .csv file
    # it can be removed with the following piece of code
    # .filter(functions.col("average_trip_length_per_day") > 0)
    # I decided to leave it, however
    dfMin = (df.orderBy(functions.asc("average_trip_length_per_day"))
             .limit(10)
             .select(functions.col("person_age").alias("age"),
                     functions.col("average_trip_length_per_day").alias("10 shortest trips (minutes)")))

    create_csv(dfMin, "longest_and_shortest_trips_by_age", "ageshortest-", filename)

def best_three_stations_in_two_weeks(df, station_name, start_time, filename):
    df = df.withColumn("date", functions.to_date(functions.col(start_time)))
    max_date = df.agg(functions.max("date")).collect()[0][0]
    date_before = max_date - datetime.timedelta(days=14)

    max_date_str = max_date.strftime("%Y-%m-%d")
    date_before_str = date_before.strftime("%Y-%m-%d")

    df = (df.filter((functions.col("date") >= date_before))
          .select("date", station_name)
          .sort(functions.asc("date")))

    df = (df.groupBy(functions.col(station_name).alias("station_leaders"))
          .agg(functions.count(station_name)
               .alias("trips_from_" + date_before_str + "_to_" + max_date_str)))

    df = (df.sort(functions
                 .desc("trips_from_" + date_before_str + "_to_" + max_date_str))
          .limit(3))

    create_csv(df, "station_leaders_in_last_two_weeks", "beststat2week-", filename)

def extract_source(df):
    regex_str = "[\/]([^\/]+[\/][^\/]+)$"
    df = df.withColumn("csv", functions.input_file_name())
    df = df.withColumn("csv", functions.regexp_extract("csv", regex_str, 1))
    csv_name = df.first()["csv"]
    return csv_name[5:-8]

def create_csv(df, foldername, reportname, filename):
    with open("reports/" + foldername + "/" + reportname + filename + ".csv", "w", newline='') as f:
        writer = csv.DictWriter(f, fieldnames=df.columns)
        writer.writeheader()
        for row in df.toLocalIterator():
            writer.writerow(row.asDict())

def main():

    # 1st file

    df1 = zip_to_df(file_names[0])

    average_trip_length(df1, "start_time", "end_time", extract_source(df1))
    number_of_trips_in_a_day(df1, "start_time", extract_source(df1))
    best_start_station_per_month(df1, "from_station_name", "start_time", extract_source(df1))
    gender_average_trip_length(df1, "start_time", "end_time", "gender", extract_source(df1))
    ages_by_trip_lengths(df1, "start_time", "end_time", "birthyear", extract_source(df1))
    best_three_stations_in_two_weeks(df1, "from_station_name", "start_time", extract_source(df1))

    # 2nd file

    df2 = zip_to_df(file_names[1])

    average_trip_length(df2, "started_at", "ended_at", extract_source(df2))
    number_of_trips_in_a_day(df2, "started_at", extract_source(df2))
    best_start_station_per_month(df2, "start_station_name", "started_at", extract_source(df2))
    # there is no gender column in 2nd file
    # there is no age column in 2nd file
    best_three_stations_in_two_weeks(df2, "start_station_name", "started_at", extract_source(df2))

    for x in file_names:
        os.remove(x+".csv")

    pass


if __name__ == "__main__":
    main()
