Functional Programming – Laboratory Work 2

Student ID: st84995
Programming Language: Scala
Framework: Apache Spark
Variant: 1

Description

This project demonstrates data processing using Scala and Apache Spark with the Istanbul Stock Exchange dataset.

Completed Tasks

Download the Istanbul Stock Exchange dataset.

Read the dataset into a Spark DataFrame.

Display the DataFrame header.

Display the first five rows.

Sort the DataFrame by date in descending order.

Remove the EM column.

Sort the DataFrame by the highest NIKKEI value.

Create a new column containing the mean of DAX and FTSE.

Create a function that returns the maximum value from a selected numeric column.

Export the processed DataFrame to CSV.

Technologies

Scala 2.13

Apache Spark 4.0.1

Java 17

Scala CLI

Visual Studio Code

Files

Main.scala – Source code for all tasks.

ISTANBUL STOCK EXCHANGE Data Set.csv – Original dataset.

Istanbul_Stock_Exchange_Result.csv – Processed dataset with the calculated mean column.

Running the Project

Run the following command in the project directory:

scala-cli run Main.scala

Result

All ten laboratory tasks were completed successfully. The processed data was exported to a CSV file.