# Title: Practicum II
# Author: Rupert Simpson, Paula Bass Werner
# Date: Summer Full 2023

library(RMySQL)
library(RSQLite)

host <- "sql9.freemysqlhosting.net"
username <- "sql9637087"
password <- "F5WLHRRTXU"
dbname <- "sql9637087"

mydbcon <- dbConnect(
  MySQL(),
  user = username,
  password = password,
  dbname = dbname,
  host = host
)

litedbcon <- dbConnect(RSQLite::SQLite(), "pharma.db")

prod_query <- "
  SELECT
    saletxn.pID,
    SUM(saletxn.amount) AS total_sold,
    (CAST(strftime('%m', saletxn.date) AS INTEGER) - 1) / 3 + 1 AS quarter,
    strftime('%Y', saletxn.date) AS year,
    reps.territory AS region
  FROM
    saletxn
  JOIN
    reps ON saletxn.rID = reps.rID
  GROUP BY
    saletxn.pID, quarter, year, region;
"

rep_query <- "
  SELECT
    saletxn.rID,
    reps.firstName,
    reps.lastName,
    SUM(saletxn.amount) AS total_sold,
    (CAST(strftime('%m', saletxn.date) AS INTEGER) - 1) / 3 + 1 AS quarter,
    strftime('%Y', saletxn.date) AS year,
    saletxn.pID
  FROM
    saletxn
  JOIN
    reps ON saletxn.rID = reps.rID
  GROUP BY
    saletxn.rID, saletxn.pID, quarter, year;
"

product_facts.df <- dbGetQuery(litedbcon, prod_query)
rep_facts.df <- dbGetQuery(litedbcon, rep_query)

dbWriteTable(mydbcon, "product_facts", product_facts.df, overwrite = T)
dbWriteTable(mydbcon, "rep_facts", rep_facts.df, overwrite = T)

dbDisconnect(mydbcon)
dbDisconnect(litedbcon)