# Title: Practicum II
# Author: Rupert Simpson & Paula Bass Werner
# Date: Summer Full 2023

# Load required libraries
library(RSQLite)
library(XML)

# Establish database connection
dbcon <- dbConnect(RSQLite::SQLite(), "pharma.db")

# Retrieve XML files
xmlFiles <- list.files("./txn-xml")

# Create data frames
reps.df <- data.frame(rID = integer(),
                      firstName = character(),
                      lastName = character(),
                      territory = character(),
                      stringsAsFactors = F)

products.df <- data.frame(pID = integer(),
                          name = character(),
                          stringsAsFactors = F)

customers.df <- data.frame(cID = integer(),
                           name = character(),
                           stringsAsFactors = F)

saletxn.df <- data.frame(txnID = integer(),
                         rID = integer(),
                         cID = integer(),
                         pID = integer(),
                         quantity = integer(),
                         amount = integer(),
                         country = character(),
                         date = character(),
                         stringsAsFactors = F)

# Initialize synthetic ID values
p.row = 1
c.row = 1
t.row = 1

# Helper function to check if a value exists
check.exists <- function(value, df, column_name) {
  matching_row <- df[df[[column_name]] == value, ]
  
  if (nrow(matching_row) > 0) {
    # If a matching row is found, return the corresponding value
    return(matching_row[[column_name]])
  } else {
    # If no matching row is found, return 0
    return(0)
  }
}

# Helper function to parse the date to the correct format
parseDate <- function(date_to_parse) {
  
  # Changes the date string received from the XML to the expected format 
  date_object <- strptime(date_to_parse, format = "%m/%d/%Y")
  date <- format(date_object, format = "%Y-%m-%d")
  return(date)
}

# Function to parse rep data from XML
parse.reps <- function(file) {
  # Parse XML
  xmlDOM <- xmlParse(paste0("./txn-xml/", file))
  
  # Assign the root of the XML to root
  root <- xmlRoot(xmlDOM)
  
  # Find xml size
  n <- xmlSize(root)
  
  # Iterate through reps XML file
  for (row in 1:n) {
    rep <- root[[row]]
    
    # Extract rep information
    rID <- gsub("^r", "", xpathSApply(rep, "./@rID"))
    firstName <- xpathSApply(rep, "./firstName", xmlValue)
    lastName <- xpathSApply(rep, "./lastName", xmlValue)
    territory <- xpathSApply(rep, "./territory", xmlValue)
    
    # Append rep data to data frame with each iteration
    reps.df[row, "rID"] <- as.integer(rID)
    reps.df[row, "firstName"] <- firstName
    reps.df[row, "lastName"] <- lastName
    reps.df[row, "territory"] <- territory
  }
  return(reps.df)
}

# Iterate through the transaction XML data and save it to data frames
for (file in 1:length(xmlFiles)) {
  # Assign current file
  currentFile <- xmlFiles[file]
  
  # Check for pharma reps file
  if (currentFile == "pharmaReps.xml") {
    # Put reps XML through reps parsing function
    reps.df <- parse.reps(currentFile)
  } else {
    # Parse XML
    xmlDOM <- xmlParse(paste0("./txn-xml/", currentFile))
    
    # Assign the root of the XML to root
    root <- xmlRoot(xmlDOM)
    
    # Find xml size
    n <- xmlSize(root)
    
    # Iterate through nodes of the XML
    for (row in 1:n) {
      txn <- root[[row]]
      
      # Assign values for each piece of data
      date <- parseDate(xpathSApply(txn, "./date", xmlValue))
      cust <- xpathSApply(txn, "./cust", xmlValue)
      prod <- xpathSApply(txn, "./prod", xmlValue)
      qty <- xpathSApply(txn, "./qty", xmlValue)
      amount <- xpathSApply(txn, "./amount", xmlValue)
      country <- xpathSApply(txn, "./country", xmlValue)
      repID <- xpathSApply(txn, "./repID", xmlValue)
      
      # Check if a product exists and if not, append it to the data frame
      if (check.exists(prod, products.df, "name") == 0) {
        products.df[p.row, "pID"] <- as.integer(p.row)
        products.df[p.row, "name"] <- prod
        p.row <- p.row + 1 # Increment p.row when a new product is added
      }
      
      # Check if a product exists and if not, append it to the data frame
      if (check.exists(cust, customers.df, "name") == 0) {
        customers.df[c.row, "cID"] <- as.integer(c.row)
        customers.df[c.row, "name"] <- cust
        c.row <- c.row + 1 # Increment c.row when a new customer is added
      }
      
      # Append to the saletxn data frame with the transaction data
      saletxn.df[t.row, "txnID"] <- as.integer(t.row)
      saletxn.df[t.row, "rID"] <- as.integer(repID)
      saletxn.df[t.row, "cID"] <- customers.df[customers.df$name == cust, "cID"]
      saletxn.df[t.row, "pID"] <- products.df[products.df$name == prod, "pID"]
      saletxn.df[t.row, "quantity"] <- as.integer(qty)
      saletxn.df[t.row, "amount"] <- as.double(amount)
      saletxn.df[t.row, "country"] <- country
      saletxn.df[t.row, "date"] <- date
      t.row <- t.row + 1 # Increment t.row when a new transaction is added
    }
  }
}

# Create tables in SQL database with required key constraints
create_reps_table <- "
  CREATE TABLE IF NOT EXISTS reps (
      rID INT PRIMARY KEY,
      firstName TEXT,
      lastName TEXT,
      territory TEXT
    );"

create_products_table <- "
  CREATE TABLE IF NOT EXISTS products (
    pID INT PRIMARY KEY,
    name TEXT
  );"

create_customers_table <- "
  CREATE TABLE IF NOT EXISTS customers (
    cID INT PRIMARY KEY,
    name TEXT
  );"

create_saletxn_table <- "
  CREATE TABLE IF NOT EXISTS saletxn (
    txnID INT PRIMARY KEY,
    rID INT,
    cID INT,
    pID INT,
    quantity INT,
    amount DOUBLE,
    country TEXT,
    date DATE,
    FOREIGN KEY (rID) REFERENCES reps(rID),
    FOREIGN KEY (cID) REFERENCES cumstomers(cID),
    FOREIGN KEY (pID) REFERENCES products(aid)
  );"

dbExecute(dbcon, create_reps_table)
dbExecute(dbcon, create_products_table)
dbExecute(dbcon, create_customers_table)
dbExecute(dbcon, create_saletxn_table)

# Populate database tables with the data saved to the data frames
# The functions below append to the pre-existing database tables, however
# this can only be run once before it duplicates data.
# dbWriteTable(dbcon, "reps", reps.df, overwrite = F, append = T)
# dbWriteTable(dbcon, "products", products.df, overwrite = F, append = T)
# dbWriteTable(dbcon, "customers", customers.df, overwrite = F, append = T)
# dbWriteTable(dbcon, "saletxn", saletxn.df, overwrite = F, append = T)

# Populate database using the overwrite flag to run the script multiple times
dbWriteTable(dbcon, "reps", reps.df, overwrite = T)
dbWriteTable(dbcon, "products", products.df, overwrite = T)
dbWriteTable(dbcon, "customers", customers.df, overwrite = T)
dbWriteTable(dbcon, "saletxn", saletxn.df, overwrite = T)

#disconnect from database
dbDisconnect(dbcon)