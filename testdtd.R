library(XML)

xmlDOM <- xmlParse("./txn-xml/pharmaReps.xml", validate = T)

print(xmlDOM)