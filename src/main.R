library(sparklyr)
library(tidyr)
library(dplyr)

source("src/Connect.R")
source("src/decol_alma_transform.R")




main <- function() {
  sc <- ConnectToDB("Spark")
  tryCatch({
    
  },
  finally = {
    close(sc)
  })
}




main()