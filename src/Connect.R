#---------------------------------
# Connect to various database, either here or on the server
#---------------------------------
# DB: Database to connect to
#---------------------------------
# DB Available:
#  -- CST
#  -- Alma
#  -- KMP
#  -- AAR
#  -- LAT
#  -- UGB
#  -- Postgres 
#  -- Impala
#  -- Spark (only available from the server)
#---------------------------------
# For Postgres, you need to manually specify the schema: tbl(con, in_schema("reduction", "mon_his5mn"))
#---------------------------------
ConnectToDB = function(DB = NULL){
  #Check inputs
  stopifnot(!is.null(DB))
  
  #Get OS
  OS = Sys.info()["sysname"] 
  
  #Define connection
  if (OS == "Windows" && DB == "CST")         Con = DBI::dbConnect(odbc::odbc(), dsn = "BDDLRF64", uid = "U_GUERARD",   PWD = "Gu3r@rD!")
  if (OS == "Windows" && DB == "Alma")        Con = DBI::dbConnect(odbc::odbc(), dsn = "ALMA64",   uid = "information", PWD = "courtoisie")
  if (OS == "Windows" && DB == "KMP")         Con = DBI::dbConnect(odbc::odbc(), dsn = "KMP64",    uid = "information", PWD = "courtoisie")    
  if (OS == "Windows" && DB == "AAR")         Con = DBI::dbConnect(odbc::odbc(), dsn = "AAR64",    uid = "information", PWD = "courtoisie")    
  if (OS == "Windows" && DB == "LAT")         Con = DBI::dbConnect(odbc::odbc(), dsn = "LAT64",    uid = "information", PWD = "courtoisie")    
  if (OS == "Windows" && DB == "UGB")         Con = DBI::dbConnect(odbc::odbc(), dsn = "UGB64",    uid = "information", PWD = "courtoisie")
  if (OS == "Windows" && DB == "Postgres")    Con = DBI::dbConnect(odbc::odbc(), dsn = "CRDAPostgres")
  if (OS == "Windows" && DB == "Impala")      Con = DBI::dbConnect(odbc::odbc(), driver = "Cloudera ODBC Driver for Impala", host = "casagzclem1", port = 21050, AuthMech = 6, UID = "", PWD = "", UseNativeQuery = 1, CurrentSchemaRestrictedMetadata=1)
  if (OS == "Windows" && DB == "Spark")       stop("Can't connect to Spark locally.")
  
  if (OS == "Linux"   && DB == "CST")         Con = DBI::dbConnect(odbc::odbc(), Driver = "Oracle", SVC = "bddlrf",   UID = "U_GUERARD",   PWD = "Gu3r@rD!",   Host = "FRSJL25.corp.riotinto.org",      Port = 1521)
  if (OS == "Linux"   && DB == "Alma")        Con = DBI::dbConnect(odbc::odbc(), Driver = "Oracle", SVC = "almostp",  UID = "information", PWD = "courtoisie", Host = "caalmszost.corp.riotinto.org",   Port = 1521)
  if (OS == "Linux"   && DB == "AAR")         Con = DBI::dbConnect(odbc::odbc(), Driver = "Oracle", SVC = "aarostp",  UID = "information", PWD = "courtoisie", Host = "caaarzost.corp.riotinto.org",    Port = 1521)
  if (OS == "Linux"   && DB == "LAT")         Con = DBI::dbConnect(odbc::odbc(), Driver = "Oracle", SVC = "latostp",  UID = "information", PWD = "courtoisie", Host = "latost.corp.riotinto.org",       Port = 1521)
  if (OS == "Linux"   && DB == "UGB")         Con = DBI::dbConnect(odbc::odbc(), Driver = "Oracle", SVC = "UGBOSTP",  UID = "information", PWD = "courtoisie", Host = "CALATSZOST2.corp.riotinto.org",  Port = 1521)      
  if (OS == "Linux"   && DB == "Postgres")    Con = DBI::dbConnect(odbc::odbc(), Driver = "postgresql", Server = "casagaprstd1", Database = "crda_ds", UID = "user_crda", PWD = "crda_ds", Port = "3306")
  if (OS == "Linux"   && DB == "Impala")      Con = DBI::dbConnect(odbc::odbc(), driver = "impala",                   UID = "",            PWD = "",           host = "casagzclem1", port = 21050, AuthMech = 6, UseNativeQuery = 1, CurrentSchemaRestrictedMetadata=1)
  if (OS == "Linux"   && DB == "Spark")       Con = ConnectToSpark()
  
  #For Postgres, Impala or Spark, we're done
  if (DB %in% c("Postgres", "Impala", "Spark"))    return(Con)
  
  #Kitimat kinda sucks on the server for some reason, and we need to try 2 different connections
  if (OS == "Linux"   && DB == "KMP")         Con = tryCatch(expr  =                 DBI::dbConnect(odbc::odbc(), Driver = "Oracle", SVC = "kitrimsp", UID = "information", PWD = "courtoisie", Host = "cakitgpcora1.corp.riotinto.org", Port = 1522),
                                                             error = function(err)   DBI::dbConnect(odbc::odbc(), Driver = "Oracle", SVC = "kitrimsp", UID = "information", PWD = "courtoisie", Host = "cakitgpcora2.corp.riotinto.org", Port = 1522))
  
  #Alter schema
  if (DB == "CST")    dummy <- DBI::dbExecute(Con, "ALTER SESSION SET CURRENT_SCHEMA = CST") 
  if (DB != "CST")    dummy <- DBI::dbExecute(Con, "ALTER SESSION SET CURRENT_SCHEMA = OST")
  
  #Set date format (makes it possible to compare date directly to "yyyy-mm-dd" strings)    
  dummy <- DBI::dbExecute(Con, "ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'")
  
  #Return connection
  return(Con)
}



#---------------------------------
# Spark
#---------------------------------
# I do not understand any of this.
#---------------------------------
ConnectToSpark = function(){
  #Set home
  Sys.setenv(SPARK_HOME = "/usr/lib/spark/")

  #Initialize configuration
  config = spark_config()
  
  #Config
  config$`spark.yarn.am.memory`               = "1g"     #Default 512m. Amount of memory to use for the YARN Application Master in client mode. spark.yarn.am.memory + some overhead should be less than yarn.nodemanager.resource.memory-mb. In cluster mode, use spark.driver.memory instead.
  config$`spark.yarn.am.memoryOverhead`       = "1g"     #Default "AM" memory * 0.10, with minimum of 384. Same as spark.driver.memoryOverhead, but for the YARN Application Master in client mode.
  config$`spark.yarn.am.cores`                = 1        #Default 1. Number of cores to use for the YARN Application Master in client mode. In cluster mode, use spark.driver.cores instead.
  config$`sparklyr.shell.executor-memory`     = "4g"     #Default 1g. Amount of memory to use per executor process.
  config$`sparklyr.shell.executor-cores`      = "1"      #Default 1 in Yarn, all the available cores on the worker in standalone. The number of cores to use on each executor.
  config$`sparklyr.shell.num-executors`       = "1"      #Default 1 in Yarn, all the available cores on the worker in standalone. The number of cores to use on each executor.
  config$`spark.kryoserializer.buffer.max.mb` = "512"
  #config$`hive.exec.dynamic.partition.mode`   = "nonstrict"
  
  #Connect
  sc = spark_connect(master = "yarn", app_name = paste0("sparklyr-", Sys.getenv("USER")), config = config)
}