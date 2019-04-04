


## INPUT DATA LOAD FROM GOOGLE 
library(dplyr)
library(xlsx)
suppressMessages(library(data.table))
suppressMessages(library(RANN))
library(GeoexperimentsResearch)
library(tidyverse)
library(broom)
install.packages("RPostgreSQL")
library(RPostgreSQL)
library(DBI)
library(tidyverse)
install.packages("googlesheets")
library(googlesheets)
install.packages("lubridate")
library(lubridate)
library("redshiftTools", lib.loc="/Library/Frameworks/R.framework/Versions/3.5/Resources/library")

(my_sheets <- gs_ls())
my_sheets %>% glimpse()


#### LOAD MAPPING TABLE FROM GOOGLE INTO OUR REDSHIFT SYSTEM ### 

###Connect to Google Sheets API 
##create list of all sheets and corresponding keys
(my_sheets <- gs_ls())
my_sheets %>% glimpse()
##identifty sheet key for Sheet Implementation Details for Grubhub UAC GeoX Experiment
#conver list to tbl class
tbl.my_sheets<-dplyr::tbl_df(my_sheets)
sheet_key<-dplyr::filter(tbl.my_sheets, sheet_title == 'Grubhub Report Spend + Impressions')$sheet_key
ss<-gs_key(sheet_key)
#Read in Sheet Values to DF 
##sheet key for copy of order spendss<-gs_key(sheet_key)
###get mapping data ###
gma_to_dma_mapping<-data.frame(gs_read(ss, ws = 3))
colnames(gma_to_dma_mapping)<-c("grubhub_dma_name","google_dma_id","google_dma_name","test_group_id","group_designation","grubhub_geo_2")

###get spend data ###
uac_install_information<-data.frame(gs_read(ss, ws = 2))
colnames(uac_install_information)<-c("day", "google_dma_id","uac_spend","impressions", "test_group_id","group_designation")

## Get Spend Data version: 3/19. Spend split by UAC and UACe
sheet_key<-dplyr::filter(tbl.my_sheets, sheet_title == 'Grubhub Report Spend + Impressions v02')$sheet_key
ss<-gs_key(sheet_key)
#UACe
uace_spend_information<-data.frame(gs_read(ss, ws = 1))
colnames(uace_spend_information)<-c("date","google_dma_id","uace_spend","uace_impression","group_number","group_designation", "grubhub_dma_name")
#convert dates 
uace_spend_information$date<-dmy(uace_spend_information$date)

#UAC
uac_spend_information<-data.frame(gs_read(ss, ws = 2))
colnames(uac_spend_information)<-c("date","google_dma_id","uac_spend","uac_impression","group_number","group_designation", "grubhub_dma_name")
#convert dates 
#uac_spend_information$date<-mdy(uac_spend_information$date)


# creates a connection to the postgres database
# note that "con" will be used later in each connection to the database
drv = dbDriver("PostgreSQL")
your_user<- 
supersecret <- 
con <- dbConnect(drv, dbname = "grubhub",
                 host = , port = "5439",
                 user = your_user , password = supersecret)


#try(rs_create_statement(results.raw.series,  table_name = 'marketing_research.twin_market_series_results4' ))
#try(rs_create_table(results.raw.series, dbcon=con, access_key = 'AKIAIOOGLNOVO7VJ4WYQ', secret_key = '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', table_name = 'marketing_research.twin_market_series_results4', split_files=4,region ='us-east-1',bucket = 'gh-database-marketing/tmp_files/jmayberry'))
#try(rs_create_table(gma_to_dma_mapping, dbcon=con, access_key = 'AKIAIOOGLNOVO7VJ4WYQ', secret_key = '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', table_name = 'marketing_research.uac_gma_dma_mapping', split_files=1,region ='us-east-1',bucket = 'gh-database-marketing/tmp_files/jmayberry'))
#try(rs_create_table(uac_install_information, dbcon=con, access_key = 'AKIAIOOGLNOVO7VJ4WYQ', secret_key = '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', table_name = 'marketing_research.uac_install_spend_info', split_files=1,region ='us-east-1',bucket = 'gh-database-marketing/tmp_files/jmayberry'))
try(rs_create_table(uace_spend_information, dbcon=con, access_key = 'AKIAIOOGLNOVO7VJ4WYQ', secret_key = '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', table_name = 'marketing_research.uace_spend_info_3_19', split_files=1,region ='us-east-1',bucket = 'gh-database-marketing/tmp_files/jmayberry'))
try(rs_create_table(uac_spend_information, dbcon=con, access_key = 'AKIAIOOGLNOVO7VJ4WYQ', secret_key = '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', table_name = 'marketing_research.uac_spend_info_3_19', split_files=1,region ='us-east-1',bucket = 'gh-database-marketing/tmp_files/jmayberry'))

grant.sql<- dbGetQuery(con,"GRANT SELECT ON marketing_research.uace_spend_info_3_19 TO GROUP powerusers, GROUP readonly;")
grant.sql<- dbGetQuery(con,"GRANT SELECT ON marketing_research.uac_spend_info_3_19 TO GROUP powerusers, GROUP readonly;")




