
#' @title Source Data for  Market Analysis
#'
#' @details
#' This analysis was performed by analyzing Install Information vs UAC and UACe infomration 
#'
#' 
#'
#' - Given a test market and a matching control marketS (from above) and test market specific data, analyze the causal impact of an intervention
#'
#' The experiment is conducted using the GeoexperimentsResearch CRAN, but originally sourced from Google to do the time series inference.
#'
#' CausalImpact version 1.0.3, Brodersen et al., Annals of Applied Statistics (2015). https://github.com/google/GeoexperimentsResearch/blob/master/vignettes/GeoexperimentsResearch-vignette.Rmd
#'
#'Inputs: 
#' Spend data provided by Katie Newton 
#' Install Infomration provided by Grubhub team
#' 
#' Test Set Up:
#'12.5% Holdout
#' 
#'
#' For more details, check out the vignette: browseVignettes("MarketMatching")
#' @author Jackson Mayberry (jmayerry at grubhub.com)
#' @keywords uac,uace,google, tbr, geoexperiments, geo, casual,infernce, time series
#' @docType package
#' @description
#'
#' @name MarketMatching
#'


#################### Library Loads ####################
library("Matching")
library("pacman")
#library("kableExtra")
library("knitr")
library("MatchIt")
library("tidyr")
library("RPostgreSQL")
library("DBI")
library("dplyr")
library("ggplot2")
library("CausalImpact")
library("grid")
library(zoo)
library("bsts")
library("tidyverse")
library("reshape")
library("gtrendsR")
library(devtools)
library(MarketMatching)
library(RPresto)
library(googlesheets)
library("aws.s3")
library(RPostgres)
library(redshiftTools)
library("DBI")
library(GeoexperimentsResearch)

#################### Redshift Connection Logic###################
# loads the PostgreSQL driver

sqlQuery <- function (query) {
  
  drv = dbDriver("PostgreSQL")
  your_user<- ""
  supersecret <- ""
  DB <- dbConnect(drv, dbname = "grubhub",
                  host = "", port = "5439",
                  user = your_user , password = supersecret)
  # creating DB connection object with RMysql package
  
  # close db connection after function call exits
  on.exit(dbDisconnect(DB))
  
  # send Query to btain result set
  rs <- dbSendQuery(DB, query)
  
  # get elements from result sets and convert to dataframe
  result <- fetch(rs, -1)
  
  # return the dataframe
  return(result)
}



#### query for install and spend infoamton ######
raw_input_information <-  sqlQuery("create temp table test4 as (WITH installs AS                                        (SELECT date(event_time) install_date,
                                         CASE
                                         WHEN length(postal_code) = 4 THEN '0' + postal_code::varchar
                                         WHEN length(postal_code) = 3 THEN '00' + postal_code
                                         ELSE postal_code::varchar
                                         END AS zipcode,
                                         CASE
                                         WHEN ((media_source = ''
                                         AND channel ~* 'UAC|Youtube'
                                         AND campaign !~* 'engagement')
                                         OR (media_source ~* 'googleadwords'
                                         AND channel ~* 'UAC|Youtube'
                                         AND campaign !~* 'engagement')
                                         OR (media_source ~* 'googleadwords'
                                         AND channel =''
                                         AND campaign !~* 'engagement'))THEN 'UAC'
                                         ELSE 'Organic'
                                         END media_source,
                                         count(1) installs
                                         FROM marketing_growth_reporting.appsflyer_installs
                                         WHERE (((media_source = ''
                                         AND channel ~* 'UAC|Youtube'
                                         AND campaign !~* 'engagement')
                                         OR (media_source ~* 'googleadwords'
                                         AND channel ~* 'UAC|Youtube'
                                         AND campaign !~* 'engagement')
                                         OR (media_source ~* 'googleadwords'
                                         AND channel =''
                                         AND campaign !~* 'engagement'))
                                         OR media_source='organic')
                                         AND date(event_time) >= '2018-09-01'
                                         and lower(app_name) ~* 'grubhub'
                                         GROUP BY 1,
                                         2,
                                         3) ,
                                         install_information_wide AS
                                         ( SELECT install_date, zipcode::varchar as zipcode, (CASE WHEN media_source = 'UAC' THEN installs ELSE 0 END) AS uac_installs, (CASE WHEN media_source = 'Organic' THEN installs ELSE 0 END) organic_installs,
                                         (sum(installs)) AS total_installs
                                         FROM installs
                                         GROUP BY 1,
                                         2,
                                         3,
                                         4),
                                         base_table AS
                                         ( SELECT DISTINCT date, dma_description
                                         FROM
                                         (SELECT DISTINCT dma_description
                                         FROM installs a
                                         INNER JOIN zipcode_summary b ON a.zipcode::varchar=b.zipcode::varchar
                                         WHERE dma_description IS NOT NULL) AS geos,
                                         list_of_all_dates_ever
                                         WHERE date > '2018-08-31'
                                         AND date < CURRENT_DATE),
                                         
                                         /*    uac_data AS
                                         ( SELECT dma_description,
                                         to_date(c.install_date, 'MM/DD/YYYY'),
                                         coalesce(sum(c.conversions),0) AS conversions,
                                         coalesce(sum(c.clicks),0) clicks,
                                         coalesce(sum(c.cost),0) AS cost
                                         FROM install_information_wide c
                                         INNER JOIN zipcode_summary ON c.zipcode::Varchar=zipcode_summary.zipcode::varchar
                                         WHERE dma_description IS NOT NULL
                                         GROUP BY 1,
                                         2),*/
                                         
                                         install_data AS
                                         ( SELECT date(install_date) AS install_date,
                                         dma_description,
                                         coalesce(SUM(uac_installs),0) AS uac_install,
                                         coalesce(SUM(organic_installs),0) AS organic_install,
                                         coalesce(SUM(total_installs),0) AS total_install
                                         FROM install_information_wide
                                         INNER JOIN zipcode_summary ON install_information_wide.zipcode::varchar=zipcode_summary.zipcode::varchar
                                         GROUP BY 1,
                                         2)
                                         SELECT DISTINCT base.date AS date,
                                         coalesce(trim(base.dma_description), 'no geo') AS geo,
                                         -- coalesce(sum(uac_data.conversions),0) AS conversions,
                                         --  coalesce(sum(uac_data.clicks),0) clicks,
                                         --coalesce(sum(uac_data.cost),0) AS cost,
                                         coalesce(SUM(install_data.uac_install),0) AS uac_install,
                                         coalesce(SUM(install_data.organic_install),0) AS organic_install,
                                         coalesce(SUM(install_data.total_install),0) AS total_install                                                                
                                         FROM base_table AS base
                                         --LEFT JOIN uac_data ON base.date = uac_data.to_date
                                         --AND base.dma_description = uac_data.dma_description
                                         LEFT JOIN install_data ON base.date = install_data.install_date
                                         AND base.dma_description = install_data.dma_description
                                         WHERE len(base.dma_description) > 1
                                         GROUP BY 1,
                                         2
                                         
                                         ORDER BY 1 ASC);
                                         
                                         
                                         select test4.*, spend.google_dma_id, spend.uac_spend,spend.uac_impression,spend.group_number,spend.group_designation, grubhub_dma_name
                                         from test4 inner join marketing_research.uac_spend_info_3_19 as spend 
                                         on test4.geo = spend.grubhub_dma_name
                                         and test4.date = spend.date
                                         where test4.geo in (select distinct grubhub_dma_name from  marketing_research.uac_spend_info_3_19 where  grubhub_dma_name !='New York NY');")
geo_assignment_data<-  sqlQuery(" select distinct grubhub_geo_2 as geo, group_designation, test_group_id from marketing_research.uac_gma_dma_mapping where test_group_id != 'Excluded' and grubhub_geo_2 not in (/*'Greenwood-Greenville MS', 'San Angelo TX', 'Mankato MN', 'Glendive Mount',*/ 'New York NY');")
                                   
###FILTRATION ####
##dates to exclude 
dates_to_exclude<-c("2018-10-31","2018-11-22")                                 
raw_input_information.date_filter<-dplyr::filter(raw_input_information,! date %in% dates_to_exclude )
raw_input_information.date_filter$uac_spend <- as.numeric(gsub('\\$|,', '', raw_input_information.date_filter$uac_spend))


##Geo Mapping ##
# create GEOASSIGNMENT ojbect
#setting grubhub dma name to geo and test_group_id to geo.group
colnames(geo_assignment_data)<-c('geo', 'group_designation', 'geo.group')
#convert geo.group field to integer 
geo_assignment_data$geo.group<-as.integer(geo_assignment_data$geo.group)
#create geo assignment object assigment 
obj.ga <- GeoAssignment(geo_assignment_data)


## Time Periods## 
# create OBJPERIOD objects -> bifuricate analysis based on dates. 
obj.per.jm <- ExperimentPeriods(c("2018-09-01", "2019-02-24", "2019-03-04", "2019-03-30"))
#@Kaite: Why exclude 2018 December and January/February? 
obj.per.google_dates<- ExperimentPeriods(c("2018-09-01", "2018-12-01","2019-03-04", "2019-03-26"))



### GTS Object  ## 
obj.gts <- GeoTimeseries(raw_input_information.date_filter, metrics=c("total_install", "uac_spend"))
obj.google_dates <- GeoExperimentData(obj.gts,
                         periods=obj.per.google_dates,
                         geo.assignment=obj.ga)
obj.jm_dates <- GeoExperimentData(obj.gts,
                                      periods=obj.per.jm,
                                      geo.assignment=obj.ga)

### run TBR Analysis #
obj.tbr.roas.google_dates <- DoTBRROASAnalysis(obj.google_dates, response='total_install', cost='uac_spend',
                                  model='tbr1',
                                  pretest.period=0,
                                  intervention.period=2,
                                  cooldown.period=NULL,
                                  control.group= c(1,2,3,5,6,7,8),
                                  treatment.group= 4)


### run analysis 
obj.tbr.roas.jm_dates <- DoTBRROASAnalysis(obj.jm_dates, response='total_install', cost='uac_spend',
                                               model='tbr1',
                                               pretest.period=0,
                                               intervention.period=2,
                                               cooldown.period=NULL,
                                               control.group= c(1,2,3,5,6,7,8),
                                               treatment.group= 4)

wri

summary(obj.tbr.roas.google_dates, level=0.95, interval.type="two-sided")

summary(obj.tbr.roas.jm_dates, level=0.95, interval.type="two-sided")




obj.tbr.roas <- DoTBRROASAnalysis(obj, response='total_install', cost='uace_spend',
                                  model='tbr1',
                                  pretest.period=0,
                                  intervention.period=1,
                                  cooldown.period=NULL,
                                  control.group= c(1,2,3,5,6,7,8),
                                  treatment.group= 4)
obj.tbr <- DoTBRAnalysis(obj, response='total_install',
                         model='tbr1',
                         pretest.period=0,
                         intervention.period=1,
                         cooldown.period=NULL,
                         control.group= c(1,2,3,5,6,7,8),
                         treatment.group= 4)
                         
                                   
                                   
