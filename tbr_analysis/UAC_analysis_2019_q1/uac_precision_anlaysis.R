
#' @title Source Data for  Market Analysis
#'
#' @details
#' This analysis was performed by analyzing GH New Diners, DAGs, and New Visitor Session Information
#'
#' - For test market in the input dataset, find the best control markets using time series matching. THIS IS NOT CURRENTLY REFLECTED IN THIS DOC BUT SHOULD BE IN THE NEXT VERSION
#'
#' - Given a test market and a matching control marketS (from above) and test market specific data, analyze the causal impact of an intervention
#'
#' The package utilizes the dtw package in CRAN to do the time series matching, and the CausalImpact package to do the inference.
#' (Created by Kay Brodersen at Google). For more information about the CausualImpact package, see the following reference:
#'
#' CausalImpact version 1.0.3, Brodersen et al., Annals of Applied Statistics (2015). http://google.github.io/CausalImpact/
#'
#' The Zoo Creation source file has two separate functions to perform the tasks described above:
#'
#' - zoo_creation.nds_dags(): Creates zoo DFs for NDs or DAGS or both. Typeical usage is calling zoo_creation.nds_dags(c('dags','nds'), '<modified cbsa name of test city>')
#'
#' - zoo_creation.session():  Creates zoo DFs for session_count, session_orders,searched_and_ordered_sessions, conversion_rate. Typical call is zoo_creation.session('all','Washington-Arlington-Alexandria DC-VA-MD-WV' ), but can also specificy specific metric desired.
#'        -This data is built off  https://stash.grubhub.com/projects/DATA/repos/etl_tasks/browse/tasks_etl2/finance_reporting/new_diner_exit_rate.sql
#'         so limited to data from past 240 Days.
#'
#' For more details, check out the vignette: browseVignettes("MarketMatching")
#' @author Jackson Mayberry (jmayerry at grubhub.com)
#' @keywords time warping, time series matching, causal impact, zoo
#' @docType package
#' @description
#' For a given test market find the best matching control markets using time series matching and analyze the impact of an intervention.
#' The intervention could be be a marketing event or some other local business tactic that is being tested.
#' The package utilizes dynamic time warping to do the matching and the CausalImpact package to analyze the causal impact.
#'
#'
#'
#'
#'
#'
#' @name MarketMatching
#' 
#' 
#' 
#' 
#'


install.packages("devtools")
library(devtools)
install_github("google/GeoexperimentsResearch")
library(GeoexperimentsResearch)


#### Do not run: begin  ####
# loads the PostgreSQL driver
drv = dbDriver("PostgreSQL")
# creates a connection to the postgres database
# note that "con" will be used later in each connection to the database
your_user<- "jmayberry"
supersecret <- "Hello1234"
con <- dbConnect(drv, dbname = "grubhub",
                 host = "grubhub.cxossxssj4ln.us-east-1.redshift.amazonaws.com", port = "5439",
                 user = your_user , password = supersecret)
uac_install_information_by_day <- read.csv(file="/Users/jmayberry/Documents/GitHub/tbr_analysis/UAC_analysis_2019_q1/input_data/uac_preanalysis_input_spend_data_.csv", header=TRUE, sep=",")
colnames(uac_install_information_by_day)<-c('row_num','date_index','location','city','state','zip_code','campaign','conversions','cost_conv','conv_rate','clicks','cost')
c=rs_upsert_table(uac_install_information_by_day, dbcon=con, access_key = 'AKIAIOOGLNOVO7VJ4WYQ', secret_key = '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', table_name = 'marketing_research.uac_install_information', split_files=12,region ='us-east-1',bucket = 'gh-database-marketing/tmp_files/jmayberry')
#### Do not run: end





app_install_raw_information<- dbGetQuery(con,"WITH installs AS
  (SELECT date(event_time) install_date,
                                         CASE
                                         WHEN length(postal_code) = 4 THEN '0' + postal_code
                                         ELSE postal_code
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
                                         AND date(event_time) >= '2018-07-01'
                                         GROUP BY 1,
                                         2,
                                         3) ,
                                         install_information_wide AS
                                         ( SELECT install_date, zipcode, (CASE WHEN media_source = 'UAC' THEN installs ELSE 0 END) AS uac_installs, (CASE WHEN media_source = 'Organic' THEN installs ELSE 0 END) organic_installs,
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
                                         FROM marketing_research.uac_install_information a
                                         INNER JOIN zipcode_summary b ON ((CASE WHEN length(trim(zip_code::varchar)) = 4 THEN '0' + zip_code::varchar WHEN length(trim(zip_code::varchar)) = 3 THEN '00' + zip_code::varchar ELSE zip_code::varchar END)::varchar) =b.zipcode
                                         WHERE dma_description IS NOT NULL) AS geos,
                                         list_of_all_dates_ever
                                         WHERE date > '2018-09-30'
                                         AND date < '2018-12-10'),
                                         uac_data AS
                                         ( SELECT dma_description,
                                         to_date(c.date_index, 'MM/DD/YYYY'),
                                         coalesce(sum(c.conversions),0) AS conversions,
                                         coalesce(sum(c.clicks),0) clicks,
                                         coalesce(sum(c.cost),0) AS cost
                                         FROM marketing_research.uac_install_information c
                                         INNER JOIN zipcode_summary ON c.zip_code=zipcode_summary.zipcode
                                         WHERE dma_description IS NOT NULL
                                         GROUP BY 1,
                                         2),
                                         install_data AS
                                         ( SELECT date(install_date) AS install_date,
                                         dma_description,
                                         coalesce(SUM(uac_installs),0) AS uac_install,
                                         coalesce(SUM(organic_installs),0) AS organic_install,
                                         coalesce(SUM(total_installs),0) AS total_install
                                         FROM install_information_wide
                                         INNER JOIN zipcode_summary ON install_information_wide.zipcode=zipcode_summary.zipcode
                                         GROUP BY 1,
                                         2)
                                         SELECT DISTINCT base.date AS date,
                                         coalesce(trim(base.dma_description), 'no geo') AS geo,
                                         coalesce(sum(uac_data.conversions),0) AS conversions,
                                         coalesce(sum(uac_data.clicks),0) clicks,
                                         coalesce(sum(uac_data.cost),0) AS cost,
                                         coalesce(SUM(install_data.uac_install),0) AS uac_install,
                                         coalesce(SUM(install_data.organic_install),0) AS organic_install,
                                         coalesce(SUM(install_data.total_install),0) AS total_install
                                         FROM base_table AS base
                                         LEFT JOIN uac_data ON base.date = uac_data.to_date
                                         AND base.dma_description = uac_data.dma_description
                                         LEFT JOIN install_data ON base.date = install_data.install_date
                                         AND base.dma_description = install_data.dma_description
                                         WHERE len(base.dma_description) > 1
                                         GROUP BY 1,
                                         2
                                         ORDER BY 2,
                                         4 ASC")

c=rs_create_table(app_install_raw_information, dbcon=con, access_key = 'AKIAIOOGLNOVO7VJ4WYQ', secret_key = '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', table_name = 'marketing_research.uac_install_information_test', split_files=12,region ='us-east-1',bucket = 'gh-database-marketing/tmp_files/jmayberry')








obj.gts<-GeoTimeseries(app_install_raw_information, metrics=c("cost",'total_install'))

obj.geo.strata<-ExtractGeoStrata(obj.gts, volume ="total_install", n.groups = 2)

obj.geo.strata<-Randomize(obj.geo.strata)

obj.ga<-GeoAssignment(obj.geo.strata)

obj.pre <-DoROASPreanalysis(obj.gts, response="total_install", geos = obj.ga, prop.to="cost", period.lengths = c(35,20,5),  n.sims = 1000)

?DoROASPreanalysis()

results<-summary(obj.pre, level=.90, type = "one-sided", precision = .1)

results
#https://github.com/google/GeoexperimentsResearch/blob/master/R/doroaspreanalysis.R





obj_dags.gts<-GeoTimeseries(all_data_dags.subset, metrics = c("total_daily_spend", "order_count"))
obj.per<- ExperimentPeriods(c("2018-10-10", "2018-12-01", "2018-12-23", "2018-12-31"))
geoassignment<-unique(all_data_dags.subset[,c('geo.group','cbsa')])
geoassignment$geo <- geoassignment$cbsa
obj_dags.ga<-GeoAssignment(geoassignment)

##create GTS Object
obj_dags.no_cool_down <- GeoExperimentData(obj_dags.gts,
                                           periods = obj_dags.per,
                                           geo.assignment = obj_dags.ga)

