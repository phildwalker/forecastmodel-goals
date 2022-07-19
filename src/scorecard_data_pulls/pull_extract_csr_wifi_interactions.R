# Pull and combine the wifi and csr interactions
# Wed Jan 12 14:57:41 2022 ------------------------------

library(odbc)
library(DBI)
library(glue)
library(tidyverse)
library(lubridate)

datesLk <- readxl::read_excel(here::here("input", "DateLookUp.xlsx"))

server <- "appsql-prod.database.windows.net"
database = "tangstats"
con <- DBI::dbConnect(odbc::odbc(), 
                      UID = "pdwalker@tangeroutlet.com", # rstudioapi::askForPassword("pdwalker"),
                      Driver="ODBC Driver 17 for SQL Server",
                      Server = server, Database = database,
                      Authentication = "ActiveDirectoryInteractive")


theme_set(theme_bw())
cb_palette <- c("#003399", "#ff2b4f", "#3686d3", "#fcab27", "#88298a", "#000000", "#969696", "#008f05", "#FF5733", "#E5F828")


# --------- pulling wifi counts

pull_sql <- glue_sql(
  "
SELECT cc.CustomerID,  - cc.CustomerContactID AS TransactionID,
	- cc.CustomerContactID AS TransactionDetailID,
	CONVERT(DATETIME, CONVERT(CHAR(8), cc.ContactDate, 112) + ' ' + CONVERT(CHAR(8), cc.CreatedOn, 108)) TransactionDate,
	cc.CreatedOn,c.CenterName,
	'A' AS Status,
	coalesce(p.Description, '')  AS DetailDesc,
	'Customer Contact' AS TransactionHistoryType,
	cc.PromoID
	--, cc.Description as promo_desc
FROM tblCustomerContacts cc
INNER JOIN tblCustomerContactTypes cct ON cc.ContactTypeID = cct.CustomerContactTypeID
INNER JOIN tblPromos p ON cc.PromoID = p.PromoID
LEFT JOIN tblcenters c ON cc.centerid = c.centerid

WHERE year(cc.contactDate) >= 2019
	and cc.PromoID <> 0
and cc.promoid in ('23610', '24865') --wifi landing page opt-in
  ",
.con = con)

parm_sql <- dbSendQuery(con, pull_sql)
wifi_pull <- dbFetch(parm_sql)

dbClearResult(parm_sql)
save(wifi_pull, file = here::here("data", "wifi_pull.rds"))

# -------- Pulling CSR interactions

pull_sql <- glue_sql(
  "
SELECT t.Customerid, d.TransactionID,
	d.TransactionDetailID,
	CONVERT(DATETIME, CONVERT(CHAR(8), t.TransactionDate, 112) + ' ' + CONVERT(CHAR(8), t.CreatedOn, 108)) TransactionDate,
	t.CreatedOn,
	c.CenterName, lg.[FirstName] + ' ' + lg.[LastName] as CSRname, lg.Title
	,d.Status,
		coalesce(r.Description, d.Description, pro.productname + '-' + isnull(pro.description, '')) AS DetailDesc, --, pro.productname
'Regular transactions' AS TransactionHistoryType
FROM tblTransactions t
	left JOIN tblTransactionDetail d ON t.TransactionID = d.TransactionID
	LEFT JOIN tblcenters c ON t.centerid = c.centerid
	LEFT JOIN tblTransactionDetailRecipients tdr ON d.TransactionDetailID = tdr.TransactionDetailID
	left join tblProducts pro on d.ProductID = pro.ProductID
	LEFT JOIN tblPromos r ON d.PromoID = r.PromoID
	left join tblLogins lg on t.CreatedBy = lg.LoginID
WHERE year(t.TransactionDate) >= 2019
--and t.customerid in (select customerid from ttc)
	AND t.Status IN ('A','N','I','V','F')
	AND d.Status IN ('A','N','I','V','F')
	--and customerid not in (0, -13)
	--and customerid in ('19577725')
order by transactiondate desc

  ",
.con = con)

parm_sql <- dbSendQuery(con, pull_sql)
csr_activity <- dbFetch(parm_sql)

dbClearResult(parm_sql)
save(csr_activity, file = here::here("data", "csr_activity.rds"))

#---------- 

wifi <- 
  wifi_pull %>%
  filter(TransactionDate <= Sys.time()) %>% 
  mutate(Date = as.Date(TransactionDate)) %>% 
  filter(!CenterName %in% c("Bromont", "Corporate", "Jeffersonville", "Nags Head", "No Center Assigned", "Ocean City",
                            "Park City", "Terrell", "Williamsburg", "Saint Sauveur")) %>% 
  count(Date) %>% 
  mutate(All = "Portfolio")



#---------- 

csr <- 
  csr_activity %>%
  filter(TransactionDate <= Sys.time()) %>% 
  mutate(UserType = case_when(Title %in% c("Web Service", "Junior Software Engineer", "TC Fund Loader Agent", 
                                           "PC Tech", "Tier 1 PC Tech","Director of Inst Trng & D", "IT", "Tech",
                                           "Software Engineer", "QA TEMP", "Network Admin", "Temp Dev", "QA Contractor",
                                           "Socai Media") ~ "Web-Tech",
                              is.na(Title) ~ "Web-Tech",
                              TRUE ~ "Human"),
         Date = as.Date(TransactionDate)) %>% 
  filter(!CenterName %in% c("Bromont", "Corporate", "Jeffersonville", "Nags Head", "No Center Assigned", "Ocean City",
                            "Park City", "Terrell", "Williamsburg", "Saint Sauveur")) %>% 
  count(UserType, Date) %>% 
  filter(UserType == "Human") %>% 
  mutate(All = "Portfolio")


#------ Build Comp function ----------

BuildCompCSR <- function(GroupCol, DateGroup){
  col_name <- enquo(GroupCol)
  sel_var <- as.name(DateGroup)
  
  dat <- 
    csr %>% 
    fuzzyjoin::fuzzy_left_join(.,
                               datesLk %>% mutate(Start = as.Date(Start), End = as.Date(End)) %>% filter(Type %in% DateGroup),
                               by = c("Date" = "Start",
                                      "Date" = "End"),
                               match_fun = list(`>=`, `<=`)) %>% 
    filter(!is.na(Year)) %>% 
    mutate(Year = as.character(Year)) %>% 
    group_by(Type, Year,DateOrder, AggregGrouping = !!col_name) %>% 
    summarise(gross = sum(n, na.rm=T),
              MinDate = min(Date),
              MaxDate = max(Date)) %>% 
    ungroup() %>% 
    mutate(Group = as.character(glue::glue("[{MinDate} - {MaxDate}]"))) %>% 
    select(-MinDate, -MaxDate, -Type, dates=Group) %>% #, "{{sel_var}}_gross" := gross, "{{sel_var}}_dates" := Group
    arrange(AggregGrouping, Year) %>% 
    mutate(Type = DateGroup) %>% 
    pivot_wider(names_from = DateOrder, values_from= c(gross, dates, Year)) %>% 
    mutate(PerComp_CY_LY = (gross_CY/gross_LY)-1,
           PerComp_CY_LY2 = (gross_CY/gross_LY2)-1)
  
  
  return(dat)
} 

BuildCompwifi <- function(GroupCol, DateGroup){
  col_name <- enquo(GroupCol)
  sel_var <- as.name(DateGroup)
  
  dat <- 
    wifi %>% 
    fuzzyjoin::fuzzy_left_join(.,
                               datesLk %>% mutate(Start = as.Date(Start), End = as.Date(End)) %>% filter(Type %in% DateGroup),
                               by = c("Date" = "Start",
                                      "Date" = "End"),
                               match_fun = list(`>=`, `<=`)) %>% 
    filter(!is.na(Year)) %>% 
    mutate(Year = as.character(Year)) %>% 
    group_by(Type, Year,DateOrder, AggregGrouping = !!col_name) %>% 
    summarise(gross = sum(n, na.rm=T),
              MinDate = min(Date),
              MaxDate = max(Date)) %>% 
    ungroup() %>% 
    mutate(Group = as.character(glue::glue("[{MinDate} - {MaxDate}]"))) %>% 
    select(-MinDate, -MaxDate, -Type, dates=Group) %>% #, "{{sel_var}}_gross" := gross, "{{sel_var}}_dates" := Group
    arrange(AggregGrouping, Year) %>% 
    mutate(Type = DateGroup) %>% 
    pivot_wider(names_from = DateOrder, values_from= c(gross, dates, Year)) %>% 
    mutate(PerComp_CY_LY = (gross_CY/gross_LY)-1,
           PerComp_CY_LY2 = (gross_CY/gross_LY2)-1)
  
  
  return(dat)
} 

# ------------------

resultsCSR <-
  bind_rows(
    # -- last week
    BuildCompCSR(GroupCol = All, DateGroup = "Wk"),
    # -- mtd
    BuildCompCSR(GroupCol = All, DateGroup = "MTD"),
    # -- ytd
    BuildCompCSR(GroupCol = All, DateGroup = "YTD"),
    # -- certif mtd
    BuildCompCSR(GroupCol = All, DateGroup = "Lck-MTD"),
    # -- certif mtd
    BuildCompCSR(GroupCol = All, DateGroup = "Lck-YTD"),
  ) %>% 
  mutate(AggregGrouping = "CSR")

resultsWifi <-
  bind_rows(
    # -- last week
    BuildCompwifi(GroupCol = All, DateGroup = "Wk"),
    # -- mtd
    BuildCompwifi(GroupCol = All, DateGroup = "MTD"),
    # -- ytd
    BuildCompwifi(GroupCol = All, DateGroup = "YTD"),
    # -- certif mtd
    BuildCompwifi(GroupCol = All, DateGroup = "Lck-MTD"),
    # -- certif mtd
    BuildCompwifi(GroupCol = All, DateGroup = "Lck-YTD"),
  ) %>% 
  mutate(AggregGrouping = "wifi")

results <- bind_rows(resultsCSR, resultsWifi)


results %>% 
  mutate(lookup= as.character(glue::glue("{AggregGrouping}|{Type}"))) %>% 
  relocate(lookup) %>% 
  write_csv(., file = here::here("output", "wifi_CSR_Scorecard_Results.csv"))


















