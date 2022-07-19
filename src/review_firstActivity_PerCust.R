# Pulling active member information... on the yearly basis and then splitting by first activity in they year...
# Thu Feb 10 17:42:08 2022 ------------------------------

library(odbc)
library(DBI)
library(glue)
library(tidyverse)
library(lubridate)

theme_set(theme_bw())
cb_palette <- c("#003399", "#ff2b4f", "#3686d3", "#fcab27", "#88298a", "#000000", "#969696", "#008f05", "#FF5733", "#E5F828")

server <- "appsql-prod.database.windows.net"
database = "tangstats"
con <- DBI::dbConnect(odbc::odbc(), 
                      UID = "pdwalker@tangeroutlet.com", # rstudioapi::askForPassword("pdwalker"),
                      Driver="ODBC Driver 17 for SQL Server",
                      Server = server, Database = database,
                      Authentication = "ActiveDirectoryInteractive")

#----- Pull from dashboard data

pull_sql <- glue_sql(
  "
with ttc (customerid, firstTC) as (
select CustomerID, min(CreatedOn) as firstTC
from dbo.tbltangerclub
where CustomerID not in (0, -13)
--and CreatedOn >= '2020-07-01'
and status = 'A'
and DeletedOn is null
group by customerID
)

SELECT cc.CustomerID, 
	cast(cc.ContactDate AS DATE) as ActivDate,
	concat(cct.Name,'-' + cct.Description) AS DetailDesc, --, pro.productname
	cct.IndicatesCustomerActivity as CustAct,
	'Customer Contact' AS TransactionHistoryType
	
FROM tblCustomerContacts cc
left join tblCustomerContactTypes cct on cc.ContactTypeID = cct.CustomerContactTypeID

WHERE year(cc.contactDate) >= 2016
	and cc.PromoID <> 0
	and cc.CustomerID in (select CustomerID from ttc)

UNION ALL 

SELECT t.Customerid, 
cast(t.TransactionDate AS DATE) as ActivDate,
	concat(pro.ProductName, '-' + pro.Description , '||' + d.Description) AS DetailDesc, --, pro.productname
	1 as CustAct,
'Regular transactions' AS TransactionHistoryType
FROM tblTransactions t
	INNER JOIN tblTransactionDetail d ON t.TransactionID = d.TransactionID
	left join tblProducts pro on d.ProductID = pro.ProductID
WHERE year(t.TransactionDate) >= 2016
  	and t.CustomerID in (select CustomerID from ttc)
	--and t.CustomerID in (1004)
	AND t.Status IN ('A','N') --,'I','V','F')
	AND d.Status IN ('A','N') --,'I','V','F')

UNION ALL

Select CustomerID, CreatedOn as ActivDate, concat(TentID, '-', ReceiptAmount) as DetailDesc, 1 as CustAct,'Logged Receipts' AS TransactionHistoryType
		From tblReceiptTracking 
Where year(CreatedOn) >= 2016
	and CustomerID in (select CustomerID from ttc)
	and Status = 'A'
	And DeletedOn is null 
	And TentID != 'TCPTS'
	And TentID is not null 
	And TentID != 'OTHER'
	And ReceiptAmount < 100000

  ",
.con = con)

parm_sql <- dbSendQuery(con, pull_sql)
tc_activity <- dbFetch(parm_sql)

dbClearResult(parm_sql)


save(tc_activity, file = here::here("data", "tc_activity.rds"))
load(file = here::here("data", "tc_activity.rds"))
#------ Clean Receipts -----

CleanTC_Act_Seas <-
  tc_activity %>%
  filter(CustAct == 1) %>% 
  mutate(Date = as.Date(ActivDate),
         Year = as.character(lubridate::year(Date))) %>% 
    filter(Year > 2016,
           Year < 2022) %>% 
  group_by(Year, CustomerID) %>%
  summarise(FirActPerYear = min(Date, na.rm=T),
            CountActPerYear = n()) %>% 
  ungroup()

save(CleanTC_Act_Seas, file = here::here("data", "CleanTC_Act_Seas.rds"))


# ----------

RecptMo <-
  CleanTC_Act_Seas %>% 
  mutate(FirMo = lubridate::floor_date(FirActPerYear, "month")) %>% 
  group_by(Year, FirMo) %>% 
  summarise(CountCusFir = n()) %>% 
  ungroup() %>% 
  mutate(Month = lubridate::month(FirMo), #, label = T, abbr = T
         CountCusFir = as.numeric(CountCusFir))




RecptMo %>% 
  # filter(PurchYear > 2016,
  #        PurchYear < 2022) %>% 
  mutate(Month = lubridate::month(FirMo, label=T, abbr = T)) %>% 
  select(-FirMo) %>% 
  pivot_wider(names_from = Year, values_from = CountCusFir) %>% 
  gt(rowname_col = "Month") %>% 
  fmt_number(columns = 2:6,  decimals = 1,  suffix= T) %>% 
  tab_header(title = "Activity: Count of Unique Customer ID's",
             subtitle = "With at least 1 activity for that year | Counted in the month first activity is logged") %>% 
  summary_rows(
    # groups = TRUE,
    columns = c(2:6), fns = list(total = "sum"),  formatter = fmt_number,
    decimals = 1, suffix= T)#pattern = "{x}M" 









