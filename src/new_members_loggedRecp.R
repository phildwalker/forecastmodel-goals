# pulling new members who logged a receipt
# Mon Feb 07 16:08:47 2022 ------------------------------

library(DBI)
library(glue)
library(tidyverse)
library(lubridate)

datesLk <- readxl::read_excel(here::here("input", "DateLookUp.xlsx"))

theme_set(theme_bw())
cb_palette <- c("#003399", "#ff2b4f", "#3686d3", "#fcab27", "#88298a", "#000000", "#969696", "#008f05", "#FF5733", "#E5F828")

server <- "appsql-prod.database.windows.net"
database = "tangstats"
con <- DBI::dbConnect(odbc::odbc(), 
                      UID = "pdwalker@tangeroutlet.com", # rstudioapi::askForPassword("pdwalker"),
                      Driver="ODBC Driver 17 for SQL Server",
                      Server = server, Database = database,
                      Authentication = "ActiveDirectoryInteractive")

#----- Date Selection -------

today <- Sys.Date()

# -------- Pulling max transaaction
pull_sql <- glue_sql(
  "
With tc_new_members as (
	Select t.CustomerID, t.CreatedBy, t.CenterID, t.FromWeb, td.TransactionDetailID, td.TransactionID, td.ProductID, 
	 td.ForCenterID, td.Quantity, td.Price, td.Status, cast(td.CreatedOn as date) as NewMemberDate, td.ModifiedOn, cast(t.TransactionDate as date)  as TransactionDate
	From tblTransactionDetail td 
	Inner Join tblTransactions t 
		On td.TransactionID = t.TransactionID
	Where td.ProductID = 1  --New TC Membership
		And td.DeletedOn is null
		and year(td.CreatedOn) >= 2019
		And td.Status in ('A', 'N')
		And t.Status in ('A', 'N')
),

receipts as (
select customerid, CreatedOn as RcptLogDate, concat(TentID, '-', ReceiptAmount) as DetailDesc
from tblReceiptTracking
Where year(CreatedOn) >= 2016
	and Status = 'A'
	and CustomerID in (select CustomerID from tc_new_members)
	And DeletedOn is null 
	And TentID != 'TCPTS'
	And TentID is not null 
	And TentID != 'OTHER'
	And ReceiptAmount < 100000
)


Select tc.*, rt.RcptLogDate, rt.DetailDesc
	from tc_new_members tc 
	left join receipts rt on tc.customerid = rt.CustomerID

  ",
.con = con)

parm_sql <- dbSendQuery(con, pull_sql)
newMem_logged <- dbFetch(parm_sql)

dbClearResult(parm_sql)

save(newMem_logged, file = here::here("data", "newMem_logged.rds"))

#-------------

newMem_firstRecp <-
  newMem_logged %>% 
  group_by(CustomerID, NewMemberDate) %>% 
  filter(RcptLogDate == min(RcptLogDate)) %>% 
  mutate(Rec_NewMem_diff = RcptLogDate- NewMemberDate)

#---------------------------


pull_sql <- glue_sql(
  "
select customerid, CreatedOn as RcptLogDate, concat(TentID, '-', ReceiptAmount) as DetailDesc
from tblReceiptTracking
Where year(CreatedOn) >= 2020
	and Status = 'A'
	And DeletedOn is null 
	And TentID != 'TCPTS'
	And TentID is not null 
	And TentID != 'OTHER'
	And ReceiptAmount < 10000


  ",
.con = con)

parm_sql <- dbSendQuery(con, pull_sql)
receipts <- dbFetch(parm_sql)

dbClearResult(parm_sql)

# save(newMem_logged, file = here::here("data", "newMem_logged.rds"))


#-------------
rec <-
  receipts %>% 
  mutate(RecpYear = lubridate::year(as.Date(RcptLogDate))) %>%
  filter(RecpYear != 2022) %>% 
  count(customerid, RecpYear) %>% 
  pivot_wider(names_from = RecpYear, values_from = n) %>% 
  mutate(Recp2020 = ifelse(is.na(`2020`), "No_2020", "Yes_2020"),
         Recp2021 = ifelse(is.na(`2020`) & !is.na(`2021`), "Yes2021_No2020", 
                           ifelse(!is.na(`2020`) & !is.na(`2021`), "Yes2021_Yes2020", "Only2020")))


rec %>% 
  count(Recp2020) %>% 
  gt::gt()






#---------------------------


pull_sql <- glue_sql(
  "
with ttc (customerid, firstTC) as (
select CustomerID, min(CreatedOn) as firstTC
from dbo.tbltangerclub
where CustomerID not in (0, -13)
and year(CreatedOn) = 2021
and status = 'A'
and DeletedOn is null
group by customerID
)

select customerid, CreatedOn as RcptLogDate, concat(TentID, '-', ReceiptAmount) as DetailDesc
from tblReceiptTracking
Where year(CreatedOn) = 2021
	and customerID in (select CustomerID from ttc)
	and Status = 'A'
	And DeletedOn is null 
	And TentID != 'TCPTS'
	And TentID is not null 
	And TentID != 'OTHER'
	And ReceiptAmount < 10000

  ",
.con = con)

parm_sql <- dbSendQuery(con, pull_sql)
Mem2021_logged2021 <- dbFetch(parm_sql)

dbClearResult(parm_sql)


SameYear <-
  Mem2021_logged2021 %>% 
  count(customerid)

SameYear %>% 
  count()











