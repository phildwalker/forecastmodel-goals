# looking into specific time periods for active members
# Mon Feb 07 14:55:28 2022 ------------------------------

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
with ttc (customerid, firstTC) as (
select CustomerID, min(CreatedOn) as firstTC
from dbo.tbltangerclub
where CustomerID not in (0, -13)
and year(CreatedOn) <= 2021
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

WHERE year(cc.contactDate) >= 2021
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
WHERE year(t.TransactionDate) >= 2021
  	and t.CustomerID in (select CustomerID from ttc)
	--and t.CustomerID in (1004)
	AND t.Status IN ('A','N') --,'I','V','F')
	AND d.Status IN ('A','N') --,'I','V','F')

UNION ALL

Select CustomerID, CreatedOn as ActivDate, concat(TentID, '-', ReceiptAmount) as DetailDesc, 1 as CustAct,'Logged Receipts' AS TransactionHistoryType
		From tblReceiptTracking 
Where year(CreatedOn) >= 2021
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
# ------- Clean and Organize

LastAct <-
  tc_activity %>% 
  mutate(All = "Portfolio",
         Date = as.Date(ActivDate)) %>% 
  filter(Date <= today) #,CustAct == 1

#--------- what txn types are included ---------------


Comb <-
  LastAct %>% 
  mutate(Year = lubridate::year(as.Date(ActivDate))) %>%
  filter(Year != 2022) %>% 
  count(CustomerID,CustAct, Year, TransactionHistoryType) %>% 
  pivot_wider(names_from = TransactionHistoryType, values_from = n) %>% 
  rename(CC = `Customer Contact`,
         TXN= `Regular transactions`) %>% 
  mutate(ActNoRec = ifelse(is.na(`Logged Receipts`) & (!is.na(CC) | !is.na(TXN)), "NoRec_WithAct", "YesRec")) %>% 
  rowwise() %>% 
  mutate(ActivCount = rowSums(across(CC:TXN), na.rm=T)) %>% 
  ungroup()


test <-
  tc_activity %>% 
  filter(CustomerID %in% c(3187))

Comb %>% 
  count(CustAct, ActNoRec)%>% 
  gt::gt()

Comb %>% 
  group_by(ActNoRec) %>% 
  summarise(couCust = n_distinct(CustomerID)) %>% 
  ungroup()


Comb %>% 
  # filter(ActNoRec != "YesRec") %>% 
  mutate(CountGroups = cut(ActivCount, c(0,1,5,10,50, 100, Inf))) %>% 
  filter(is.na(CountGroups))
  count(ActNoRec, CountGroups) %>% 
  ggplot(aes(CountGroups, n, fill=ActNoRec))+
    geom_bar(stat = "identity", position="dodge")+
    geom_text(aes(y=n, label=n), vjust=-.5, position = position_dodge(width = .9))+
  scale_y_continuous(labels = scales::comma)+
  labs(x = "Amount of Activity in the Year",
       y= "Count of Members")+
  scale_fill_manual(values = cb_palette)





