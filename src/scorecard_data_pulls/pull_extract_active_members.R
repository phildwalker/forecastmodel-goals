# pulling down the active member data
# Tue Jan 11 12:05:38 2022 ------------------------------

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
# today <- as.Date("2022-01-10")
# Lck_MaxMo <- lubridate::floor_date(today, "month")-1
# Lck_MinYR <- lubridate::floor_date(Lck_MaxMo, "year")
# LY_Lck_MinYR <- Lck_MinYR %m+% years(-3)
# 
# #------
# OldestDate <- LY_Lck_MinYR %m+% months(-18)
# OldestDate

# -------- Pulling max transaaction


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
	concat(pro.ProductName, '-' + pro.Description) AS DetailDesc, --, pro.productname
	1 as CustAct,
'Regular transactions' AS TransactionHistoryType
FROM tblTransactions t
	INNER JOIN tblTransactionDetail d ON t.TransactionID = d.TransactionID
	INNER join tblProducts pro on d.ProductID = pro.ProductID
WHERE year(t.TransactionDate) >= 2016
	and t.CustomerID in (select CustomerID from ttc)
	--select CustomerID from ttc)
	AND t.Status IN ('A','N') --,'I','V','F')
	AND d.Status IN ('A','N') --,'I','V','F')


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
  filter(Date <= today,
         CustAct == 1)


KeptTypes <-
  LastAct %>% 
  count(DetailDesc, TransactionHistoryType, sort = T)

#------ Build Comps ----  
# DateGroup = "Wk"

BuildComp <- function(GroupCol, DateGroup){
  col_name <- enquo(GroupCol)
  sel_var <- as.name(DateGroup)
  
  DtList <- c("CY", "LY", "LY2")
  df_total = data.frame()
  
  for (Dt in DtList) {
    
    dlk <- datesLk %>% mutate(Start = as.Date(Start)%m+% months(-18), End = as.Date(End) ) %>% filter(Type %in% DateGroup, DateOrder == Dt)
    
    dat <- 
      LastAct %>% 
      filter(Date >= as.Date(dlk$Start) &  
               Date <= as.Date(dlk$End))  %>% 
      bind_cols(., dlk) %>% 
      mutate(Year = as.character(Year)) %>% 
      group_by(Type, Year,DateOrder, AggregGrouping = !!col_name) %>% #!!col_name
      summarise(gross = n_distinct(CustomerID),
                MinDate = min(Date),
                MaxDate = max(Date)) %>% 
      ungroup()
    
    df_total <- rbind(df_total,dat) 
  }

  df_total %>% 
  mutate(Group = as.character(glue::glue("[{MinDate} - {MaxDate}]"))) %>% 
    select(-MinDate, -MaxDate, -Type, dates=Group) %>% #, "{{sel_var}}_gross" := gross, "{{sel_var}}_dates" := Group
    arrange(AggregGrouping, Year) %>% 
    mutate(Type = DateGroup) %>% 
    pivot_wider(names_from = DateOrder, values_from= c(gross, dates, Year)) %>% 
    mutate(PerComp_CY_LY = (gross_CY/gross_LY)-1,
           PerComp_CY_LY2 = (gross_CY/gross_LY2)-1) 

}


# BuildComp(GroupCol = All, DateGroup = "Wk")





#---- Build datasets -------

results <-
  bind_rows(
    # -- last week
    BuildComp(GroupCol = All, DateGroup = "Wk"),
    # -- mtd
    # BuildComp(GroupCol = All, DateGroup = "MTD"),
    # -- ytd
    # BuildComp(GroupCol = All, DateGroup = "YTD"),
    # -- certif mtd
    BuildComp(GroupCol = All, DateGroup = "Lck-MTD"),
    # -- certif mtd
    BuildComp(GroupCol = All, DateGroup = "Lck-YTD"),
  ) %>% 
  mutate(AggregGrouping = "actMem")

results %>% 
  mutate(lookup= as.character(glue::glue("{AggregGrouping}|{Type}"))) %>% 
  relocate(lookup) %>% 
  write_csv(., file = here::here("output", "ActivMem_Scorecard_Results.csv"))


# bind_rows(BuildCompAll(GroupCol = All, StartDate = maxWk, EndDate = minWk, LY_Start = LY_maxWk, LY_End = LY_minWk, TypeName = "Wk"),
#           BuildCompAll(GroupCol = All, StartDate = Lck_MaxMo, EndDate = Lck_MinMo, LY_Start = LY_Lck_MaxMo, LY_End = LY_Lck_MinMo, TypeName = "LcK_MTD"), 
#           BuildCompAll(GroupCol = All, StartDate = Lck_MaxYR, EndDate = Lck_MinYR, LY_Start = LY_Lck_MaxYR, LY_End = LY_Lck_MinYR, TypeName = "Lck_YTD"))  %>% 
#   write_csv(., file = here::here("output", "TC_ActivityCompar_All.csv"))








