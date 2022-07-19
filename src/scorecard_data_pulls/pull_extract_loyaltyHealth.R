# pulling in the loyalty health portion of the data
# Tue Jan 18 10:25:23 2022 ------------------------------

library(DBI)
library(glue)
library(tidyverse)
library(lubridate)

datesLk <- readxl::read_excel(here::here("input", "DateLookUp.xlsx"))

theme_set(theme_bw())
cb_palette <- c("#003399", "#ff2b4f", "#3686d3", "#fcab27", "#88298a", "#000000", "#969696", "#008f05", "#FF5733", "#E5F828")

today <- Sys.Date()
# today <- as.Date("2022-01-10")



server <- "appsql-prod.database.windows.net"
database = "tangstats"
con <- DBI::dbConnect(odbc::odbc(), 
                      UID = "pdwalker@tangeroutlet.com", # rstudioapi::askForPassword("pdwalker"),
                      Driver="ODBC Driver 17 for SQL Server",
                      Server = server, Database = database,
                      Authentication = "ActiveDirectoryInteractive")


#----- pull data: new members
pull_sql <- glue_sql(
  "
Select cust.CustomerID, cust.CreatedOn as CustCreationDT
	From tblCustomers cust
where cust.CreatedBy <> 2620
order by cust.CustomerID
  ",
.con = con)

parm_sql <- dbSendQuery(con, pull_sql)
newCust <- dbFetch(parm_sql)

dbClearResult(parm_sql)

# save(newCust, file = here::here("data", "newCust.rds"))


#-----------

pull_sql <- glue_sql(
  "
With tc_new_members (TC_CustID, CreatedBy, CenterID, FromWeb, TransactionDetailID, TransactionID, ProductID, ForCenterID, 
Quantity, Price, Status, CreatedOn, ModifiedOn, TransactionDate) as 
(
	Select t.CustomerID, t.CreatedBy, t.CenterID, t.FromWeb, td.TransactionDetailID, td.TransactionID, td.ProductID, 
	 td.ForCenterID, td.Quantity, td.Price, td.Status, cast(td.CreatedOn as date), td.ModifiedOn, cast(t.TransactionDate as date)  
	From tblTransactionDetail td 
	Inner Join tblTransactions t 
		On td.TransactionID = t.TransactionID
	Where td.ProductID = 1  --New TC Membership
		And td.DeletedOn is null
		And td.Status in ('A', 'N')
		And t.Status in ('A', 'N')
)

select *
from tc_new_members

  ",
.con = con)

parm_sql <- dbSendQuery(con, pull_sql)
NewMem <- dbFetch(parm_sql)

dbClearResult(parm_sql)


#----- Join datasets together ---------

CustMem <- 
  full_join(newCust,
            NewMem,
            by = c("CustomerID" = "TC_CustID")) %>% 
  mutate(All = "Portfolio",
         Date = as.Date(CustCreationDT),
         TCDate = as.Date(TransactionDate),
         TCorInsid = ifelse(is.na(TransactionID), "Insider", "TC"),
         TC_whenJoin = ifelse(as.Date(CustCreationDT) == as.Date(CreatedOn), "StartTC", "StartInsider"),
         WebCent = ifelse(FromWeb == T, "Web", "Center")) %>%
  ungroup()

# unique(CustMem$TC_whenJoin)
# unique(CustMem$FromWeb)

# dlk <- datesLk %>% mutate(Start = as.Date(Start), End = as.Date(End) ) %>% filter(Type %in% c("Lck-YTD"), DateOrder == "CY")
# 
# 
# dat <- 
#   CustMem %>% 
#   filter(TCDate >= as.Date(dlk$Start) &  
#            TCDate <= as.Date(dlk$End)) %>% 
#   bind_cols(., dlk) %>% 
#   mutate(Year = as.character(Year)) %>% 
#   group_by(Type, Year,DateOrder, AggregGrouping = !!col_name) %>% #!!col_name
#   summarise(gross = n_distinct(CustomerID, na.rm=T),
#             MinDate = min(TCDate),
#             MaxDate = max(TCDate)) %>% 
#   ungroup()


#------ Build Comp function ----------

BuildCompLoyal <- function(GroupCol, DateGroup, DateFilt, DataName){
  col_name <- enquo(GroupCol)
  sel_var <- as.name(DateGroup)
  date_name <- enquo(DateFilt)
  
  DtList <- c("CY", "LY", "LY2")
  df_total = data.frame()
  
  for (Dt in DtList) {
    
    dlk <- datesLk %>% mutate(Start = as.Date(Start), End = as.Date(End) ) %>% filter(Type %in% DateGroup, DateOrder == Dt)
    
    dat <- 
      CustMem %>% 
      filter(., !!date_name >= as.Date(dlk$Start) &  
               !!date_name <= as.Date(dlk$End)) %>% 
      bind_cols(., dlk) %>% 
      mutate(Year = as.character(Year)) %>% 
      group_by(Type, Year,DateOrder, AggregGrouping = !!col_name) %>% #!!col_name
      summarise(gross = n(), #n_distinct(CustomerID, na.rm=T),
                  MinDate = min(!!date_name),
                  MaxDate = max(!!date_name)) %>% 
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
           PerComp_CY_LY2 = (gross_CY/gross_LY2)-1) %>% 
    mutate(TypeName = DataName) %>% 
    relocate(TypeName) %>% 
    # filter(!is.na(AggregGrouping)) %>% 
    ungroup()
  
}



# BuildCompLoyal(GroupCol = All, DateGroup = "Lck-YTD", DateFilt = TCDate, DataName = "NewMem")
# BuildCompLoyal(GroupCol = WebCent, DateGroup = "Lck-YTD", DateFilt = TCDate, DataName = "NewMem")

# ------- Build dataset -----------





resultsAll <-
  bind_rows(
    # -- last week
    BuildCompLoyal(GroupCol = All, DateGroup = "Wk", DateFilt = TCDate, DataName = "NewMem"),
    BuildCompLoyal(GroupCol = WebCent, DateGroup = "Wk", DateFilt = TCDate, DataName = "NewMem"),
    BuildCompLoyal(GroupCol = All, DateGroup = "Wk", DateFilt = Date, DataName = "NewCust"),
    BuildCompLoyal(GroupCol = TCorInsid, DateGroup = "Wk", DateFilt = Date, DataName = "NewCust"),
    # -- mtd
    BuildCompLoyal(GroupCol = All, DateGroup = "MTD", DateFilt = TCDate, DataName = "NewMem"),
    BuildCompLoyal(GroupCol = WebCent, DateGroup = "MTD", DateFilt = TCDate, DataName = "NewMem"),
    BuildCompLoyal(GroupCol = All, DateGroup = "MTD", DateFilt = Date, DataName = "NewCust"),
    BuildCompLoyal(GroupCol = TCorInsid, DateGroup = "MTD", DateFilt = Date, DataName = "NewCust"),

    # -- ytd
    BuildCompLoyal(GroupCol = All, DateGroup = "YTD", DateFilt = TCDate, DataName = "NewMem"),
    BuildCompLoyal(GroupCol = WebCent, DateGroup = "YTD", DateFilt = TCDate, DataName = "NewMem"),
    BuildCompLoyal(GroupCol = All, DateGroup = "YTD", DateFilt = Date, DataName = "NewCust"),
    BuildCompLoyal(GroupCol = TCorInsid, DateGroup = "YTD", DateFilt = Date, DataName = "NewCust"),

    # -- certif mtd
    BuildCompLoyal(GroupCol = All, DateGroup = "Lck-MTD", DateFilt = TCDate, DataName = "NewMem"),
    BuildCompLoyal(GroupCol = WebCent, DateGroup = "Lck-MTD", DateFilt = TCDate, DataName = "NewMem"),
    BuildCompLoyal(GroupCol = All, DateGroup = "Lck-MTD", DateFilt = Date, DataName = "NewCust"),
    BuildCompLoyal(GroupCol = TCorInsid, DateGroup = "Lck-MTD", DateFilt = Date, DataName = "NewCust"),
    # -- certif ytd
    BuildCompLoyal(GroupCol = All, DateGroup = "Lck-YTD", DateFilt = TCDate, DataName = "NewMem"),
    BuildCompLoyal(GroupCol = WebCent, DateGroup = "Lck-YTD", DateFilt = TCDate, DataName = "NewMem"),
    BuildCompLoyal(GroupCol = All, DateGroup = "Lck-YTD", DateFilt = Date, DataName = "NewCust"),
    BuildCompLoyal(GroupCol = TCorInsid, DateGroup = "Lck-YTD", DateFilt = Date, DataName = "NewCust")

  ) %>% 
  # mutate(AggregGrouping = "receipts") %>% 
  ungroup()



resultsAll %>% 
  mutate(lookup= as.character(glue::glue("{TypeName}|{AggregGrouping}|{Type}"))) %>% 
  relocate(lookup) %>% 
  select(-AggregGrouping) %>% 
  arrange(lookup) %>% 
  write_csv(., file = here::here("output", "Loyalty_Scorecard_Results.csv"))












