# pulling receipt information 
# Tue Jan 11 08:48:38 2022 ------------------------------


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

# -------- Pulling 

pull_sql <- glue_sql(
  "
Select CustomerID, CenterID, TentID, CreatedOn, PurchaseDate, ReceiptAmount, MinsBtwnPurchAndSubmit 
	From (
		Select * 
			, datediff(minute, PurchaseDate, CreatedOn) as MinsBtwnPurchAndSubmit
		From tblReceiptTracking 
		Where Status = 'A'
			And DeletedOn is null 
			And TentID != 'TCPTS'
			And TentID is not null 
			And TentID != 'OTHER'
			And ReceiptAmount < 100000
	) a

  ",
.con = con)

parm_sql <- dbSendQuery(con, pull_sql)
recpts <- dbFetch(parm_sql)

dbClearResult(parm_sql)

save(recpts, file = here::here("data", "recpts.rds"))



#---------- 

receipts <- 
  recpts %>% 
  mutate(Date = as.Date(PurchaseDate),
         CreatDate = as.Date(CreatedOn),
         Within24 = ifelse(MinsBtwnPurchAndSubmit < (60*24), T, F))%>% 
  mutate(All = "Portfolio")

# unique(csr$UserType)

#------ Build Comp function ----------

BuildCompRec <- function(GroupCol, SummarCol, DateGroup, Win24, SamePerd,AggType = c("cust", "numb", "recpt", "avgVal"), DataName){
  col_name <- enquo(GroupCol)
  sel_var <- as.name(DateGroup)
  summ_name <- enquo(SummarCol)
  
  DtList <- c("CY", "LY", "LY2")
  df_total = data.frame()
  
  for (Dt in DtList) {
    
    dlk <- datesLk %>% mutate(Start = as.Date(Start), End = as.Date(End) ) %>% filter(Type %in% DateGroup, DateOrder == Dt)
    
    dat <- 
      receipts %>% 
      {if(Win24){
        filter(., Within24 == T) 
      } else {
        .
      }}  %>% 
# Add extra filter if purch and submit within same period
      {if(SamePerd){
        filter(., CreatDate >= as.Date(dlk$Start) &  
                 CreatDate <= as.Date(dlk$End) & 
                 Date >= as.Date(dlk$Start) &  
                 Date <= as.Date(dlk$End))
      } else {
        filter(., Date >= as.Date(dlk$Start) &  
                 Date <= as.Date(dlk$End))
      }}  %>% 
      bind_cols(., dlk) %>% 
      mutate(Year = as.character(Year)) %>% 
      group_by(Type, Year,DateOrder, AggregGrouping = All) %>% #!!col_name
      {if(AggType == "cust"){
        summarise(., gross = n_distinct(CustomerID, na.rm=T),
                  MinDate = min(Date),
                  MaxDate = max(Date))
      } else if(AggType == "numb"){
        summarise(., gross = n(),
                  MinDate = min(Date),
                  MaxDate = max(Date))
      } else if(AggType == "recpt"){
        summarise(., gross = sum(ReceiptAmount, na.rm=T), ##!!summ_name
                  MinDate = min(Date),
                  MaxDate = max(Date))
      } else if(AggType == "avgVal"){
        summarise(., gross = mean(ReceiptAmount, na.rm=T),
                  MinDate = min(Date),
                  MaxDate = max(Date))
      }}  %>% 
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
    relocate(TypeName) 
  
}

# SummarCol=ReceiptAmount,
# BuildCompRec(GroupCol = All, AggType = "avgVal", DateGroup = "Lck-MTD", Win24=F,SamePerd=T, DataName = "AvgValSamePer")

# ------------------
resultsAll <-
  bind_rows(
    # -- last week
    BuildCompRec(GroupCol = All, AggType = "recpt", DateGroup = "Wk", Win24=T,SamePerd=F, DataName = "RecValue24hr"),
    BuildCompRec(GroupCol = All, AggType = "cust", DateGroup = "Wk", Win24=T,SamePerd=F, DataName = "MembSubm24hr"),
    BuildCompRec(GroupCol = All, AggType = "numb", DateGroup = "Wk", Win24=T, SamePerd=F,DataName = "NumbRecpt24hr"),
    BuildCompRec(GroupCol = All, AggType = "avgVal", DateGroup = "Wk", Win24=T,SamePerd=F, DataName = "AvgVal24hr"),

    # -- mtd
    BuildCompRec(GroupCol = All, AggType = "recpt", DateGroup = "MTD", Win24=T,SamePerd=F, DataName = "RecValue24hr"),
    BuildCompRec(GroupCol = All, AggType = "cust", DateGroup = "MTD", Win24=T,SamePerd=F, DataName = "MembSubm24hr"),
    BuildCompRec(GroupCol = All, AggType = "numb", DateGroup = "MTD", Win24=T,SamePerd=F, DataName = "NumbRecpt24hr"),
    BuildCompRec(GroupCol = All, AggType = "avgVal", DateGroup = "MTD", Win24=T,SamePerd=F, DataName = "AvgVal24hr"),
    # BuildCompRec(GroupCol = All, DateGroup = "MTD"),
    # -- ytd
    BuildCompRec(GroupCol = All, AggType = "recpt", DateGroup = "YTD", Win24=T,SamePerd=F, DataName = "RecValue24hr"),
    BuildCompRec(GroupCol = All, AggType = "cust", DateGroup = "YTD", Win24=T,SamePerd=F, DataName = "MembSubm24hr"),
    BuildCompRec(GroupCol = All, AggType = "numb", DateGroup = "YTD", Win24=T, SamePerd=F,DataName = "NumbRecpt24hr"),
    BuildCompRec(GroupCol = All, AggType = "avgVal", DateGroup = "YTD", Win24=T,SamePerd=F, DataName = "AvgVal24hr"),
    # BuildCompRec(GroupCol = All, DateGroup = "YTD"),
    # -- certif mtd
    BuildCompRec(GroupCol = All, AggType = "recpt", DateGroup = "Lck-MTD", Win24=T,SamePerd=F, DataName = "RecValue24hr"),
    BuildCompRec(GroupCol = All, AggType = "cust", DateGroup = "Lck-MTD", Win24=T,SamePerd=F, DataName = "MembSubm24hr"),
    BuildCompRec(GroupCol = All, AggType = "numb", DateGroup = "Lck-MTD", Win24=T, SamePerd=F, DataName = "NumbRecpt24hr"),
    BuildCompRec(GroupCol = All, AggType = "avgVal", DateGroup = "Lck-MTD", Win24=T,SamePerd=F, DataName = "AvgVal24hr"),
    BuildCompRec(GroupCol = All, AggType = "recpt", DateGroup = "Lck-MTD", Win24=F,SamePerd=F, DataName = "RecValueAll"),
    BuildCompRec(GroupCol = All, AggType = "cust", DateGroup = "Lck-MTD", Win24=F,SamePerd=F, DataName = "MembSubmAll"),
    BuildCompRec(GroupCol = All, AggType = "numb", DateGroup = "Lck-MTD", Win24=F,SamePerd=F, DataName = "NumbRecptAll"),
    BuildCompRec(GroupCol = All, AggType = "avgVal", DateGroup = "Lck-MTD", Win24=F,SamePerd=F, DataName = "AvgValAll"),
    BuildCompRec(GroupCol = All, AggType = "recpt", DateGroup = "Lck-MTD", Win24=F,SamePerd=T, DataName = "RecValueSamePer"),
    BuildCompRec(GroupCol = All, AggType = "cust", DateGroup = "Lck-MTD", Win24=F,SamePerd=T, DataName = "MembSubmSamePer"),
    BuildCompRec(GroupCol = All, AggType = "numb", DateGroup = "Lck-MTD", Win24=F,SamePerd=T, DataName = "NumbRecptSamePer"),
    BuildCompRec(GroupCol = All, AggType = "avgVal", DateGroup = "Lck-MTD", Win24=F,SamePerd=T, DataName = "AvgValSamePer"),
    # BuildCompRec(GroupCol = All, DateGroup = "Lck-MTD"),
    # -- certif mtd
    BuildCompRec(GroupCol = All, AggType = "recpt", DateGroup = "Lck-YTD", Win24=T,SamePerd=F, DataName = "RecValue24hr"),
    BuildCompRec(GroupCol = All, AggType = "cust", DateGroup = "Lck-YTD", Win24=T,SamePerd=F, DataName = "MembSubm24hr"),
    BuildCompRec(GroupCol = All, AggType = "numb", DateGroup = "Lck-YTD", Win24=T,SamePerd=F, DataName = "NumbRecpt24hr"),
    BuildCompRec(GroupCol = All, AggType = "avgVal", DateGroup = "Lck-YTD", Win24=T,SamePerd=F, DataName = "AvgVal24hr"),
    BuildCompRec(GroupCol = All, AggType = "recpt", DateGroup = "Lck-YTD", Win24=F,SamePerd=F, DataName = "RecValueAll"),
    BuildCompRec(GroupCol = All, AggType = "cust", DateGroup = "Lck-YTD", Win24=F,SamePerd=F, DataName = "MembSubmAll"),
    BuildCompRec(GroupCol = All, AggType = "numb", DateGroup = "Lck-YTD", Win24=F,SamePerd=F, DataName = "NumbRecptAll"),
    BuildCompRec(GroupCol = All, AggType = "avgVal", DateGroup = "Lck-YTD", Win24=F,SamePerd=F, DataName = "AvgValAll"),
    BuildCompRec(GroupCol = All, AggType = "recpt", DateGroup = "Lck-YTD", Win24=F,SamePerd=T, DataName = "RecValueSamePer"),
    BuildCompRec(GroupCol = All, AggType = "cust", DateGroup = "Lck-YTD", Win24=F,SamePerd=T, DataName = "MembSubmSamePer"),
    BuildCompRec(GroupCol = All, AggType = "numb", DateGroup = "Lck-YTD", Win24=F,SamePerd=T, DataName = "NumbRecptSamePer"),
    BuildCompRec(GroupCol = All, AggType = "avgVal", DateGroup = "Lck-YTD", Win24=F,SamePerd=T, DataName = "AvgValSamePer")
    # BuildCompRec(GroupCol = All, DateGroup = "Lck-YTD"),
  ) %>% 
  mutate(AggregGrouping = "receipts")



resultsAll %>% 
  mutate(lookup= as.character(glue::glue("{TypeName}|{Type}"))) %>% 
  relocate(lookup) %>% 
  select(-AggregGrouping) %>% 
  arrange(lookup) %>% 
  write_csv(., file = here::here("output", "receipts_Scorecard_Results.csv"))





