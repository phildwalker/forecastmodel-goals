# Reviewing... First receipt date for unique members by year
# Fri Feb 04 15:24:01 2022 ------------------------------

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
load(file = here::here("data", "recpts.rds"))
#------ Clean Receipts -----

CleanRecpts_Seas <-
  recpts %>%
  mutate(Date = as.Date(PurchaseDate),
         CreatDate = as.Date(CreatedOn),
         PurchYear = as.character(lubridate::year(Date))) %>% 
  group_by(PurchYear, CustomerID) %>%
  summarise(FirRecPerYear = min(Date, na.rm=T),
            CountRecPerYear = n()) %>% 
  ungroup()

save(CleanRecpts_Seas, file = here::here("data", "CleanRecpts_Seas.rds"))


RecptMo <-
  CleanRecpts_Seas %>% 
  mutate(FirRecMo = lubridate::floor_date(FirRecPerYear, "month")) %>% 
  group_by(PurchYear, FirRecMo) %>% 
  summarise(CountCusFir = n()) %>% 
  ungroup() %>% 
  mutate(Month = lubridate::month(FirRecMo), #, label = T, abbr = T
         CountCusFir = as.numeric(CountCusFir))



library(gt)

RecptMo %>% 
  filter(PurchYear >= 2016,
         PurchYear < 2022) %>% 
  group_by(PurchYear) %>% 
  summarise(TotalMem = sum(CountCusFir, na.rm = T)) %>% 
  ungroup() %>% 
  gt(rowname_col = "PurchYear") %>% 
  fmt_number(columns = 2,  decimals = 1,  suffix= T)


library(geomtextpath)

(p1 <- RecptMo %>% 
  filter(PurchYear >= 2016) %>% 
  ggplot(aes(Month, CountCusFir, color=PurchYear))+
    geom_point()+
    # geom_line(size = 1.1)+
    geom_textpath(aes(label = PurchYear), size = 5, linewidth=1, alpha=0.7)+
    # geom_smooth(se=F)+
  scale_y_continuous(labels = scales::comma, breaks = seq(0,40000, 5000),expand = c(0,0), limits = c(0,40000))+
  scale_x_continuous(breaks = seq(1,12,1))+
  scale_color_manual(values = cb_palette)+
  labs(title ="Count of Unique Customer's First Reciept Logged Over Time",
       y = NULL, x="Month Number")+
  theme(legend.position = "none"))


(p2 <- RecptMo %>% 
  filter(PurchYear >= 2016,
         PurchYear < 2022) %>% 
  group_by(PurchYear) %>% 
  mutate(Perc = CountCusFir/ sum(CountCusFir)) %>% 
  ungroup() %>% 
  ggplot(aes(Month, Perc, color=PurchYear))+
  geom_point()+
  # geom_line(size = 1.1)+
  geom_textpath(aes(label = PurchYear), size = 5, linewidth=1, alpha=0.7)+
  # geom_smooth(se=F)+
  scale_y_continuous(labels = scales::percent, breaks = seq(0,.21, .03), expand = c(0,0), limits = c(0, 0.21))+
  scale_x_continuous(breaks = seq(1,12,1))+
  scale_color_manual(values = cb_palette)+
  labs(title ="Monthly Percent of Unique Customer's First Reciept Logged Over Time",
       y = NULL, x="Month Number")+
  theme(legend.position = "none"))



library(patchwork)

p1 + p2
