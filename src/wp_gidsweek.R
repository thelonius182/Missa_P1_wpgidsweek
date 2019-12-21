library(dplyr)
library(lubridate)
library(magrittr)

giva_latest_home <- "C:/cz_salsa/config/giva_start.txt"
giva_latest <-
  read.csv(giva_latest_home,
           sep = "",
           stringsAsFactors = FALSE) %>% 
  mutate(latest_run = ymd(latest_run))

current_run <- giva_latest$latest_run + days(7)

giva_latest %<>% mutate(latest_run = current_run)

write.csv(x = giva_latest, file = giva_latest_home)
