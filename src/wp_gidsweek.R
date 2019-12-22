library(dplyr)
library(lubridate)
library(magrittr)
library(chron)
library(stringr)

giva_latest_home <- "C:/cz_salsa/config/giva_start.txt"
giva_latest <-
  read.csv(giva_latest_home,
           sep = "",
           stringsAsFactors = FALSE) %>%
  mutate(latest_run = ymd(latest_run))

current_run_start <- giva_latest$latest_run + ddays(7)
current_run_stop <- current_run_start + ddays(6)

giva_latest %<>% mutate(latest_run = current_run_start)

write.csv2(
  x = giva_latest,
  file = giva_latest_home,
  quote = F,
  row.names = F
)

cz_slot_days <- seq(from = current_run_start, to = current_run_stop, by = "days")
cz_slot_hours <- seq(0, 23, by = 1)
cz_slot_dates <- merge(cz_slot_days, chron(time = paste(cz_slot_hours, ":", 0, ":", 0)))
colnames(cz_slot_dates) <- c("slot_date", "slot_time")
cz_slot_dates$date_time <- as.POSIXct(paste(cz_slot_dates$slot_date, cz_slot_dates$slot_time))
row.names(cz_slot_dates) <- NULL


cz_slot_dates <- as.data.frame(cz_slot_dates) %>%
  select(date_time) %>%
  mutate(cz_slot = paste0(wday(date_time, label = T, abbr = T),
                          str_pad(hour(date_time),
                                  width = 2,
                                  side = "left",
                                  pad = "0"
                          )),
         ordinal_day = 1 + (day(date_time) - 1) %/% 7L
  ) %>%
  arrange(date_time)
