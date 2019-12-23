library(dplyr)
library(tidyr)
library(lubridate)
library(magrittr)
library(chron)
library(stringr)
library(yaml)

# Set new first day of gidsweek -------------------------------------------

giva_latest_home <- "C:/cz_salsa/config/giva_start.txt"
giva_latest <-
  read.csv(giva_latest_home,
           sep = "",
           stringsAsFactors = FALSE) %>%
  mutate(latest_run = ymd(latest_run))

current_run_start <- giva_latest$latest_run + ddays(7)
current_run_stop <- current_run_start + ddays(7)

giva_latest %<>% mutate(latest_run = current_run_start)

write.csv2(
  x = giva_latest,
  file = giva_latest_home,
  quote = F,
  row.names = F
)

# create time series ------------------------------------------------------

cz_slot_days <- seq(from = current_run_start, to = current_run_stop, by = "days")
cz_slot_hours <- seq(0, 23, by = 1)
cz_slot_dates <- merge(cz_slot_days, chron(time = paste(cz_slot_hours, ":", 0, ":", 0)))
colnames(cz_slot_dates) <- c("slot_date", "slot_time")
cz_slot_dates$date_time <- as.POSIXct(paste(cz_slot_dates$slot_date, cz_slot_dates$slot_time))
row.names(cz_slot_dates) <- NULL

# create cz-slot prefixes and combine with calendar -----------------------

cz_slot_dates <- as.data.frame(cz_slot_dates) %>%
  select(date_time) %>%
  mutate(ordinal_day = 1 + (day(date_time) - 1) %/% 7L,
         cz_slot_pfx = paste0(wday(date_time, label = T, abbr = T),
                          ordinal_day,
                          "_",
                          str_pad(hour(date_time),
                                  width = 2,
                                  side = "left",
                                  pad = "0"
                          ))
  ) %>%
  select(-ordinal_day) %>% 
  arrange(date_time)

# read modelrooster -------------------------------------------------------

config <- read_yaml("config.yaml")

tbl_zenderschema <- readRDS(paste0(config$giva.rds.dir, "zenderschema.RDS"))

week_0_init <- tbl_zenderschema %>%
  select(cz_slot = slot,
         hh_formule,
         wekelijks,
         product_wekelijks,
         bijzonderheden_wekelijks,
         AB_cyclus,
         product_A_cyclus,
         product_B_cyclus,
         bijzonderheden_A_cyclus,
         bijzonderheden_B_cyclus,
         balk,
         balk_tonen
  )

# create cz_slot_key levels for factor ------------------------------------

cz_slot_key_levels = c(
  "titel",
  "product",
  "product A",
  "product B",
  "in mt-rooster",
  "cmt mt-rooster",
  "herhaling",
  "balk",
  "size"
)

# pivot-long to collapse week attributes into NV-pairs --------------------

week_0_long1 <- gather(week_0_init, slot_key, slot_value, -cz_slot, na.rm = T) %>% 
  mutate(slot_key = case_when(slot_key == "wekelijks" ~ "titel", 
                              slot_key == "hh_formule" ~ "herhaling",
                              slot_key == "bijzonderheden_wekelijks" ~ "cmt mt-rooster",
                              slot_key == "bijzonderheden_A_cyclus" ~ "cmt mt-rooster",
                              slot_key == "bijzonderheden_B_cyclus" ~ "cmt mt-rooster",
                              slot_key == "product_wekelijks" ~ "product",
                              slot_key == "balk_tonen" ~ "in mt-rooster",
                              slot_key == "AB_cyclus" ~ "titel",
                              slot_key == "product_A_cyclus" ~ "product A",
                              slot_key == "product_B_cyclus" ~ "product B",
                              T ~ slot_key),
         slot_key = factor(slot_key, ordered = T, levels = cz_slot_key_levels),
         cz_slot_day = str_sub(cz_slot, 1, 2),
         cz_slot_day = factor(cz_slot_day, ordered = T, levels = c("do",
                                                                   "vr",
                                                                   "za",
                                                                   "zo",
                                                                   "ma",
                                                                   "di",
                                                                   "wo"
                                                                   )),
         cz_slot_hour = str_sub(cz_slot, 3, 4),
         cz_slot_size = str_sub(cz_slot, 5),
         ord_day_1 = 1,
         ord_day_2 = 2,
         ord_day_3 = 3,
         ord_day_4 = 4,
         ord_day_5 = 5
  ) %>% 
  arrange(cz_slot_day, cz_slot_hour, slot_key)

week_0_long2 <-
  gather(
    data = week_0_long1,
    key = ord_day_tag,
    value = ord_day, -starts_with("cz_"), -starts_with("slot_")
  ) %>%
  select(
    cz_slot_day,
    ord_day,
    cz_slot_hour,
    cz_slot_key = slot_key,
    cz_slot_value = slot_value,
    cz_slot_size
  ) %>% 
  arrange(cz_slot_day, ord_day, cz_slot_hour, cz_slot_key)

# promote  slot-size to be an attribute ---------------------------------------------

week_temp <- week_0_long2 %>% 
  select(-cz_slot_key, -cz_slot_value) %>% 
  mutate(cz_slot_key = "size",
         cz_slot_key = factor(x = cz_slot_key, ordered = T, levels = cz_slot_key_levels),
         cz_slot_value = cz_slot_size) %>% 
  select(cz_slot_day, ord_day, cz_slot_hour, cz_slot_key, cz_slot_value, cz_slot_size) %>% 
  distinct

# final result week-0 -----------------------------------------------------

week_0 <- bind_rows(week_0_long2, week_temp) %>% 
  select(-cz_slot_size) %>% 
  arrange(cz_slot_day, ord_day, cz_slot_hour, cz_slot_key)
  
rm(week_0_init, week_0_long1, week_0_long2, week_temp)
