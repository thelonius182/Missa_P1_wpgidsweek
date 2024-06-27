pacman::p_load(dplyr, tidyr, lubridate, magrittr, stringr, fs, keyring,
               RMySQL, yaml, purrr, futile.logger, jsonlite, readr, conflicted)

conflicts_prefer(dplyr::lag, dplyr::lead, dplyr::filter, lubridate::minutes, .quiet = T)

source("src/cz_gwk_functions.R")

flog.appender(appender.file("/Users/nipper/Logs/wpgidsweek.log"), name = "wpgidsweeklog")
flog.info("= = = = = WP-Gidsweek (dev-branch) = = = = = = = = = =", name = "wpgidsweeklog")
 
# cz-week's 168 hours comprise 8 weekdays, not 7 (Thursday AM and PM)
# but to the schedule-template both Thursdays are the same, as the
# template is undated.
# Both Thursday parts will separate when the schedule gets 'calendarized'
# current_run_start <- ymd("2024-05-16")
current_run_start <- ymd(start_new_czweek_universe(), quiet = T)
current_run_stop <- current_run_start + days(7)
log_date <- format(current_run_start, "%e %B %Y") |> str_trim()
flog.info(sprintf("CZ-gidsweek vanaf donderdag %s, 13:00", log_date), name = "wpgidsweeklog")

source("src/get_google_czdata.R")
flog.info("Google-data ingelezen", name = "wpgidsweeklog")

# create time series ------------------------------------------------------
cz_slot_days <- seq(from = current_run_start, to = current_run_stop, by = "days")
cz_slot_hours <- seq(0, 23, by = 1)
cz_slot_dates <- merge(cz_slot_days, chron::chron(time = paste(cz_slot_hours, ":", 0, ":", 0)))
colnames(cz_slot_dates) <- c("slot_date", "slot_time")
cz_slot_dates$date_time <- as.POSIXct(paste(cz_slot_dates$slot_date, cz_slot_dates$slot_time), "GMT")
row.names(cz_slot_dates) <- NULL

# + create cz-slot prefixes and combine with calendar ---------------------
cz_slot_dates_raw <- as.data.frame(cz_slot_dates) %>%
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

# create factor levels ----------------------------------------------------
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

weekday_levels = c("do", "vr", "za", "zo", "ma", "di", "wo")

# = = = = = = = = = = = = = = = = = = = = = = = = + = = = = = = = = = -----

# prep the weekly's, under 'week-0' ---------------------------------------
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

# + pivot-long to collapse week attributes into NV-pairs ------------------
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
         cz_slot_day = factor(cz_slot_day, ordered = T, levels = weekday_levels),
         cz_slot_hour = str_sub(cz_slot, 3, 4),
         cz_slot_size = str_sub(cz_slot, 5),
         ord_day_1 = 1,
         ord_day_2 = 2,
         ord_day_3 = 3,
         ord_day_4 = 4,
         ord_day_5 = 5
  ) %>% 
  arrange(cz_slot_day, cz_slot_hour, slot_key)

# + do 5 copies of each slot, one for each ordinal day --------------------
week_0_long2 <-
  gather(
    data = week_0_long1,
    key = ord_day_tag,
    value = ord_day, 
    -starts_with("cz_"), -starts_with("slot_")
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

# + promote slot-size to be an attribute too ------------------------------
week_temp <- week_0_long2 %>% 
  select(-cz_slot_key, -cz_slot_value) %>% 
  mutate(cz_slot_key = "size",
         cz_slot_key = factor(x = cz_slot_key, ordered = T, levels = cz_slot_key_levels),
         cz_slot_value = cz_slot_size) %>% 
  select(cz_slot_day, ord_day, cz_slot_hour, cz_slot_key, cz_slot_value, cz_slot_size) %>% 
  distinct

# + final result week-0 ---------------------------------------------------
week_0 <- bind_rows(week_0_long2, week_temp) %>% 
  select(-cz_slot_size) %>% 
  arrange(cz_slot_day, ord_day, cz_slot_hour, cz_slot_key)
  
rm(week_0_init, week_0_long1, week_0_long2, week_temp)

# = = = = = = = = = = = = = = = = = = = = = = = = + = = = = = = = = = -----

# prep week-1/5 (days 1-7/8-14/... of month) ------------------------------
# no matter which weekday comes first!

week_1 <-  prep_week("1")
week_2 <-  prep_week("2")
week_3 <-  prep_week("3")
week_4 <-  prep_week("4")
week_5 <-  prep_week("5")

# + merge the weeks, repair Thema -----------------------------------------

cz_week <- bind_rows(week_0, week_1, week_2, week_3, week_4, week_5) %>% 
  mutate(cz_slot_pfx = paste0(cz_slot_day, ord_day, "_", cz_slot_hour)) %>% 
  select(cz_slot_pfx, cz_slot_key, cz_slot_value) %>% 
  arrange(cz_slot_pfx, cz_slot_key) %>% 
  distinct %>% 
  # repair Thema
  filter(!cz_slot_pfx %in%  c("wo2_21", "wo4_21")) %>% 
  mutate(cz_slot_value = if_else(cz_slot_pfx %in% c("wo2_20", "wo4_20") 
                                 & cz_slot_key == "size",
                                 "120",
                                 cz_slot_value)
  )

rm(week_0, week_1, week_2, week_3, week_4, week_5, tbl_zenderschema)

# shrink date list Thursdays ----------------------------------------------
# list already runs  Thursday to Thursday; make it run from 13:00 - 13:00
df_start <- min(cz_slot_dates$date_time)
hour(df_start) <- 13
df_stop <- max(cz_slot_dates$date_time)
hour(df_stop) <- 13

cz_slot_dates <- cz_slot_dates_raw %>% 
  filter(date_time >= df_start & date_time < df_stop)

rm(cz_slot_dates_raw)

# = = = = = = = = = = = = = = = = = = = = = = = = + = = = = = = = = = -----

# join dates, slots & info ------------------------------------------------
# + get cycle for A/B-week ------------------------------------------------
cycle <- get_cycle(current_run_start)

#+ get iTunes-repeats --------------------------------------------------
itunes_repeat_slots <- cz_week %>% 
  filter(cz_slot_key == paste0("product ", cycle) & cz_slot_value == "h") %>% 
  select(cz_slot_pfx)

#+... join on 'titel', exclude on itunes-repeat ----
cz_week_titles <- cz_week %>% 
  filter(cz_slot_key == "titel") %>% 
  anti_join(itunes_repeat_slots)

#+ get sizes of each program ----
cz_week_sizes <- cz_week %>% 
  filter(cz_slot_key == "size") %>% 
  select(cz_slot_pfx, size = cz_slot_value)

#+ get regular repeats ----
suppressWarnings(
  cz_week_repeats <- cz_week %>%
    filter(cz_slot_key == "herhaling") %>%
    mutate(
      hh_offset_dag = if_else(cz_slot_value == "tw", 
                              7L, 
                              as.integer(str_sub(cz_slot_value, 1, 2))),
      hh_offset_uur = if_else(cz_slot_value == "tw", 
                              as.integer(str_sub(cz_slot_pfx, 5, 6)),
                              as.integer(str_sub(cz_slot_value, 5, 6)))
    ) %>%
    select(cz_slot_pfx, hh_offset_dag, hh_offset_uur) 
)

# + join the lot ----
repeat { # main control loop

  #+... but test for missing info's first! ------------------------------------
  na_infos <- cz_week_titles %>% 
    anti_join(tbl_wpgidsinfo, by = c("cz_slot_value" = "key_modelrooster")) %>%
    select(cz_slot_value) %>% distinct
  
  if (nrow(na_infos) > 0) {
    na_infos_df <- unite(data = na_infos, col = regel, sep = " ")
    flog.info("WP-gidsinfo is incompleet. Geen vermelding voor %s\nscript wordt afgebroken.", 
              na_infos_df, name = "wpgidsweeklog")
    break
  }
  
  # + save for NS ----
  nipperstudio_week_his <- read_rds("C:/cz_salsa/cz_exchange/nipperstudio_week.RDS")

  nipperstudio_week <- cz_slot_dates %>%
    inner_join(cz_week_titles) %>%
    inner_join(cz_week_sizes) %>%
    bind_rows(nipperstudio_week_his) %>% distinct() %>% arrange(date_time)

  write_rds(x = nipperstudio_week, file = "C:/cz_salsa/cz_exchange/nipperstudio_week.RDS")
  
  broadcasts.I <- cz_slot_dates %>% 
    inner_join(cz_week_titles, by = join_by(cz_slot_pfx)) %>% 
    inner_join(cz_week_sizes, by = join_by(cz_slot_pfx)) %>% 
    left_join(cz_week_repeats, by = join_by(cz_slot_pfx)) %>% 
    inner_join(tbl_wpgidsinfo, by = c("cz_slot_value" = "key_modelrooster")) %>% 
    left_join(tbl_wpgidsinfo_nl_en, by = c("productie_taak" = "item_NL")) %>% 
    rename(productie_taak_EN = item_EN) %>% 
    left_join(tbl_wpgidsinfo_nl_en, by = c("genre_NL1" = "item_NL")) %>% 
    rename(genre_EN1 = item_EN) %>% 
    left_join(tbl_wpgidsinfo_nl_en, by = c("genre_NL2" = "item_NL")) %>% 
    rename(genre_EN2 = item_EN) %>% 
    mutate(json_start = date_time,
           json_stop = date_time + dminutes(as.integer(size))) %>% 
    select(date_time,
           json_start,
           json_stop,
           hh_offset_dag,
           hh_offset_uur,
           size,
           titel_NL,
           titel_EN,
           genre_NL1,
           genre_EN1,
           genre_NL2,
           genre_EN2,
           intro_NL,
           intro_EN,
           productie_taak,
           productie_taak_EN,
           productie_mdw
    )
  
  broadcasts_orig <- broadcasts.I %>% 
    select(-hh_offset_dag, -hh_offset_uur, -size)

  broadcasts_repeat.I <- broadcasts.I %>% 
    filter(!is.na(hh_offset_dag)) %>% 
    mutate(json_start = json_start + ddays(as.integer(hh_offset_dag)))

  hour(broadcasts_repeat.I$json_start) <- broadcasts_repeat.I$hh_offset_uur

  broadcasts_repeat.II <- broadcasts_repeat.I %>% 
    mutate(json_stop = json_start + dminutes(as.integer(size)))
  
  broadcasts_repeat <- broadcasts_repeat.II %>% 
    select(-hh_offset_dag, -hh_offset_uur, -size) 

  broadcasts_repeat$titel_NL <- NA_character_
  broadcasts_repeat$titel_EN <- NA_character_
  broadcasts_repeat$genre_NL1 <- NA_character_
  broadcasts_repeat$genre_NL2 <- NA_character_
  broadcasts_repeat$genre_EN1 <- NA_character_
  broadcasts_repeat$genre_EN2 <- NA_character_
  broadcasts_repeat$intro_NL <- NA_character_
  broadcasts_repeat$intro_EN <- NA_character_
  broadcasts_repeat$productie_taak <- NA_character_
  broadcasts_repeat$productie_taak_EN <- NA_character_
  broadcasts_repeat$productie_mdw <- NA_character_

  broadcasts.II <- bind_rows(broadcasts_orig, broadcasts_repeat) %>% 
    arrange(date_time)
  
  broadcasts.III <- broadcasts.II %>% 
    mutate(herhalingVan = if_else(is.na(titel_NL), 
                                  format(date_time,"%Y-%m-%d %H:%M"),
                                  NA_character_),
           # if_else doesn't play nicely with dates set to NA
           # herhalingVan = replace(x = herhalingVan, !is.na(titel_NL), NA),
           # nv_ts: name/value-pair timestamp, in front of pgm's nv-pair set
           nv_ts = if_else(!is.na(titel_NL), date_time, json_start)
    ) %>% 
    select(nv_ts,
           json_start,
           json_stop,
           herhalingVan,
           titel_NL,
           titel_EN,
           genre_NL1,
           genre_EN1,
           genre_NL2,
           genre_EN2,
           intro_NL,
           intro_EN,
           productie_taak,
           productie_taak_EN,
           productie_mdw
    )

  # prep json ----
  # . + originals with 1 genre ----
  tib_json_ori_gen1 <- broadcasts.III |> filter(is.na(herhalingVan) & is.na(genre_NL2)) |> 
    select(nv_ts, start = json_start, json_stop, titel_NL:productie_mdw, -genre_NL2, -genre_EN2) |> 
    mutate(obj_name = fmt_utc_ts(nv_ts), 
           stop = format(json_stop, "%Y-%m-%d %H:%M"),
           start = format(start, "%Y-%m-%d %H:%M"),
           `post-type` = "programma") |> 
    select(obj_name, `post-type`, start, stop, everything(), -nv_ts, -json_stop) |> 
    rename(`titel-nl` = titel_NL,
           `titel-en` = titel_EN,
           `genre-1-nl` = genre_NL1,
           `genre-1-en` = genre_EN1,
           `std.samenvatting-nl` = intro_NL,
           `std.samenvatting-en` = intro_EN,
           `productie-1-taak-nl` = productie_taak,
           `productie-1-taak-en` = productie_taak_EN,
           `productie-1-mdw` = productie_mdw)
  
  json_ori_gen1 <- bc2json(tib_json_ori_gen1)
  
  # . + originals with 2 genres ----
  tib_json_ori_gen2 <- broadcasts.III |> filter(is.na(herhalingVan) & !is.na(genre_NL2)) |> 
    select(nv_ts, start = json_start, json_stop, titel_NL:productie_mdw) |> 
    mutate(obj_name = fmt_utc_ts(nv_ts), 
           stop = format(json_stop, "%Y-%m-%d %H:%M"),
           start = format(start, "%Y-%m-%d %H:%M"),
           `post-type` = "programma") |> 
    select(obj_name, `post-type`, start, stop, everything(), -nv_ts, -json_stop) |> 
    rename(`titel-nl` = titel_NL,
           `titel-en` = titel_EN,
           `genre-1-nl` = genre_NL1,
           `genre-1-en` = genre_EN1,
           `genre-2-nl` = genre_NL2,
           `genre-2-en` = genre_EN2,
           `std.samenvatting-nl` = intro_NL,
           `std.samenvatting-en` = intro_EN,
           `productie-1-taak-nl` = productie_taak,
           `productie-1-taak-en` = productie_taak_EN,
           `productie-1-mdw` = productie_mdw)
  
  json_ori_gen2 <- bc2json(tib_json_ori_gen2)
  
  # . + replays ----
  tib_json_rep <- broadcasts.III |> filter(!is.na(herhalingVan)) |> 
    select(nv_ts, start = json_start, json_stop, herhalingVan) |> 
    mutate(obj_name = fmt_utc_ts(nv_ts), 
           stop = format(json_stop, "%Y-%m-%d %H:%M"),
           start = format(start, "%Y-%m-%d %H:%M"),
           `post-type` = "programma",
           `herhaling-van-post-type` = "programma",
           `herhaling-van` = herhalingVan) |> 
    select(obj_name, `post-type`, start, stop, everything(), -nv_ts, -json_stop, -herhalingVan)

  json_rep <- bc2json(tib_json_rep)
  
  # . + join them ----
  cz_week_json_qfn <- file_temp(pattern = "cz_week_json", ext = "json")
  file_create(cz_week_json_qfn)
  write_file(json_ori_gen1, cz_week_json_qfn, append = F)
  write_file(json_ori_gen2, cz_week_json_qfn, append = T)
  write_file(json_rep, cz_week_json_qfn, append = T)
  
  # the file still has 3 intact json-objects. Remove the inner boundaries to make it a single object
  temp_json_file.1 <- read_file(cz_week_json_qfn)
  temp_json_file.2 <- temp_json_file.1 |> str_replace_all("[}][{]", ",")
  
  # create final json-file ----
  final_json_qfn <- paste0("CZ_gidsweek_", format(current_run_start, "%Y_%m_%d"), ".json")
  write_file(temp_json_file.2, path_join(c("C:", "cz_salsa", "gidsweek_uploaden", final_json_qfn)), 
             append = F)
}

flog.info("= = = = = = = = = = = = = FIN = = = = = = = = = = = = = = = = = = = = = = =", 
          name = "wpgidsweeklog")
