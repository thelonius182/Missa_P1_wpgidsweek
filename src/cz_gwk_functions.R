prep_week <- function(week_nr) {
  week_x_init <- tbl_zenderschema %>%
    select(cz_slot = slot, contains(week_nr))
  
  # pivot-long to collapse week attributes into NV-pairs ------------------
  week_x_long <-
    gather(
      data = week_x_init,
      key = slot_key,
      value = slot_value, -cz_slot,
      na.rm = T
    ) %>%
    mutate(
      slot_key = case_when(
        slot_key == paste0("week_", week_nr) ~ "titel",
        slot_key == paste0("bijzonderheden_week_", week_nr) ~ "cmt mt-rooster",
        slot_key == paste0("product_week_", week_nr) ~ "product",
        T ~ slot_key
      ),
      slot_key = factor(slot_key, ordered = T, levels = cz_slot_key_levels),
      cz_slot_day = str_sub(cz_slot, 1, 2),
      cz_slot_day = factor(cz_slot_day, ordered = T, levels = weekday_levels),
      cz_slot_hour = str_sub(cz_slot, 3, 4),
      cz_slot_size = str_sub(cz_slot, 5),
      ord_day = as.integer(week_nr)
    ) %>%
    select(
      cz_slot_day,
      ord_day,
      cz_slot_hour,
      cz_slot_key = slot_key,
      cz_slot_value = slot_value,
      cz_slot_size
    ) %>%
    arrange(cz_slot_day, cz_slot_hour, cz_slot_key)
  
  # promote slot-size to be an attribute too ------------------------------
  week_temp <- week_x_long %>%
    select(-cz_slot_key, -cz_slot_value) %>%
    mutate(
      cz_slot_key = "size",
      cz_slot_key = factor(x = cz_slot_key,
                           ordered = T,
                           levels = cz_slot_key_levels),
      cz_slot_value = cz_slot_size
    ) %>%
    select(cz_slot_day,
           ord_day,
           cz_slot_hour,
           cz_slot_key,
           cz_slot_value,
           cz_slot_size) %>%
    distinct
  
  # final result week-x ---------------------------------------------------
  week_x <- bind_rows(week_x_long, week_temp) %>%
    select(-cz_slot_size) %>%
    arrange(cz_slot_day, ord_day, cz_slot_hour, cz_slot_key)
  
  rm(week_x_init, week_x_long, week_temp)
  
  return(week_x)
}

get_cycle <- function(cz_week_start) {
  # test: cz_week_start <- "2019-11-21"
  ref_date_B_cycle <- ymd("2019-10-17")
  i_diff <- as.integer(cz_week_start - ref_date_B_cycle) %/% 7L
  if_else(i_diff %% 2 == 0, "B", "A")
}

fmt_utc_ts <- function(some_date) {
  format(some_date, "%Y-%m-%d_%a%H-%Z%z") %>%
    str_replace("CES?T", "UTC") %>%
    str_sub(1, 22)
}


bc2json <- function(pm_tib_json) {
  
  # Convert tibble to a list of named lists
  list_json <- lapply(1:nrow(pm_tib_json), function(i1) {
    as.list(pm_tib_json[i1, -1])
  })
  
  # set obj_name as the key
  names(list_json) <- pm_tib_json$obj_name
  
  toJSON(list_json, pretty = TRUE, auto_unbox = T)
}

start_of_week_pls <- function(arg_ts = now(tz = "Europe/Amsterdam")) {
  
  # adjust ts when running this on a Thursday after 13:00
  if (wday(arg_ts, week_start = 1, label = T) == "do" && hour(arg_ts) >= 13) {
    arg_ts <- arg_ts + days(1)
  }
  
  # get first Thursday 13:00 after arg_ts
  hour(arg_ts) <- 0
  minute(arg_ts) <- 0
  second(arg_ts) <- 0
  while (wday(arg_ts, week_start = 1, label = T) != "do") {
    arg_ts <- arg_ts + days(1)
  }
  
  tmp_format <- stamp("1969-07-20 17:18:19", orders = "%Y-%m0-%d %H:%M:%S", quiet = T)
  tmp_format(arg_ts + hours(13))
}

get_wp_conn <- function() {
  
  # sqlstmt <- "show variables like 'character_set_client'"
  # result <- dbGetQuery(conn = wp_conn, statement = sqlstmt)
  
  # if (pm_db_type == "prd") {
  #   db_host <- key_get(service = paste0("sql-wp", pm_db_type, "_host"))
  #   db_user <- key_get(service = paste0("sql-wp", pm_db_type, "_user"))
  #   db_password <- key_get(service = paste0("sql-wp", pm_db_type, "_pwd"))
  #   db_name <- key_get(service = paste0("sql-wp", pm_db_type, "_db"))
  # } else {
  #   woj_gids_creds_dev <- read_rds(config$db_dev_creds)
  #   db_host <- woj_gids_creds_dev$db_host
  #   db_user <- woj_gids_creds_dev$db_user
  #   db_password <- woj_gids_creds_dev$db_password
  #   db_name <- woj_gids_creds_dev$db_name
  # }
  
  db_port <- 3306
  # flog.appender(appender.file("/Users/nipper/Logs/nipper.log"), name = "nipperlog")
  
  grh_conn <- tryCatch( 
    {
      dbConnect(drv = MySQL(), user = db_user, password = db_password,
                dbname = db_name, host = db_host, port = db_port)
    },
    error = function(cond) {
      cat("Wordpress database connection failed (dev: is PuTTY running?)")
      # flog.error("Wordpress database onbereikbaar (dev: check PuTTY)", name = "nipperlog")
      return("connection-error")
    }
  )
  
  return(grh_conn)
}

# start_new_czweek_universe <- function() {
# 
#   wp_conn <- get_wp_conn()
#   
#   if (typeof(wp_conn) != "S4") {
#     stop("db-connection failed")
#   }
# 
#   qry <- "select distinct po2.post_date as cz_week_start, 
# 	               (SELECT 
#                      cast(sum(TIMESTAMPDIFF(HOUR, 
#                                             po1.post_date, 
#                                             str_to_date(pm1.meta_value, '%Y-%m-%d %H:%i:%s')))
#                           as char) as n_hrs
#                   FROM wp_posts po1 
#                      JOIN wp_postmeta pm1 ON pm1.post_id = po1.id
#                      JOIN wp_term_relationships tr1 ON tr1.object_id = po1.id
#                   WHERE po1.post_type = 'programma' 
#                     and po1.post_status = 'publish'
#                     AND tr1.term_taxonomy_id = 5
#                     AND po1.post_date BETWEEN po2.post_date 
#                                           AND date_add(po2.post_date, interval 167 HOUR)
#                     and pm1.meta_key = 'pr_metadata_uitzenddatum_end'
#                  ) as n_hrs
#           from wp_posts po2 
#           where po2.post_date > date_add(current_date(), interval -2 WEEK)
#             and DAYOFWEEK(po2.post_date) = 5 -- (1 = Sunday, 5 = Thursday)
#             and hour(po2.post_date) = 13 
#             and po2.post_type = 'programma';"
#   
#   cz_weeks <- dbGetQuery(wp_conn, qry)
#   dbDisconnect(wp_conn)
#   
#   cz_weeks.1 <- cz_weeks |> mutate(n_hrs = parse_integer(n_hrs),
#                                    cz_week_start = ymd_hms(cz_week_start, quiet = T)) 
#   cz_weeks.2 <- cz_weeks.1 |> filter(n_hrs == max(n_hrs)) |> arrange(desc(cz_week_start))
#   start_new_czweek <- cz_weeks.2$cz_week_start[1] + days(7)
#   tmp_format <- stamp("1969-07-20", orders = "%Y-%m-%d", quiet = T)
#   tmp_format(start_of_next_week)
# }

start_new_czweek_universe <- function() {
  qfns <- dir_ls(path = "C:/cz_salsa/gidsweek_uploaden/", 
                 type = "file",
                 regexp = "CZ_gidsweek_\\d{4}.*[.]json$") |> sort(decreasing = T)
  latest_upload <- str_extract(qfns[1], pattern = "\\d{4}_\\d{2}_\\d{2}") |> ymd()
  as.character(latest_upload + days(7))
}
