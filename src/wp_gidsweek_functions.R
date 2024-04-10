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

get_wp_conn <- function(pm_db_type = "prd") {
  
  # sqlstmt <- "show variables like 'character_set_client'"
  # result <- dbGetQuery(conn = wp_conn, statement = sqlstmt)
  
  if (pm_db_type == "prd") {
    db_host <- key_get(service = paste0("sql-wp", pm_db_type, "_host"))
    db_user <- key_get(service = paste0("sql-wp", pm_db_type, "_user"))
    db_password <- key_get(service = paste0("sql-wp", pm_db_type, "_pwd"))
    db_name <- key_get(service = paste0("sql-wp", pm_db_type, "_db"))
  } else {
    woj_gids_creds_dev <- read_rds(config$db_dev_creds)
    db_host <- woj_gids_creds_dev$db_host
    db_user <- woj_gids_creds_dev$db_user
    db_password <- woj_gids_creds_dev$db_password
    db_name <- woj_gids_creds_dev$db_name
  }
  
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



start_of_week_gids_universe <- function() {

  wp_conn <- get_wp_conn()
  
  if (typeof(wp_conn) != "S4") {
    stop("db-connection failed")
  }
  
  # qry <- "select DATE_FORMAT(max(post_date), '%Y-%m-%d %a %H:%i') AS ts_latest_post
  qry <- "select max(post_date) as max_post_date_chr
          from wp_posts
          where post_date > '2024-03-01' 
            and DAYOFWEEK(post_date) = 5 
            and hour(post_date) = 13 
            and post_type = 'programma'
          ;"
  latest_post <- dbGetQuery(wp_conn, qry)
  dbDisconnect(wp_conn)
  
  start_of_week <- ymd_hms(latest_post$max_post_date_chr, quiet = T) + days(7)
  tmp_format <- stamp("1969-07-20", orders = "%Y-%m-%d", quiet = T)
  tmp_format(start_of_week)
}
