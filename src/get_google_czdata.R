library(googledrive)
library(keyring)
library(readxl)
library(yaml)
library(gargle)

cz_extract_sheet <- function(ss_name, sheet_name) {
  readxl::read_xlsx(ss_name,
            sheet = sheet_name,
            .name_repair = ~ ifelse(nzchar(.x), .x, LETTERS[seq_along(.x)]))
}

cz_get_url <- function(cz_ss) {
  cz_url <- paste0("url_", cz_ss)
  paste0("https://", config$url_pfx, config[[cz_url]])
}

# downloads GD ------------------------------------------------------------

# aanmelden bij GD loopt via de procedure die beschreven is in "Operation Missa > Voortgang > 4.Toegangsrechten GD".

# Roosters 3.0 ophalen bij GD
path_roosters <- paste0(config$gs_downloads, "/", "roosters.xlsx")
drive_download(file = cz_get_url("roosters"), overwrite = T, path = path_roosters)

# iTunes-cupboard ophalen bij GD
path_itunes_cupboard <- paste0(config$gs_downloads, "/", "itunes_cupboard.xlsx")
drive_download(file = cz_get_url("itunes_cupboard"), overwrite = T, path = path_itunes_cupboard)

# WP-gids-info ophalen bij GD
path_wp_gidsinfo <- paste0(config$gs_downloads, "/", "wordpress_gidsinfo.xlsx")
drive_download(file = cz_get_url("wordpress_gidsinfo"), overwrite = T, path = path_wp_gidsinfo)

# Nipper-spreadsheet ophalen bij GD
path_wp_nipper_express <- paste0(config$gs_downloads, "/", "nipper_express.xlsx")
drive_download(file = cz_get_url("nipper_express"), overwrite = T, path = path_wp_nipper_express)

# sheets als df -----------------------------------------------------------

tbl_raw_zenderschema <- cz_extract_sheet(path_roosters, sheet_name = paste0("modelrooster-", config$modelrooster_versie))
tbl_raw_presentatie <- cz_extract_sheet(path_roosters, sheet_name = "presentatie")
tbl_raw_montage <- cz_extract_sheet(path_roosters, sheet_name = "montage")
tbl_raw_itunes_cupboard <- cz_extract_sheet(path_itunes_cupboard, sheet_name = "playlist_names")
tbl_raw_wpgidsinfo <- cz_extract_sheet(path_wp_gidsinfo, sheet_name = "gids-info")
tbl_raw_wpgidsinfo_nl_en <- cz_extract_sheet(path_wp_gidsinfo, sheet_name = "vertalingen NL-EN")

tbl_nipper_playlists <- cz_extract_sheet(path_wp_nipper_express, sheet_name = "playlists")
tbl_nipper_select <- cz_extract_sheet(path_wp_nipper_express, sheet_name = "nipper-select")
tbl_nipper_keys <- cz_extract_sheet(path_wp_nipper_express, sheet_name = "programma_sleutels")
tbl_nipper_audiolocaties <- cz_extract_sheet(path_wp_nipper_express, sheet_name = "audio_locaties")

# refactor raw tables -----------------------------------------------------

source("src/refactor_raw_tables.R")

# persist refactored tbles ------------------------------------------------

# saveRDS(tbl_zenderschema, file = paste0(config$cz_rds_store, "zenderschema.RDS"))
# saveRDS(tbl_presentatie, file = paste0(config$cz_rds_store, "presentatie.RDS"))
# saveRDS(tbl_montage, file = paste0(config$cz_rds_store, "montage.RDS"))
# saveRDS(tbl_itunes_cupboard, file = paste0(config$cz_rds_store, "itunes_cupboard.RDS"))
# saveRDS(tbl_wpgidsinfo, file = paste0(config$cz_rds_store, "gidsinfo.RDS"))
# saveRDS(tbl_wpgidsinfo_nl_en, file = paste0(config$cz_rds_store, "gidsinfo_nl_en.RDS"))
# 
# saveRDS(tbl_nipper_playlists, file = paste0(config$cz_rds_store, "nipper_playlists.RDS"))
# saveRDS(tbl_nipper_select, file = paste0(config$cz_rds_store, "nipper_select.RDS"))
# saveRDS(tbl_nipper_keys, file = paste0(config$cz_rds_store, "nipper_keys.RDS"))
# saveRDS(tbl_nipper_audiolocaties, file = paste0(config$cz_rds_store, "nipper_audiolocaties.RDS"))
