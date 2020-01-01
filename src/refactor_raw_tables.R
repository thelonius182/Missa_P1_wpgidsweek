library(stringr)
library(dplyr)
library(lubridate)


# zenderschema ------------------------------------------------------------

tbl_zenderschema <- tbl_raw_zenderschema %>% 
  mutate(start = str_pad(string = start, side = "left", width = 5, pad = "0"), 
         slot = paste0(str_sub(dag, start = 1, end = 2), start)
  ) %>% 
  rename(hh_formule = `hhOffset-dag.uur`,
         balk = Balk,
         balk_tonen = Toon,
         wekelijks = `elke week`,
         product_wekelijks = te,
         bijzonderheden_wekelijks = be,
         AB_cyclus = `twee-wekelijks`,
         product_A_cyclus = A,
         bijzonderheden_A_cyclus = ba,
         product_B_cyclus = B,
         bijzonderheden_B_cyclus = bb, 
         week_1 = `week 1`,
         product_week_1 = t1,
         bijzonderheden_week_1 = b1,
         week_2 = `week 2`,
         product_week_2 = t2,
         bijzonderheden_week_2 = b2,
         week_3 = `week 3`,
         product_week_3 = t3,
         bijzonderheden_week_3 = b3,
         week_4 = `week 4`,
         product_week_4 = t4,
         bijzonderheden_week_4 = b4,
         week_5 = `week 5`,
         product_week_5 = t5,
         bijzonderheden_week_5 = b5) %>%
  select(-starts_with("r"), -dag, -start) %>% 
  select(slot, hh_formule, everything())


# presentatie-rooster -----------------------------------------------------

tbl_presentatie <-  tbl_raw_presentatie %>% 
  filter(row_number() > 1) %>% 
  select(uitzending = B, uren, pres = Presentatie, tech = Techniek, live = Status) %>% 
  mutate(live = if_else(live == "Live", T, F))


# montage-rooster ---------------------------------------------------------

tbl_montage <- tbl_raw_montage %>% 
  mutate(start_duur = str_pad(string = Tijd.Duur, side = "left", width = 5, pad = "0"), 
         dag = wday(Uitzending, label = T, abbr = T),
         slot = paste0(dag, start_duur)
  ) %>% 
  select(uitzending = Uitzending,
         slot,
         titel = Titel,
         product = Type
  )


# iTunes Cupboard ---------------------------------------------------------

tbl_itunes_cupboard <- tbl_raw_itunes_cupboard


# WordPress Gidsinfo ------------------------------------------------------

tbl_wpgidsinfo <- tbl_raw_wpgidsinfo %>% 
  select(key_modelrooster = `key-modelrooster`,
         redactie_NL = `redactie-NL`,
         titel_NL = `titel-NL`,
         titel_EN = `titel-EN`,
         slug,
         productie_taak = `productie-1-taak`,
         productie_mdw = `productie-1-mdw`,
         productie = uitzendtype,
         genre_NL1 = `genre-1-NL`,
         genre_NL2 = `genre-2-NL`,
         intro_NL = `std.samenvatting-NL`,
         intro_EN = `std.samenvatting-EN`) %>% 
  arrange(key_modelrooster)


# Vertalingen WP-gidsinfo -------------------------------------------------

tbl_wpgidsinfo_nl_en <- tbl_raw_wpgidsinfo_nl_en %>% 
  select(item_NL = `item-NL`,
         item_EN = `item-EN`) %>% 
  arrange(item_NL)


# Opruimen ----------------------------------------------------------------

rm(
  tbl_raw_zenderschema,
  tbl_raw_presentatie,
  tbl_raw_montage,
  tbl_raw_itunes_cupboard,
  tbl_raw_wpgidsinfo,
  tbl_raw_wpgidsinfo_nl_en
)
