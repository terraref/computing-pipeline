require(bit64)
require(data.table)
plots <- fread("update_traits_cultivars.csv")
cultivar_id <- fread("cultivar_id.csv")

setnames(cultivar_id, 'id', 'cultivar_id')
plots2 <- merge(plots, cultivar_id, by = 'name')

sites <- fread("sites.csv")
setnames(sites, 'id', 'site_id')

sitenames <- plots2[!is.na(D_row),list(cultivar_id, sitename = paste0("MAC Field Scanner Season 2 Range ",Range,' Pass ', D_row))]

sites2 <- merge(sites, sitenames, by = 'sitename')

updates <- sites2[,list(update = paste0('update traits set cultivar_id = ', cultivar_id, ' where site_id = ', site_id, ';'))]

writeLines(updates$update, 'updates.sql')
