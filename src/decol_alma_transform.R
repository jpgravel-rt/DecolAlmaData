library(sparklyr)
library(zoo)
library(stringr)

align_data <- function(sc, data, sampling_period) {

  period <- data %>% 
    summarise(from = min(ts, na.rm = T), to = max(ts, na.rm = T)) %>%
    collect()
  
  time_seq = sdf_seq(sc, 
                     from = floor(as.numeric(period$from)),
                     to = ceiling(as.numeric(period$to)),
                     by = as.numeric(sampling_period))
  
  data_with_id <- data %>% 
    mutate(id = as.numeric(ts)) %>%
    filter(str_trim(strval) == "")
    select(-ts)
  
  complete_data <- time_seq %>%
    full_join(data_with_id, by = id)
  
  interpolated_data <- complete_data %>%
    mutate(ts = as.POSIXct(id), numval = na.approx(x = id, xout = numval)) %>%
    select(ts, tag, numval)
  
}