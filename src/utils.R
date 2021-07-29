

library(sparklyr)
library(dplyr)
library(lubridate)


#TODO: update parameters
interpolate <- function(sdf, sc, discretization = 1) {
  
  #TODO: Convert timestamp (ts) to a unix time variable called 'time'.
  #TODO: Alingn the discretization with the whole time in seconds, ex: if disc is 5: 0,5,10,15... not 2,7,12,17
  #TODO: Make sure the discretized time is contained within non NA sampled data
  
  
  # Get the time period of the samplings
  period = sdf %>% 
    summarise(start=min(time), end=max(time)) %>% 
    collect()
  
  # Create a full set of discrete time values
  series <- sdf_seq(sc, from=period$start, to=period$end, by=discretization) %>% 
    select(time=id)
  
  
  
  # The target size is not the series size since the discretization and the actual
  # samples may end up with interleaved entries. Thus, the number of entries to 
  # reach is the result of a full join.
  target_size <- (sdf %>% 
                    full_join(series) %>% 
                    count() %>% 
                    collect())$n
  
  # Add interpolated values until the dataframe contains the same number of
  # values as the target size.
  sdf_size <- (sdf %>% count() %>% collect())$n
  while (sdf_size < target_size) {
    print(paste0("Interpolation ", round(sdf_size / target_size * 100, 2), "%"))
    sdf <- sdf %>%
      full_join(series) %>%  # May add multiple consecutive NAs.
      arrange(time) %>%      # Sort by time.
      filter(!is.na(numval) | !is.na(lag(numval))) %>%  # Keep 1 NA between 2 val
      mutate(numval=if_else(is.na(numval),              # Interpolate if NA
                            lag(numval) +                     # y0 +
                              (time - lag(time)) *            # (x - x0) *
                              (lead(numval) - lag(numval)) /  # (y1 - y0) /
                              (lead(time) - lag(time)),       # (x1 - x0)
                            numval))
    sdf_size <- (sdf %>% count() %>% collect())$n
  }
  
  # Remove all entries that do not match the target series discretization
  sdf <- sdf %>% inner_join(series)
  
  print("Interpolation 100%")
  
  
}