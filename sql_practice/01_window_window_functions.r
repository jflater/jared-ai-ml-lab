suppressPackageStartupMessages({
  library(tidyverse)
  library(lubridate)
})

residents <- read_csv("data/raw/residents.csv", show_col_types = FALSE)

# Which residents are nearing discharge? Who's been here longest?
# Use window functions to rank and compute durations.

result <- residents |>
  mutate(
    # Calculate length of stay in days
    length_of_stay = as.numeric(
      difftime(
        coalesce(discharge_date, Sys.Date()),
        admission_date,
        units = "days"
      )
    )
  ) |>
  arrange(admission_date) |>
  mutate(
    # Global row number ordered by admission date
    admission_order = row_number()
  ) |>
  group_by(facility_id) |>
  arrange(admission_date, .by_group = TRUE) |>
  mutate(
    # Rank by longest stay within each facility
    longest_stay_rank_by_facility = rank(desc(length_of_stay)),
    # Previous admission date within facility
    prev_resident_admission_date = lag(admission_date),
    # Cumulative resident days within facility
    cumulative_resident_days = cumsum(length_of_stay)
  ) |>
  ungroup() |>
  arrange(desc(length_of_stay))

result

result |>
  group_by(facility_id) |>
  summarize(
    total_residents = n(),
    average_length_of_stay = mean(length_of_stay, na.rm = TRUE),
    max_length_of_stay = max(length_of_stay, na.rm = TRUE)
  )
