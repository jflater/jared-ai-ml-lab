suppressPackageStartupMessages(library(tidyverse))

# Build a CTE to identify high-complexity residents (frequent care
# events, long stays, multiple services)

residents <- read_csv("data/raw/residents.csv", show_col_types = FALSE)
care_events <- read_csv("data/raw/care_events.csv", show_col_types = FALSE)

care_events <- care_events |>
  left_join(residents, by = c("resident_id", "facility_id")) |>
  group_by(resident_id, first_name, last_name, facility_id) |>
  summarize(
    care_event_count = n(),
    num_event_types = n_distinct(event_type),
    avg_event_duration = mean(duration_minutes, na.rm = TRUE),
    .groups = "drop"
  )

resident_tenure <- residents |>
  group_by(resident_id) |>
  mutate(discharge_date = if_else(
    is.na(discharge_date),
    Sys.Date(), discharge_date
  )) |>
  summarize(
    length_of_stay = as.numeric(
      difftime(discharge_date, admission_date, units = "days")
    ),
    .groups = "drop"
  )

resident_complexity <- care_events |>
  left_join(resident_tenure, by = "resident_id") |>
  mutate(complexity_score =
           (care_event_count / 100) +
           (length_of_stay / 100) +
           (num_event_types / 5)) |>
  select(resident_id, first_name, last_name, facility_id, complexity_score) |>
  arrange(desc(complexity_score))

billing <- read_csv("data/raw/billing.csv", show_col_types = FALSE)
str(billing)
unique(billing$payment_status)

billing <- billing |>
  mutate(
    status_flag = case_when(
      payment_status == "Overdue" ~ "Overdue",
      TRUE ~ payment_status
    )
  ) |>
  group_by(resident_id) |>
  summarise(
    total_billing = sum(total_charge),
    payment_status = first(status_flag),
    .groups = "drop"
  )
# Show high complexity residents with overdue payment
resident_complexity_revenue <- resident_complexity |>
  left_join(billing, by = "resident_id") |>
  arrange(desc(total_billing))

print(resident_complexity_revenue)
