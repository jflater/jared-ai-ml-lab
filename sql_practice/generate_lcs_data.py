"""
Generate synthetic LCS (senior living) dataset.
Mimics real operational data: residents, facilities, admissions, care events, billing.
"""
 
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
 
# Set seed for reproducibility
np.random.seed(42)
random.seed(42)
 
# Parameters
NUM_FACILITIES = 5
NUM_RESIDENTS = 150
NUM_STAFF = 40
DAYS_BACK = 365  # 1 year of history
 
start_date = datetime.now() - timedelta(days=DAYS_BACK)

# ============================================================================
# 1. FACILITIES
# ============================================================================
facilities = pd.DataFrame({
    'facility_id': range(1, NUM_FACILITIES + 1),
    'facility_name': [
        'Sunrise Senior Living - Des Moines',
        'Brookdale - Cedar Rapids',
        'Waverly Oaks - Ames',
        'Golden Years - Iowa City',
        'Heritage Springs - Waterloo'
    ],
    'city': ['Des Moines', 'Cedar Rapids', 'Ames', 'Iowa City', 'Waterloo'],
    'state': ['IA'] * NUM_FACILITIES,
    'beds_total': [120, 95, 80, 110, 100],
    'facility_type': ['Independent Living', 'Assisted Living', 'Memory Care', 'Skilled Nursing', 'Assisted Living']
})
 
# ============================================================================
# 2. RESIDENTS
# ============================================================================
resident_ids = range(1, NUM_RESIDENTS + 1)
facility_ids = np.random.choice(facilities['facility_id'], size=NUM_RESIDENTS)
 
# Generate admission dates over past year
admission_dates = [start_date + timedelta(days=random.randint(0, DAYS_BACK)) for _ in range(NUM_RESIDENTS)]
 
# Discharge dates (some residents discharged, some still active)
discharge_dates = []
for i, adm_date in enumerate(admission_dates):
    if random.random() < 0.15:  # 15% discharged
        discharge_dates.append(adm_date + timedelta(days=random.randint(30, 300)))
    else:
        discharge_dates.append(None)  # Still resident
 
residents = pd.DataFrame({
    'resident_id': resident_ids,
    'facility_id': facility_ids,
    'first_name': np.random.choice(['Mary', 'John', 'Patricia', 'James', 'Robert', 'Michael', 'Linda', 'Barbara', 'Elizabeth'], NUM_RESIDENTS),
    'last_name': np.random.choice(['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez'], NUM_RESIDENTS),
    'age': np.random.randint(65, 102, NUM_RESIDENTS),
    'gender': np.random.choice(['M', 'F'], NUM_RESIDENTS),
    'admission_date': admission_dates,
    'discharge_date': discharge_dates,
    'care_level': np.random.choice(['Independent', 'Assisted', 'Memory Care', 'Skilled Nursing'], NUM_RESIDENTS),
    'monthly_rate': np.random.choice([3500, 4500, 5500, 6500, 7500], NUM_RESIDENTS),
})

# ============================================================================
# 3. STAFF
# ============================================================================
staff = pd.DataFrame({
    'staff_id': range(1, NUM_STAFF + 1),
    'facility_id': np.random.choice(facilities['facility_id'], size=NUM_STAFF),
    'first_name': np.random.choice(['John', 'Sarah', 'Michael', 'Jennifer', 'David', 'Lisa'], NUM_STAFF),
    'last_name': np.random.choice(['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia'], NUM_STAFF),
    'role': np.random.choice(['CNA', 'RN', 'LPN', 'Care Coordinator', 'Dietary', 'Housekeeping'], NUM_STAFF),
    'hire_date': [start_date + timedelta(days=random.randint(0, DAYS_BACK)) for _ in range(NUM_STAFF)],
})

# ============================================================================
# 4. ADMISSIONS (Events)
# ============================================================================
admissions = []
for idx, row in residents.iterrows():
    admissions.append({
        'admission_id': row['resident_id'] * 1000 + 1,  # Simple ID
        'resident_id': row['resident_id'],
        'facility_id': row['facility_id'],
        'admission_date': row['admission_date'],
        'admission_type': np.random.choice(['New', 'Readmission', 'Transfer'], 1)[0],
        'primary_reason': np.random.choice(['Assisted Living', 'Recovery', 'Memory Care', 'Skilled Nursing'], 1)[0],
    })
 
admissions_df = pd.DataFrame(admissions)

# ============================================================================
# 5. CARE EVENTS (Daily interactions: medications, activities, assessments)
# ============================================================================
care_events = []
event_id = 1
 
for idx, resident in residents.iterrows():
    resident_id = resident['resident_id']
    start_care = resident['admission_date']
    end_care = resident['discharge_date'] if pd.notna(resident['discharge_date']) else datetime.now()
    
    # 2-5 events per day per resident
    num_days = (end_care - start_care).days
    num_events = random.randint(int(num_days * 2), int(num_days * 5))
    
    for _ in range(num_events):
        event_date = start_care + timedelta(days=random.randint(0, (end_care - start_care).days))
        
        care_events.append({
            'care_event_id': event_id,
            'resident_id': resident_id,
            'facility_id': resident['facility_id'],
            'event_date': event_date,
            'event_type': np.random.choice(['Medication', 'Activity', 'Meal', 'Assessment', 'Hygiene', 'Therapy'], 1)[0],
            'staff_id': np.random.choice(staff[staff['facility_id'] == resident['facility_id']]['staff_id'].values),
            'duration_minutes': np.random.randint(5, 120),
            'notes': '',  # Could populate with synthetic text
        })
        event_id += 1
 
care_events_df = pd.DataFrame(care_events)

# ============================================================================
# 6. BILLING (Monthly charges)
# ============================================================================
billing = []
billing_id = 1
 
for idx, resident in residents.iterrows():
    resident_id = resident['resident_id']
    monthly_rate = resident['monthly_rate']
    start_bill = resident['admission_date'].replace(day=1)  # Bill from first of month
    end_bill = resident['discharge_date'] if pd.notna(resident['discharge_date']) else datetime.now()
    
    current_month = start_bill
    while current_month < end_bill:
        billing.append({
            'billing_id': billing_id,
            'resident_id': resident_id,
            'facility_id': resident['facility_id'],
            'billing_month': current_month.replace(day=1),
            'base_charge': monthly_rate,
            'additional_services': np.random.randint(0, 1500),  # PT, OT, etc.
            'total_charge': monthly_rate + np.random.randint(0, 1500),
            'payment_status': np.random.choice(['Paid', 'Pending', 'Overdue'], 1, p=[0.85, 0.10, 0.05])[0],
            'payment_date': current_month + timedelta(days=random.randint(1, 30)) if random.random() < 0.9 else None,
        })
        billing_id += 1
        current_month += timedelta(days=30)
 
billing_df = pd.DataFrame(billing)

# ============================================================================
# SAVE TO CSV
# ============================================================================
import os
os.makedirs('data/raw', exist_ok=True)
 
facilities.to_csv('data/raw/facilities.csv', index=False)
residents.to_csv('data/raw/residents.csv', index=False)
staff.to_csv('data/raw/staff.csv', index=False)
admissions_df.to_csv('data/raw/admissions.csv', index=False)
care_events_df.to_csv('data/raw/care_events.csv', index=False)
billing_df.to_csv('data/raw/billing.csv', index=False)
 
print("✓ Synthetic LCS dataset generated:")
print(f"  - facilities.csv ({len(facilities)} rows)")
print(f"  - residents.csv ({len(residents)} rows)")
print(f"  - staff.csv ({len(staff)} rows)")
print(f"  - admissions.csv ({len(admissions_df)} rows)")
print(f"  - care_events.csv ({len(care_events_df)} rows)")
print(f"  - billing.csv ({len(billing_df)} rows)")
print("\nAll files saved to data/raw/")

