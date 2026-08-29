import requests


# --------------------------------------------------
# 1. DOB Safety Violations API
# --------------------------------------------------

API_URL = (
    "https://data.cityofnewyork.us/"
    "resource/855j-jady.json"
)


# --------------------------------------------------
# 2. Project date range
# --------------------------------------------------

START_DATE = "2025-08-25T00:00:00.000"
END_DATE = "2026-08-25T00:00:00.000"


# --------------------------------------------------
# 3. Test API connection
# --------------------------------------------------

print("==================================")
print("DOB SAFETY VIOLATIONS CONNECTION")
print("==================================")


sample_params = {
    "$limit": 5,
    "$order": "violation_issue_date DESC"
}


response = requests.get(
    API_URL,
    params=sample_params,
    timeout=60
)


print(
    f"STATUS: {response.status_code}"
)


response.raise_for_status()

sample_data = response.json()


print(
    f"ROWS: {len(sample_data)}"
)


# --------------------------------------------------
# 4. Display fields and first record
# --------------------------------------------------

if sample_data:

    first_record = sample_data[0]

    print()
    print("FIELDS:")

    for field in first_record.keys():
        print(
            f" - {field}"
        )

    print()
    print("LATEST RECORD:")

    print(
        f"Violation number: "
        f"{first_record.get('violation_number')}"
    )

    print(
        f"Issue date: "
        f"{first_record.get('violation_issue_date')}"
    )

    print(
        f"Violation type: "
        f"{first_record.get('violation_type')}"
    )

    print(
        f"Violation status: "
        f"{first_record.get('violation_status')}"
    )

    print(
        f"BIN: "
        f"{first_record.get('bin')}"
    )

    print(
        f"Borough: "
        f"{first_record.get('borough')}"
    )

    print(
        f"Block: "
        f"{first_record.get('block')}"
    )

    print(
        f"Lot: "
        f"{first_record.get('lot')}"
    )


# --------------------------------------------------
# 5. Profile violations for project period
# --------------------------------------------------

print()
print("==================================")
print("DOB VIOLATIONS PROFILE")
print("==================================")


profile_params = {

    "$select": (
        "violation_type, "
        "violation_status, "
        "count(*) as total"
    ),

    "$where": (
        f"violation_issue_date >= '{START_DATE}' "
        f"AND violation_issue_date < '{END_DATE}'"
    ),

    "$group": (
        "violation_type, violation_status"
    ),

    "$order": (
        "total DESC"
    ),

    "$limit": 500
}


profile_response = requests.get(
    API_URL,
    params=profile_params,
    timeout=60
)


print(
    f"STATUS: {profile_response.status_code}"
)


profile_response.raise_for_status()

profile_data = profile_response.json()


# --------------------------------------------------
# 6. Display profiling result
# --------------------------------------------------

total_records = 0


for row in profile_data:

    violation_type = row.get(
        "violation_type",
        "UNKNOWN"
    )

    violation_status = row.get(
        "violation_status",
        "UNKNOWN"
    )

    total = int(
        row.get(
            "total",
            0
        )
    )

    total_records += total

    print(
        f"Type: {violation_type} "
        f"| Status: {violation_status} "
        f"| Total: {total}"
    )


# --------------------------------------------------
# 7. Final summary
# --------------------------------------------------

print()
print("----------------------------------")

print(
    f"TOTAL DOB VIOLATIONS: "
    f"{total_records}"
)

print("----------------------------------")

print(
    "DOB PROFILE COMPLETED"
)

print("==================================")