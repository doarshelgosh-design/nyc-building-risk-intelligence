import requests


# --------------------------------------------------
# 1. NYC 311 API configuration
# --------------------------------------------------

API_URL = (
    "https://data.cityofnewyork.us/"
    "resource/erm2-nwe9.json"
)


# --------------------------------------------------
# 2. Test API connection and inspect sample records
# --------------------------------------------------

sample_params = {
    "$limit": 5,
    "$where": "agency='HPD'",
    "$order": "created_date DESC"
}


print("==================================")
print("NYC 311 CONNECTION TEST")
print("==================================")


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


if sample_data:

    print()
    print("FIELDS:")

    for field in sample_data[0].keys():
        print(
            f" - {field}"
        )


    print()
    print("LATEST RECORD:")

    latest_record = sample_data[0]

    print(
        f"Unique key: "
        f"{latest_record.get('unique_key')}"
    )

    print(
        f"Created date: "
        f"{latest_record.get('created_date')}"
    )

    print(
        f"Agency: "
        f"{latest_record.get('agency')}"
    )

    print(
        f"Complaint type: "
        f"{latest_record.get('complaint_type')}"
    )

    print(
        f"Borough: "
        f"{latest_record.get('borough')}"
    )

    print(
        f"BBL: "
        f"{latest_record.get('bbl')}"
    )


# --------------------------------------------------
# 3. Profile HPD complaint types
# --------------------------------------------------

print()
print("==================================")
print("NYC 311 HPD COMPLAINT PROFILE")
print("==================================")


profile_params = {

    "$select": (
        "complaint_type, "
        "count(*) as total"
    ),

    "$where": (
        "agency='HPD'"
    ),

    "$group": (
        "complaint_type"
    ),

    "$order": (
        "total DESC"
    ),

    "$limit": 100
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


print()

for row in profile_data:

    complaint_type = row.get(
        "complaint_type",
        "UNKNOWN"
    )

    total = row.get(
        "total",
        "0"
    )

    print(
        f"{complaint_type}: {total}"
    )


# --------------------------------------------------
# 4. Final result
# --------------------------------------------------

print()
print("==================================")
print("NYC 311 PROFILE COMPLETED")
print("==================================")