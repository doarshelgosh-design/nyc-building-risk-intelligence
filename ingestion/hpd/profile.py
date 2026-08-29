import requests


# --------------------------------------------------
# 1. HPD Violations API
# --------------------------------------------------

API_URL = (
    "https://data.cityofnewyork.us/"
    "resource/wvxf-dwi5.json"
)


# --------------------------------------------------
# 2. Date range
# --------------------------------------------------

START_DATE = "2025-08-25T00:00:00.000"
END_DATE = "2026-08-25T00:00:00.000"


# --------------------------------------------------
# 3. Socrata query
# --------------------------------------------------

params = {
    "$select": (
        "class, "
        "violationstatus, "
        "count(*) as total"
    ),

    "$where": (
        f"inspectiondate >= '{START_DATE}' "
        f"AND inspectiondate < '{END_DATE}'"
    ),

    "$group": (
        "class, violationstatus"
    ),

    "$order": (
        "class, violationstatus"
    )
}


# --------------------------------------------------
# 4. Request data
# --------------------------------------------------

response = requests.get(
    API_URL,
    params=params,
    timeout=60
)

print(
    f"STATUS: {response.status_code}"
)


# --------------------------------------------------
# 5. Stop if API returned an error
# --------------------------------------------------

response.raise_for_status()


# --------------------------------------------------
# 6. Read result
# --------------------------------------------------

data = response.json()


# --------------------------------------------------
# 7. Display profiling result
# --------------------------------------------------

print()
print("==================================")
print("HPD VIOLATIONS PROFILE")
print("==================================")

for row in data:

    violation_class = row.get(
        "class",
        "UNKNOWN"
    )

    violation_status = row.get(
        "violationstatus",
        "UNKNOWN"
    )

    total = row.get(
        "total",
        "0"
    )

    print(
        f"Class: {violation_class} "
        f"| Status: {violation_status} "
        f"| Total: {total}"
    )

print("==================================")