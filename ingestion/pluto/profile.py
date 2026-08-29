import requests


# --------------------------------------------------
# 1. PLUTO API configuration
# --------------------------------------------------

API_URL = (
    "https://data.cityofnewyork.us/"
    "resource/64uk-42ks.json"
)


# --------------------------------------------------
# 2. Test API connection
# --------------------------------------------------

print("==================================")
print("PLUTO CONNECTION TEST")
print("==================================")


sample_params = {
    "$limit": 5
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
# 3. Display available fields
# --------------------------------------------------

if sample_data:

    first_record = sample_data[0]

    print()
    print("FIELDS:")

    for field in first_record.keys():
        print(
            f" - {field}"
        )


    # --------------------------------------------------
    # 4. Display important building fields
    # --------------------------------------------------

    print()
    print("SAMPLE RECORD:")

    fields_to_check = [
        "borough",
        "block",
        "lot",
        "bbl",
        "address",
        "zipcode",
        "landuse",
        "bldgclass",
        "ownername",
        "lotarea",
        "bldgarea",
        "numbldgs",
        "numfloors",
        "unitsres",
        "unitstotal",
        "yearbuilt",
        "yearalter1",
        "yearalter2",
        "latitude",
        "longitude",
    ]

    for field in fields_to_check:

        print(
            f"{field}: "
            f"{first_record.get(field)}"
        )


# --------------------------------------------------
# 5. Count total PLUTO records
# --------------------------------------------------

print()
print("==================================")
print("PLUTO RECORD COUNT")
print("==================================")


count_params = {
    "$select": "count(*) as total"
}


count_response = requests.get(
    API_URL,
    params=count_params,
    timeout=60
)


print(
    f"STATUS: {count_response.status_code}"
)


count_response.raise_for_status()

count_data = count_response.json()


if count_data:

    print(
        f"TOTAL RECORDS: "
        f"{count_data[0].get('total')}"
    )


# --------------------------------------------------
# 6. Profile land use
# --------------------------------------------------

print()
print("==================================")
print("PLUTO LAND USE PROFILE")
print("==================================")


landuse_params = {

    "$select": (
        "landuse, "
        "count(*) as total"
    ),

    "$group": "landuse",

    "$order": "total DESC"
}


landuse_response = requests.get(
    API_URL,
    params=landuse_params,
    timeout=60
)


print(
    f"STATUS: {landuse_response.status_code}"
)


landuse_response.raise_for_status()

landuse_data = landuse_response.json()


for row in landuse_data:

    landuse = row.get(
        "landuse",
        "UNKNOWN"
    )

    total = row.get(
        "total",
        "0"
    )

    print(
        f"Land Use: {landuse} "
        f"| Total: {total}"
    )


# --------------------------------------------------
# 7. Final result
# --------------------------------------------------

print()
print("==================================")
print("PLUTO PROFILE COMPLETED")
print("==================================")
