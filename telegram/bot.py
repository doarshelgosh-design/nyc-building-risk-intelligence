import os
import re
import requests

from dotenv import load_dotenv
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)


# ==================================================
# CONFIG
# ==================================================

load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

ES_URL = "http://localhost:9200"

BUILDING_INDEX = "building_risk_index"
PROPERTY_INDEX = "property_risk_index"


if not BOT_TOKEN:
    raise ValueError(
        "TELEGRAM_BOT_TOKEN was not found in .env"
    )


# ==================================================
# ADDRESS NORMALIZATION
# ==================================================

def normalize_address(value):
    """
    Normalize address before comparison.

    Example:
        " 2 West 120 Street "
            ->
        "2 WEST 120 STREET"
    """

    if not value:
        return None

    value = str(value).strip().upper()

    # Replace multiple spaces with one space
    value = re.sub(r"\s+", " ", value)

    return value


# ==================================================
# DISPLAY HELPER
# ==================================================

def display_value(value, default="N/A"):

    if value is None:
        return default

    if value == "":
        return default

    return value


# ==================================================
# ELASTICSEARCH - BUILDING SEARCH
# ==================================================

def search_building(address):

    normalized_input = normalize_address(address)

    query = {
        "size": 10,
        "_source": [
            "building_id",
            "bin",
            "property_id",
            "search_address",
            "current_address",
            "property_address",
            "address_aliases",
            "borough",
            "building_risk_score",
            "building_risk_level",
            "property_risk_score",
            "property_risk_level",
            "complaints_0_30",
            "hpd_active_violations",
            "dob_active_violations"
        ],
        "query": {
            "bool": {
                "should": [
                    {
                        "match_phrase": {
                            "search_address": address
                        }
                    },
                    {
                        "match_phrase": {
                            "current_address": address
                        }
                    },
                    {
                        "match_phrase": {
                            "property_address": address
                        }
                    },
                    {
                        "match_phrase": {
                            "address_aliases": address
                        }
                    }
                ],
                "minimum_should_match": 1
            }
        }
    }

    response = requests.post(
        f"{ES_URL}/{BUILDING_INDEX}/_search",
        json=query,
        timeout=10
    )

    response.raise_for_status()

    hits = response.json()["hits"]["hits"]

    # Elasticsearch can return similar addresses.
    # We do NOT trust ranking alone.
    # We verify that the returned address is actually
    # equal to the user's normalized address.
    for hit in hits:

        result = hit["_source"]

        candidate_addresses = [
            result.get("search_address"),
            result.get("current_address"),
            result.get("property_address")
        ]

        aliases = result.get("address_aliases") or []

        candidate_addresses.extend(aliases)

        normalized_candidates = [
            normalize_address(candidate)
            for candidate in candidate_addresses
            if candidate
        ]

        if normalized_input in normalized_candidates:
            return result

    return None


# ==================================================
# ELASTICSEARCH - PROPERTY SEARCH
# ==================================================

def search_property(address):

    normalized_input = normalize_address(address)

    query = {
        "size": 10,
        "_source": [
            "property_id",
            "bbl",
            "property_address",
            "search_address",
            "borough",
            "zipcode",
            "property_risk_score",
            "property_risk_level",
            "property_risk_source",
            "building_count",
            "max_building_risk_score",
            "avg_building_risk_score",
            "property_only_risk_score"
        ],
        "query": {
            "bool": {
                "should": [
                    {
                        "match_phrase": {
                            "search_address": address
                        }
                    },
                    {
                        "match_phrase": {
                            "property_address": address
                        }
                    }
                ],
                "minimum_should_match": 1
            }
        }
    }

    response = requests.post(
        f"{ES_URL}/{PROPERTY_INDEX}/_search",
        json=query,
        timeout=10
    )

    response.raise_for_status()

    hits = response.json()["hits"]["hits"]

    for hit in hits:

        result = hit["_source"]

        candidate_addresses = [
            result.get("search_address"),
            result.get("property_address")
        ]

        normalized_candidates = [
            normalize_address(candidate)
            for candidate in candidate_addresses
            if candidate
        ]

        if normalized_input in normalized_candidates:
            return result

    return None


# ==================================================
# /START COMMAND
# ==================================================

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    await update.message.reply_text(
        "🏙 NYC Building Risk Bot\n\n"
        "Send me a NYC address and I will check its risk.\n\n"
        "Example:\n"
        "2 WEST 120 STREET\n\n"
        "Use /help for more information."
    )


# ==================================================
# /HELP COMMAND
# ==================================================

async def help_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    await update.message.reply_text(
        "ℹ️ NYC Building Risk Bot Help\n\n"

        "Send me a NYC address and I will search "
        "for its risk information.\n\n"

        "Example:\n"
        "2 WEST 120 STREET\n\n"

        "🔎 Search logic:\n"
        "1️⃣ Exact Building match\n"
        "2️⃣ Property fallback if no Building is found\n"
        "3️⃣ No result is returned if the address "
        "cannot be matched safely\n\n"

        "🏢 Building results may include:\n"
        "• Building Risk Score\n"
        "• Risk Level\n"
        "• BIN\n"
        "• Borough\n"
        "• Property Risk\n"
        "• 311 complaints\n"
        "• HPD active violations\n"
        "• DOB active violations\n\n"

        "🏠 Property results may include:\n"
        "• Property Risk Score\n"
        "• Risk Level\n"
        "• BBL\n"
        "• Risk Source\n\n"

        "Commands:\n"
        "/start - Start the bot\n"
        "/help - Show this help message"
    )


# ==================================================
# ADDRESS HANDLER
# ==================================================

async def handle_address(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    address = update.message.text.strip()

    if not address:
        return

    try:

        # ==================================================
        # 1. SEARCH EXACT BUILDING
        # ==================================================

        building = search_building(address)

        if building:

            message = (
                "🏢 BUILDING FOUND\n\n"

                f"📍 Address: "
                f"{display_value(building.get('current_address'))}\n"

                f"🏙 Borough: "
                f"{display_value(building.get('borough'))}\n"

                f"🔢 BIN: "
                f"{display_value(building.get('bin'))}\n\n"

                f"⚠️ Building Risk: "
                f"{display_value(building.get('building_risk_score'))}\n"

                f"📊 Risk Level: "
                f"{display_value(building.get('building_risk_level'))}\n\n"

                f"🏠 Property Risk: "
                f"{display_value(building.get('property_risk_score'))}\n\n"

                f"📞 311 complaints (0-30 days): "
                f"{display_value(building.get('complaints_0_30'), 0)}\n"

                f"🏚 HPD active violations: "
                f"{display_value(building.get('hpd_active_violations'), 0)}\n"

                f"🏗 DOB active violations: "
                f"{display_value(building.get('dob_active_violations'), 0)}"
            )

            await update.message.reply_text(message)

            return


        # ==================================================
        # 2. PROPERTY FALLBACK
        # ==================================================

        property_data = search_property(address)

        if property_data:

            message = (
                "🏠 PROPERTY FOUND\n\n"

                f"📍 Address: "
                f"{display_value(property_data.get('property_address'))}\n"

                f"🏙 Borough: "
                f"{display_value(property_data.get('borough'))}\n"

                f"🔢 BBL: "
                f"{display_value(property_data.get('bbl'))}\n\n"

                f"⚠️ Property Risk: "
                f"{display_value(property_data.get('property_risk_score'))}\n"

                f"📊 Risk Level: "
                f"{display_value(property_data.get('property_risk_level'))}\n"

                f"📌 Risk Source: "
                f"{display_value(property_data.get('property_risk_source'))}"
            )

            await update.message.reply_text(message)

            return


        # ==================================================
        # 3. NO TRUSTED MATCH
        # ==================================================

        await update.message.reply_text(
            "❌ I could not find an exact match "
            "for this address.\n\n"
            "Please check the street number "
            "and street name.\n\n"
            "Example:\n"
            "2 WEST 120 STREET"
        )


    # ==================================================
    # ELASTICSEARCH ERROR
    # ==================================================

    except requests.exceptions.RequestException as e:

        print(
            "ELASTICSEARCH ERROR:",
            e
        )

        await update.message.reply_text(
            "⚠️ Elasticsearch is currently unavailable.\n"
            "Please try again later."
        )


    # ==================================================
    # GENERAL ERROR
    # ==================================================

    except Exception as e:

        print(
            "ERROR:",
            e
        )

        await update.message.reply_text(
            "⚠️ Something went wrong while searching."
        )


# ==================================================
# MAIN
# ==================================================

def main():

    app = (
        Application
        .builder()
        .token(BOT_TOKEN)
        .build()
    )

    # /start
    app.add_handler(
        CommandHandler(
            "start",
            start
        )
    )

    # /help
    app.add_handler(
        CommandHandler(
            "help",
            help_command
        )
    )

    # Address messages
    app.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            handle_address
        )
    )

    print(
        "Telegram bot is running..."
    )

    app.run_polling()


# ==================================================
# ENTRY POINT
# ==================================================

if __name__ == "__main__":
    main()