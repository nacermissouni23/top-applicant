"""
Location parser: splits raw location strings into city, region, country.

Handles patterns like:
  - "Austin, TX"
  - "Bengaluru, Karnataka, India"
  - "United States"
  - "Remote"
  - "México, Mexico"
"""

import re
from typing import Dict, Optional

# US state abbreviations → full names
US_STATES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
}

# Known countries (common in LinkedIn)
KNOWN_COUNTRIES = {
    "united states", "united states of america", "usa", "us",
    "canada", "united kingdom", "uk", "ireland",
    "india", "australia", "germany", "france", "spain", "italy",
    "netherlands", "sweden", "norway", "denmark", "finland",
    "brazil", "mexico", "singapore", "japan", "china", "south korea",
    "israel", "uae", "united arab emirates", "switzerland",
    "belgium", "austria", "poland", "portugal", "czech republic",
    "new zealand", "south africa", "argentina", "colombia", "chile",
    "philippines", "indonesia", "thailand", "vietnam", "malaysia",
    "taiwan", "hong kong", "nigeria", "kenya", "egypt", "saudi arabia",
}

# If US state abbreviation is the last part, country is US
US_STATE_ABBREVS = set(US_STATES.keys())


def parse_location(raw_location: str) -> Dict[str, Optional[str]]:
    """
    Parse a raw location string into structured components.
    
    Returns:
        dict with keys: city, region, country
        Values are None if not determinable.
    """
    result = {"city": None, "region": None, "country": None}

    if not raw_location or raw_location.strip().upper() == "N/A":
        return result

    raw = raw_location.strip()

    # Handle "Remote" variants
    if re.match(r"^remote\b", raw, re.IGNORECASE):
        result["country"] = "Remote"
        return result

    parts = [p.strip() for p in raw.split(",")]
    parts = [p for p in parts if p]  # Remove empty parts

    if len(parts) == 1:
        # Could be a country or just a region
        token = parts[0]
        if token.lower() in KNOWN_COUNTRIES:
            result["country"] = token
        elif token.upper() in US_STATE_ABBREVS:
            result["region"] = US_STATES.get(token.upper(), token)
            result["country"] = "United States"
        else:
            # Might be a country we don't recognize or a city
            result["country"] = token

    elif len(parts) == 2:
        # "Austin, TX" or "Toronto, Canada"
        part0, part1 = parts
        if part1.strip().upper() in US_STATE_ABBREVS:
            result["city"] = part0
            result["region"] = US_STATES.get(part1.strip().upper(), part1.strip())
            result["country"] = "United States"
        elif part1.strip().lower() in KNOWN_COUNTRIES:
            result["city"] = part0
            result["country"] = part1.strip()
        else:
            # "City, Region" — country unknown
            result["city"] = part0
            result["region"] = part1.strip()

    elif len(parts) >= 3:
        # "Bengaluru, Karnataka, India" or "New York, NY, United States"
        result["city"] = parts[0]
        result["region"] = parts[1].strip()
        result["country"] = parts[2].strip()

        # Normalize US state abbreviation in the region slot
        if result["region"].upper() in US_STATE_ABBREVS:
            result["region"] = US_STATES.get(result["region"].upper(), result["region"])

    return result
