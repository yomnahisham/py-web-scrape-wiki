import os
from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup, Tag
import pymysql
from datetime import datetime
import re
import csv
import concurrent.futures

# load env variables for db connection
load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
    'charset': os.getenv('DB_CHARSET')
}

# function to connect to the database
def connect_db():
    return pymysql.connect(**DB_CONFIG)

# function to insert venue into db
def insert_venue(venue_list):
    conn = connect_db()
    cursor = conn.cursor()
    
    for venue in venue_list:
        # Remove empty or whitespace-only items.
        venue = [v.strip() for v in venue if v.strip()]
        
        # Check if we have at least 3 fields and one of the first two is "coconut grove".
        if len(venue) >= 3 and ("coconut grove" in [venue[0].lower(), venue[1].lower()] or "cocoanut grove" in [venue[0].lower(), venue[1].lower()]):
            # Force: neighborhood = "Coconut Grove", venue_name = the other of the first two.
            if venue[0].lower() == "coconut grove":
                neighborhood = venue[0]
                venue_name = venue[1]
            else:
                neighborhood = venue[1]
                venue_name = venue[0]
            city = venue[2]
            if len(venue) >= 5:
                state = venue[3]
                country = venue[4]
            elif len(venue) == 4:
                state = venue[3]
                country = "U.S."
            else:
                state = "California"
                country = "U.S."
        elif len(venue) == 1:
            # Format: [venue_name]
            venue_name = venue[0]
            neighborhood = None
            city = None
            state = "California"
            country = "U.S."
        elif len(venue) == 2:
            # Format: [venue_name, city]
            venue_name, city = venue
            neighborhood = None
            state = "California"
            country = "U.S."
        elif len(venue) == 3:
            if venue[1].lower() == "hollywood":
                venue_name, neighborhood, state = venue
                city = "Los Angeles"
                country = "U.S."
            else:
                # Format: [venue_name, city, state]
                venue_name, city, state = venue
                neighborhood = None
                country = "U.S."
        elif len(venue) == 4:
            if venue[1].lower() == "hollywood":
                venue_name, neighborhood, state, country = venue
                city = "Los Angeles"
            else:
                # Format: [venue_name, city, state, country]
                venue_name, city, state, country = venue
                neighborhood = None
        elif len(venue) >= 5:
            # Format: [venue_name, neighborhood, city, state, country] (ignore extras)
            venue_name, neighborhood, city, state, country = venue[:5]
            # if neighborhood equals venue_name (ignoring case), clear it
            if neighborhood and venue_name.lower() == neighborhood.lower():
                neighborhood = None
        else:
            print("Invalid venue format:", venue)
            continue

        # Normalize the venue name by removing a leading "the " (case-insensitive)
        norm_venue_name = re.sub(r'^the\s+', '', venue_name, flags=re.IGNORECASE).lower()
        # Build two variants: one without and one with "the " prefix.
        variant1 = norm_venue_name
        variant2 = "the " + norm_venue_name

        # only compare venue names for duplicates
        select_query = """
            SELECT venue_id
            FROM venue
            WHERE LOWER(venue_name) = %s OR LOWER(venue_name) = %s
        """
        cursor.execute(select_query, (variant1, variant2))
            
        result = cursor.fetchone()
        if result is None:
            cursor.execute(
                "INSERT INTO venue (venue_name, neighborhood, city, state, country) VALUES (%s, %s, %s, %s, %s)",
                (venue_name, neighborhood, city, state, country)
            )
        else:
            print(f"Venue '{venue_name}' already exists (ID: {result[0]}).")
            
    conn.commit()
    cursor.close()
    conn.close()

# function to insert person into db
def insert_person(person_list, person_info):
    conn = connect_db()
    cursor = conn.cursor()
    
    for person in person_list:
        # Remove empty or whitespace-only items.
        person = [p.strip() for p in person if p.strip()]
        first_name = person[0]
        middle_name = person[1] if len(person) == 3 else None
        last_name = person[2] if len(person) == 3 else person[1]
        date_of_birth = person_info[0]
        birth_country = person_info[1]
        date_of_death = person_info[2]

        if date_of_birth is not None:
            select_query = """
                SELECT person_id
                FROM person
                WHERE first_name = %s AND last_name = %s AND birthDate = %s
            """
            cursor.execute(select_query, (first_name, last_name, date_of_birth))
        else: 
            select_query = """
                SELECT person_id
                FROM person
                WHERE first_name = %s AND last_name = %s
            """
            cursor.execute(select_query, (first_name, last_name))
        if cursor.fetchone() is None:
            cursor.execute(
                "INSERT INTO person (first_name, middle_name, last_name, birthDate, country, deathDate) VALUES (%s, %s, %s, %s, %s, %s)",
                (first_name, middle_name, last_name, date_of_birth, birth_country, date_of_death)
            )
        else:
            print(f"Person '{first_name} {last_name}' already exists.")
    conn.commit()
    cursor.close()
    conn.close()

# function to get the venue id
def get_venue_id(venue_name):
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute("SELECT venue_id FROM venue WHERE venue_name = %s", (venue_name,))
    venue_id = cursor.fetchone()
    cursor.close()
    conn.close()
    return venue_id

# function to insert award into db
def insert_award(n, event_date, venue_ids, duration, network):
    conn = connect_db()
    cursor = conn.cursor()
    
    # ensure network is a string
    network_param = ', '.join(network) if isinstance(network, list) else network

    for venue_id in venue_ids:
        # extract the actual venue id from the tuple if necessary
        vid = venue_id[0] if isinstance(venue_id, tuple) else venue_id
        cursor.execute(
            "SELECT award_edition_id FROM award_edition WHERE edition = %s AND venue_id = %s AND network = %s",
            (n, vid, network_param)
        )
        if cursor.fetchone() is not None:
            print(f"Award {n} at venue {vid} already exists.")
        else:
            cursor.execute(
                "INSERT INTO award_edition (edition, aYear, cDate, venue_id, duration, network) VALUES (%s, %s, %s, %s, %s, %s)",
                (
                    n,
                    datetime.strptime(format_date(event_date), "%Y-%m-%d").year,
                    format_date(event_date),
                    vid,
                    duration,
                    network_param
                )
            )
    conn.commit()
    cursor.close()
    conn.close()

# function to insert award into a CSV file
def insert_award_csv(n, event_date, venue_ids, duration, network, csv_file="awards.csv"):

    # Ensure network is a string.
    network_param = ', '.join(network) if isinstance(network, list) else network

    # Prepare data for each venue
    rows = []
    for venue_id in venue_ids:
        vid = venue_id[0] if isinstance(venue_id, tuple) else venue_id
        try:
            formatted_date = format_date(event_date)
            rows.append({
                "Edition": n,
                "Year": datetime.strptime(formatted_date, "%Y-%m-%d").year,
                "Date": formatted_date,
                "Venue ID": vid,
                "Duration": duration,
                "Network": network_param
            })
        except Exception as e:
            print(f"Error formatting date for event {n}: {e}")
            rows.append({
                "Edition": n,
                "Year": "Error",
                "Date": f"Error: {e}",
                "Venue ID": vid,
                "Duration": duration,
                "Network": network_param
            })

    # write to CSV
    file_exists = os.path.isfile(csv_file)
    with open(csv_file, mode="a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=["Edition", "Year", "Date", "Venue ID", "Duration", "Network"])
        if not file_exists:
            writer.writeheader()  # write header only if file doesn't exist
        writer.writerows(rows)
    print(f"Award {n} details written to {csv_file}.")

# function to insert new positions into the db
def insert_position(position_list):
    conn = connect_db()
    cursor = conn.cursor()

    for position in position_list:
        position_title = position
        if position_title:
            cursor.execute(
                "SELECT position_id FROM positions WHERE title = %s", (position_title,)
            )
            already_exists = cursor.fetchone()
            if already_exists is None:
                cursor.execute(
                    "INSERT INTO positions (title) VALUES (%s)", (position_title,)
                )
            else: 
                print(f"Positons {position_title} already exists.")
        else:
            print("Failed to get position title from position list.")
    
    conn.commit()
    cursor.close()
    conn.close()

# function to insert the person, positon, and award connection into the db
def insert_person_connection(connection_list):
    conn = connect_db()
    cursor = conn.cursor()
    for connection in connection_list:
        award_num, first_name, last_name, date_of_birth, position = connection
        # Fetch person_id based on first name, last name, and date of birth
        if date_of_birth:
            cursor.execute(
                "SELECT person_id FROM person WHERE first_name = %s AND last_name = %s AND birthDate = %s",
                (first_name, last_name, date_of_birth)
            )
        else:
            cursor.execute(
                "SELECT person_id FROM person WHERE first_name = %s AND last_name = %s",
                (first_name, last_name)
            )
        person_id = cursor.fetchone()

        # fetch award_id based on award number
        cursor.execute(
            "SELECT award_edition_id FROM award_edition WHERE edition = %s",
            (award_num,)
        )
        award_id = cursor.fetchone()

        # fetch position_id based on position
        cursor.execute(
            "SELECT position_id FROM positions WHERE title = %s",
            (position,)
        )
        position_id = cursor.fetchone()

        # Use logical AND (and) instead of bitwise (&)
        if person_id and award_id and position_id:
            person_id = person_id[0]
            award_id = award_id[0]
            position_id = position_id[0]
            # check if the connection already exists
            cursor.execute(
                "SELECT * FROM award_edition_person WHERE award_id = %s AND person_id = %s AND position_id = %s",
                (award_id, person_id, position_id)
            )
            if cursor.fetchone() is None:
                # Insert the connection into the database
                cursor.execute(
                    "INSERT INTO award_edition_person (award_id, person_id, position_id) VALUES (%s, %s, %s)",
                    (award_id, person_id, position_id)
                )
            else:
                print(f"Connection for award {award_num}, person {first_name} {last_name}, position {position} already exists.")
        else:
            print(f"Missing data for award {award_num}, person {first_name} {last_name}, position {position}.")

    conn.commit()
    cursor.close()
    conn.close()


def insert_movie_person(connection_list):
    conn = connect_db()
    cursor = conn.cursor()

    for connection in connection_list:
        movie_name, first_name, last_name, date_of_birth, position = connection
        # fetch person_id based on first name, last name, and date of birth
        if date_of_birth:
            cursor.execute(
                "SELECT person_id FROM person WHERE first_name = %s AND last_name = %s AND birthDate = %s",
                (first_name, last_name, date_of_birth)
            )
        else:
            cursor.execute(
                "SELECT person_id FROM person WHERE first_name = %s AND last_name = %s",
                (first_name, last_name)
            )
        person_id = cursor.fetchone()

        # fetch movie_id based on movie_name 
        cursor.execute(
            "SELECT movie_id FROM movie WHERE movie_name = %s",
            (movie_name,)
        )
        movie_id = cursor.fetchone()

        # fetch position_id based on position
        cursor.execute(
            "SELECT position_id FROM positions WHERE title = %s",
            (position,)
        )
        position_id = cursor.fetchone()

        # Use logical AND (and) instead of bitwise (&)
        if person_id and movie_id and position_id:
            person_id = person_id[0]
            movie_id = movie_id[0]
            position_id = position_id[0]
            # check if the connection already exists
            cursor.execute(
                "SELECT * FROM movie_crew WHERE movie_id = %s AND person_id = %s AND position_id = %s",
                (movie_id, person_id, position_id)
            )
            if cursor.fetchone() is None:
                # Insert the connection into the database
                cursor.execute(
                    "INSERT INTO movie_crew (movie_id, person_id, position_id) VALUES (%s, %s, %s)",
                    (movie_id, person_id, position_id)
                )
            else:
                print(f"Connection for award {movie_name}, person {first_name} {last_name}, position {position} already exists.")
        else:
            print(f"Missing data for award {movie_name}, person {first_name} {last_name}, position {position}.")

    conn.commit()
    cursor.close()
    conn.close()

#function to insert the movie details gathered from scrape_movie_details
def insert_movie(movie_name, release_dates, in_language, run_time, country, production_companies):
    conn = connect_db()
    cursor = conn.cursor()

    for release_date in release_dates:
        cursor.execute(
            "SELECT * FROM movie WHERE movie_name = %s AND release_date = %s", (movie_name, release_date)
        )
        if cursor.fetchone() is None:
            cursor.execute(
                "INSERT INTO movie (movie_name, release_date, in_language, run_time, country) VALUES (%s, %s, %s, %s, %s)",
                (movie_name, release_date, in_language, run_time, country)
            )
        else: 
            print(f"Movie {movie_name} already exists.") 
            continue
    
    for company in production_companies:
        cursor.execute(
            "SELECT pd_id FROM production_company WHERE company_name = %s", (company)
        )
        company_id = cursor.fetchone()

        if company_id:
            for release_date in release_dates:
                cursor.execute(
                    "SELECT movie_id FROM movie WHERE movie_name = %s AND release_date = %s", (movie_name, release_date)
                )
                movie_id = cursor.fetchone()
                if movie_id and company_id:
                    cursor.execute(
                        "SELECT * FROM movie_produced_by WHERE movie_id = %s AND pd_id = %s", (movie_id[0], company_id[0])
                    )
                    if cursor.fetchone() is None:
                        cursor.execute(
                            "INSERT INTO movie_produced_by (movie_id, pd_id) VALUES (%s, %s)", (movie_id[0], company_id[0])
                        )
                    else:
                        print(f"Entry for movie_id={movie_id[0]} and pd_id={company_id[0]} already exists.")
                else:
                    print(f"Failed to insert into movie_produced_by: movie_id={movie_id}, company_id={company_id}")
        else: 
            print(f"No company with name {company} exists")

    conn.commit()
    cursor.close()
    conn.close()


def insert_production_company(production_companies):
    conn = connect_db()
    cursor = conn.cursor()

    for company in production_companies:
        cursor.execute(
            "SELECT * FROM production_company WHERE company_name = %s", (company)
        )
        if cursor.fetchone() is None:
            cursor.execute(
                "INSERT INTO production_company (company_name) VALUES (%s)", (company)
            )
        else:
            print(f"Company {company} already exists.")

    conn.commit()
    cursor.close()
    conn.close()


# function to get the ordinal of a number which will be used in the url
def ordinal(n):
    if 11 <= n <= 13:
        return f'{n}th'
    else: 
        return f'{n}{["th","st","nd","rd","th","th","th","th","th","th"][n % 10]}'

# function to convert date into the formatted input for the database
def format_date(date_str):
    # remove brackets and their content if present
    date_clean = re.sub(r'\[.*?\]', '', date_str)
    # remove parenthesized content
    date_full_clean = re.sub(r'\(.*?\)', '', " ".join(date_clean.split()).replace(',', '')).strip()
    try:
        dt = datetime.strptime(date_full_clean, '%d %B %Y')
    except ValueError:
        try:
            dt = datetime.strptime(date_full_clean, '%B %d %Y')  # handle 'March 2 2025' format
        except ValueError:
            raise ValueError(f"Date format not recognized: {date_full_clean}")
    return dt.strftime("%Y-%m-%d")

def format_movie_date(date_str):
    # remove any parenthesized content (like "(Cannes)" or "(United States)")
    cleaned = re.sub(r'\(.*?\)', '', date_str).strip()
    # remove any trailing commas
    cleaned = cleaned.strip(', ')
    
    # check if the cleaned date contains a 4-digit year
    if not re.search(r'\b\d{4}\b', cleaned):
        print(f"Error formatting date '{date_str}': Date format not recognized: {cleaned}")
        return None

    # try two common formats:
    possible_formats = ["%B %d, %Y", "%d %B %Y"]
    for fmt in possible_formats:
        try:
            dt = datetime.strptime(cleaned, fmt)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            continue

    print(f"Error formatting date '{date_str}': Date format not recognized: {cleaned}")
    return None


# function to format the site location
def format_site(site_str):
    #print("Site String:", site_str)
    # remove any bracketed content (including newlines)
    site_clean = re.sub(r'\[.*?\]', '', site_str, flags=re.DOTALL)
    # remove parentheses while keeping their content intact
    site_clean = re.sub(r'\((.*?)\)', r'\1', site_clean)
    # replace any occurrence of "in" surrounded by whitespace (including newlines) with a comma
    site_clean = re.sub(r'\s+in\s+', ', ', site_clean)
    # split on both commas and newlines
    parts = re.split(r'[,\n]+', site_clean)
    # strip extra whitespace and remove empty strings
    parts = [p.strip() for p in parts if p.strip()]
    return parts


# function to format the people who hosted the event
def format_person(host_str):
    if isinstance(host_str, list):
        formatted_hosts = []
        for host in host_str:
            host_clean = re.sub(r'\[.*?\]', '', host)
            words = host_clean.split()  # split into a list of words
            formatted_hosts.extend(words)  # extend the list instead of appending
        return formatted_hosts
    else:
        host_clean = re.sub(r'\[.*?\]', '', host_str)
        return host_clean.split()
    
# function to format raw text into multiple locations
def format_site_multi(raw_text):
    # remove bracketed text that appears on its own lines (including its surrounding newline characters)
    raw_text = re.sub(r'\n\[\s*.*?\s*\]\n', '\n', raw_text, flags=re.DOTALL)
    
    # step 1: split raw_text into blocks separated by blank lines
    blocks = re.split(r'\n\s*\n', raw_text.strip())

    locations = []
    for block in blocks:
        # split each block into lines and strip unnecessary whitespace
        lines = [line.strip() for line in block.splitlines() if line.strip()]

        # remove "and", "in" (case insensitive)
        lines = [line for line in lines if line.lower() not in {"and", "in"}]

        if not lines:
            continue

        # merge lines that are only punctuation, but if a line starts with a comma and has text,
        # then treat it as a new entry rather than merging.
        cleaned_lines = []
        for line in lines:
            if line.startswith(","):
                stripped = line.lstrip(",").strip()
                # Only merge if nothing remains after stripping
                if stripped == "":
                    if cleaned_lines:
                        cleaned_lines[-1] += " " + stripped
                    else:
                        cleaned_lines.append(line)
                else:
                    # Append the stripped content as a separate element
                    cleaned_lines.append(stripped)
            elif all(char in ",.;:" for char in line):
                if cleaned_lines:
                    cleaned_lines[-1] += " " + line.lstrip(",").strip()
                else:
                    cleaned_lines.append(line)
            else:
                cleaned_lines.append(line)

        # remove parentheses but keep the content inside them
        cleaned_lines = [re.sub(r'[()]', '', line).strip() for line in cleaned_lines]

        # assume the first line is the venue name, and subsequent lines are location details
        venue = cleaned_lines[0]
        details = []
        for line in cleaned_lines[1:]:
            # split correctly on commas and ensure proper separation of city and state
            parts = [part.strip() for part in re.split(r',\s*', line) if part.strip()]
            details.extend(parts)
        
        locations.append([venue] + details)

    return locations

def format_movie_name(movie_title):
    return movie_title.replace(" ", "_")

# function to convert the duration strictly into minutes
def convert_duration_to_minutes(duration_str):
    total_minutes = 0
    # look for a pattern like "Xh" (hours)
    m = re.search(r'(\d+)\s*h', duration_str)
    if m:
        hours = int(m.group(1))
        total_minutes += hours * 60
    # look for a pattern like "Ym" (minutes)
    m = re.search(r'(\d+)\s*m', duration_str)
    if m:
        minutes = int(m.group(1))
        total_minutes += minutes
    # fallback: if no hours/minutes pattern, try to extract a number followed by "minute"
    if total_minutes == 0:
        m = re.search(r'(\d+)\s*minute', duration_str, flags=re.IGNORECASE)
        if m:
            total_minutes = int(m.group(1))
    return total_minutes

# function to check if the link is valid
def can_follow_link(entity_type, article):
    """
    Given an entity type (e.g. "director") and an article name (e.g. "Hamish_Hamilton"),
    this function checks whether the Wikipedia page at:
         https://en.wikipedia.org/wiki/{article}
    actually corresponds to the desired person.
    
    If the page contains a disambiguation note (e.g., "This article is about ..."),
    it will try an alternate URL by appending _({entity_type}) to the article name.
    Returns the URL that appears valid.
    """
    base_url = "https://en.wikipedia.org/wiki/"
    url = f"{base_url}{article}"
    response = requests.get(url)
    
    if response.status_code != 200:
        print(f"Error: Could not fetch {url}")
        return None

    soup = BeautifulSoup(response.content, 'lxml')

    # find disambiguation or hatnote
    hatnotes = soup.find_all("div", {'class': 'hatnote navigation-not-searchable'})

    if hatnotes:
        print(f"Potential disambiguation found for {article}")
        for hatnote in hatnotes:
            if entity_type and entity_type.lower() in hatnote.text.lower():
                print(f"Disambiguation detected, switching to specific entity type: {entity_type}")
                alt_article = f"{article}_({entity_type})"
                alt_url = f"{base_url}{alt_article}"
                
                # Check if alternative URL is valid
                alt_response = requests.get(alt_url)
                if alt_response.status_code == 200 and "Wikipedia does not have an article" not in alt_response.text:
                    return alt_url
                else:
                    print(f"Alternative URL not found: {alt_url}, sticking with original.")

    return url

# function to scrape the person details (follows link into person's wikipedia page)
def scrape_person_list(person_list, entity_type=None):
    results = []
    for person in person_list:
        if isinstance(person, list):
            name = "_".join(part.strip() for part in person if part)
        else:
            name = person.strip()
        
        print("Person:", person)
        print("Full Name:", name)
        
        url = can_follow_link(entity_type, name)
        if not url:
            print(f"Skipping {name} as no valid URL could be determined.")
            results.append((None, None, None))
            continue
        
        print("URL:", url)
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'lxml')
        person_infobox = soup.find("table", class_=lambda c: c and "infobox" in c and "vcard" in c)
        
        person_birth_date = None
        person_birth_country = None
        person_death_date = None

        if person_infobox:
            person_details = person_infobox.find_all("tr")
            for row in person_details:
                header = row.find("th")
                if header:
                    header_text = header.text.strip()
                    if "Born" in header_text:
                        born_cell = row.find("td")
                        birth_date_span = row.find("span", {'class': 'bday'})
                        if birth_date_span:
                            person_birth_date = birth_date_span.text.strip()
                            print("Birth Date:", person_birth_date)
                        birthplace_div = row.find("div", {'class': 'birthplace'})
                        if birthplace_div:
                            person_birth_country = birthplace_div.text.strip()
                            person_birth_country = re.sub(r'[\)\]]', '', person_birth_country).strip()
                            # extract the last part after splitting by commas, ensuring it's not empty or invalid
                            parts = [part.strip() for part in person_birth_country.split(",") if part.strip()]
                            if parts:
                                person_birth_country = parts[-1]
                            else:
                                person_birth_country = None
                        else:
                            # fallback: use the full text of the cell
                            born_text = born_cell.get_text(" ", strip=True)
                            if person_birth_date:
                                born_text = born_text.replace(person_birth_date, "").strip()
                            born_text = re.sub(r'\(.*?\)', '', born_text).strip()
                            born_text = re.sub(r'\[.*?\]', '', born_text).strip()
                            # split the remaining text by commas
                            parts = [p.strip() for p in born_text.split(",") if p.strip()]
                            if parts:
                                # take the last element as the birthplace
                                person_birth_country = parts[-1]
                                # take the last element as the birthplace and remove anything after
                                print("Birth Country (fallback):", person_birth_country)
                    if "Died" in header_text:
                        death_date_span = row.find("span", class_="dday")
                        if death_date_span:
                            person_death_date = death_date_span.text.strip()
                            print("Death Date:", person_death_date)
        else:
            print("No infobox found for", name)

        results.append((
            person_birth_date if person_birth_date is not None else None,
            person_birth_country if person_birth_country is not None else None,
            person_death_date if person_death_date is not None else None
        ))
    return results

def scrape_movie_details(movie_title):
    if not movie_title:
        print("Empty list. No movies provided.")

    url = f"https://en.wikipedia.org/wiki/{format_movie_name(movie_title)}"
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'lxml')

    # get the movie name from the page's main heading
    movie_name = soup.find("h1", id="firstHeading").text.strip()
    print("Movie Name:", movie_name)

    movie_infobox = soup.find("table", {'class': 'infobox vevent'})
    if not movie_infobox:
        print(f"Could not find movie infobox for {movie_title} at {url}")
        return
    movie_details = movie_infobox.find_all("tr")

    in_language = None
    country = None

    movie_directors = []
    movie_producers = []
    movie_writers = []
    movie_stars = []
    movie_cinematography = []
    movie_editor = []
    movie_music = []
    production_companies = []
    release_dates = []
    running_time = []

    positions = []
    connections = []

    for row in movie_details:
        header = row.find("th")
        if header:
            header_text = header.text.strip()
            if "directed by" in header_text.lower():
                positions.append("Director")
                td = row.find("td")
                li_items = td.find_all("li")
                if li_items:
                    for li in li_items:
                        director_text = li.get_text(strip=True)
                        movie_directors.append(format_person(director_text))
                else:
                    links = td.find_all("a")
                    if links:
                        for a in links:
                            director_text = a.text.strip()
                            movie_directors.append(format_person(director_text))
                    else:
                        movie_directors = [format_person(td.text.strip())]
                if movie_directors:
                    print("Formatted Director:", movie_directors)
                    person_details = scrape_person_list(movie_directors)
                    for i, (birth_date, birth_country, death_date) in enumerate(person_details):
                        print("(Director) Birth Date:", birth_date)
                        print("(Director) Birth Country:", birth_country)
                        print("(Director) Death Date:", death_date)
                        if i < len(movie_directors):
                            insert_person([movie_directors[i]], [birth_date, birth_country, death_date])
                            connections.append((movie_name, movie_directors[i][0], movie_directors[i][-1], birth_date, "Director"))
            if "written by" in header_text.lower():
                positions.append("Writer")
                td = row.find("td")
                li_items = td.find_all("li")
                if li_items:
                    for li in li_items:
                        writer_text = li.get_text(strip=True)
                        movie_writers.append(format_person(writer_text))
                else:
                    links = td.find_all("a")
                    if links:
                        for a in links:
                            writer_text = a.text.strip()
                            movie_writers.append(format_person(writer_text))
                    else:
                        movie_writers = [format_person(td.text.strip())]
                if movie_writers:
                    print("Formatted Writer:", movie_writers)
                    person_details = scrape_person_list(movie_writers)
                    for i, (birth_date, birth_country, death_date) in enumerate(person_details):
                        print("(Writer) Birth Date:", birth_date)
                        print("(Writer) Birth Country:", birth_country)
                        print("(Writer) Death Date:", death_date)
                        if i < len(movie_writers):
                            insert_person([movie_writers[i]], [birth_date, birth_country, death_date])
                            connections.append((movie_name, movie_writers[i][0], movie_writers[i][-1], birth_date, "Writer"))

            if "produced by" in header_text.lower():
                positions.append("Producer")
                td = row.find("td")
                li_items = td.find_all("li")
                if li_items:
                    for li in li_items:
                        producer_text = li.get_text(strip=True)
                        movie_producers.append(format_person(producer_text))
                else:
                    links = td.find_all("a")
                    if links:
                        for a in links:
                            producer_text = a.text.strip()
                            movie_producers.append(format_person(producer_text))
                    else:
                        movie_producers = [format_person(td.text.strip())]
                if movie_producers:
                    print("Formatted Producer:", movie_producers)
                    person_details = scrape_person_list(movie_producers)
                    for i, (birth_date, birth_country, death_date) in enumerate(person_details):
                        print("(Producer) Birth Date:", birth_date)
                        print("(Producer) Birth Country:", birth_country)
                        print("(Producer) Death Date:", death_date)
                        if i < len(movie_producers):
                            insert_person([movie_producers[i]], [birth_date, birth_country, death_date])
                            connections.append((movie_name, movie_producers[i][0], movie_producers[i][-1], birth_date, "Producer"))

            if "starring" in header_text.lower():
                positions.append("Star")
                td = row.find("td")
                li_items = td.find_all("li")
                if li_items:
                    for li in li_items:
                        text = li.get_text(strip=True)
                        movie_stars.append(format_person(text))
                else:
                    links = td.find_all("a")
                    if links:
                        for a in links:
                            text = a.text.strip()
                            movie_stars.append(format_person(text))
                    else:
                        movie_stars = [format_person(td.text.strip())]
                if movie_stars:
                    print("Formatted Stars:", movie_stars)
                    person_details = scrape_person_list(movie_stars)
                    for i, (birth_date, birth_country, death_date) in enumerate(person_details):
                        print("(Stars) Birth Date:", birth_date)
                        print("(Stars) Birth Country:", birth_country)
                        print("(Stars) Death Date:", death_date)
                        if i < len(movie_stars):
                            insert_person([movie_stars[i]], [birth_date, birth_country, death_date])
                            connections.append((movie_name, movie_stars[i][0], movie_stars[i][-1], birth_date, "Star"))
                            
            if "cinematography" in header_text.lower():
                positions.append("Cinematographer")
                td = row.find("td")
                li_items = td.find_all("li")
                if li_items:
                    for li in li_items:
                        text = li.get_text(strip=True)
                        movie_cinematography.append(format_person(text))
                else:
                    links = td.find_all("a")
                    if links:
                        for a in links:
                            text = a.text.strip()
                            movie_cinematography.append(format_person(text))
                    else:
                        movie_cinematography = [format_person(td.text.strip())]
                if movie_cinematography:
                    print("Formatted Cinematographer:", movie_cinematography)
                    person_details = scrape_person_list(movie_cinematography)
                    for i, (birth_date, birth_country, death_date) in enumerate(person_details):
                        print("(Cinematographer) Birth Date:", birth_date)
                        print("(Cinematographer) Birth Country:", birth_country)
                        print("(Cinematographer) Death Date:", death_date)
                        if i < len(movie_cinematography):
                            insert_person([movie_cinematography[i]], [birth_date, birth_country, death_date])
                            connections.append((movie_name, movie_cinematography[i][0], movie_cinematography[i][-1], birth_date, "Cinematographer"))

            if "edited by" in header_text.lower():
                positions.append("Editor")
                td = row.find("td")
                li_items = td.find_all("li")
                if li_items:
                    for li in li_items:
                        text = li.get_text(strip=True)
                        movie_editor.append(format_person(text))
                else:
                    links = td.find_all("a")
                    if links:
                        for a in links:
                            text = a.text.strip()
                            movie_editor.append(format_person(text))
                    else:
                        movie_editor = [format_person(td.text.strip())]
                if movie_editor:
                    print("Formatted Editor:", movie_editor)
                    person_details = scrape_person_list(movie_editor)
                    for i, (birth_date, birth_country, death_date) in enumerate(person_details):
                        print("(Editor) Birth Date:", birth_date)
                        print("(Editor) Birth Country:", birth_country)
                        print("(Editor) Death Date:", death_date)
                        if i < len(movie_editor):
                            insert_person([movie_editor[i]], [birth_date, birth_country, death_date])
                            connections.append((movie_name, movie_editor[i][0], movie_editor[i][-1], birth_date, "Editor"))

            if "music by" in header_text.lower():
                positions.append("Composer")
                td = row.find("td")
                li_items = td.find_all("li")
                if li_items:
                    for li in li_items:
                        text = li.get_text(strip=True)
                        movie_music.append(format_person(text))
                else:
                    links = td.find_all("a")
                    if links:
                        for a in links:
                            text = a.text.strip()
                            movie_music.append(format_person(text))
                    else:
                        movie_music = [format_person(td.text.strip())]
                if movie_music:
                    print("Formatted Composer:", movie_music)
                    person_details = scrape_person_list(movie_music)
                    for i, (birth_date, birth_country, death_date) in enumerate(person_details):
                        print("(Composer) Birth Date:", birth_date)
                        print("(Composer) Birth Country:", birth_country)
                        print("(Composer) Death Date:", death_date)
                        if i < len(movie_music):
                            insert_person([movie_music[i]], [birth_date, birth_country, death_date])
                            connections.append((movie_name, movie_music[i][0], movie_music[i][-1], birth_date, "Composer"))

            if "production" in header_text.lower():
                td = row.find("td")
                li_items = td.find_all("li")
                if li_items:
                    for li in li_items:
                        text = li.get_text(strip=True)
                        production_companies.append(text)
                else:
                    links = td.find_all("a")
                    if links:
                        for a in links:
                            text = a.text.strip()
                            production_companies.append(text)
                    else:
                        production_companies = [td.text.strip()]
                if production_companies:
                    print("Formatted Production Companies:", production_companies)

            if "release dates" in header_text.lower():
                dates_text = row.find("td").text.strip()
                # split only on newlines to preserve the comma in dates like "May 21, 2024 (Cannes)"
                dates_list = re.split(r'\n+', dates_text)
                for date in dates_list:
                    date = date.strip()
                    if date:
                        try:
                            formatted_date = format_movie_date(date)
                            release_dates.append(formatted_date)
                        except Exception as e:
                            print(f"Error formatting date '{date}': {e}")
                for release in release_dates:
                    print("Release Date:", release)

            if "running time" in header_text.lower():
                running_time = re.search(r'(\d+)\s*minutes?', row.find("td").text.strip(), re.IGNORECASE)
                running_time = int(running_time.group(1)) if running_time else None
                print("Running Time:", running_time)
            
            if "language" in header_text.lower():
                in_language = re.sub(r'\[.*?\]', '', row.find("td").text.strip())
                print("Language:", in_language)
            
            if "country" in header_text.lower():
                country = row.find("td").text.strip()
                print("Country:", country) 
    print(movie_name)
    insert_position(positions)
    insert_production_company(production_companies)
    insert_movie(movie_name, release_dates, in_language, running_time, country, production_companies)
    insert_movie_person(connections)
            

# actual function to scrape award info data (mainly follows the infobox and gets more data whenever required)
def scrape_award_info_data(n):
    url = f"https://en.wikipedia.org/wiki/{ordinal(n)}_Academy_Awards"
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'lxml')
    award_infobox = soup.find("table", {'class': 'infobox vevent'})
    award_details = award_infobox.find_all("tr")

    event_date = None
    event_site = None
    event_host = None
    event_preshowhost = None
    event_producer = None
    event_director = None
    event_network = None
    event_duration = None

    venue_id = []
    positions = []
    connections = []

    # dynamically find the indices for date, site, and host
    for row in award_details:
        header = row.find("th")
        if header:
            header_text = header.text.strip()
            if "date" in header_text.lower():
                event_date = row.find("td").text.strip()
                print("Date:", format_date(event_date))

            if "site" in header_text.lower():
                td = row.find("td")
                # get the raw text while preserving newlines and remove bracketed content
                raw_text = re.sub(r'\[.*?\]', '', td.get_text(separator="\n").strip()).strip()
                #print("Raw Site (full text):", raw_text)
                links = td.find_all("a")
                # if there are exactly 2 links, assume one location
                if links and len(links) == 2 | 3:
                    #return a flat list
                    event_site = [format_site(raw_text)]
                #if there are more than 2 links, assume multiple locations
                elif links and len(links) > 3:
                    event_site = format_site_multi(raw_text)
                else:
                    event_site = [format_site(raw_text)]
                print("Formatted Site:", event_site)
                insert_venue(event_site)
                for site in event_site:
                    venue_id.append(get_venue_id(site[0]))
            
            if "hosted by" in header_text.lower():
                positions.append("Host")
                td = row.find("td")
                event_host = []
                # First check for <li> tags
                li_items = td.find_all("li")
                if li_items:
                    for li in li_items:
                        host_text = li.get_text(strip=True)
                        if 'emcee' in host_text.lower():
                            continue
                        event_host.append(format_person(host_text))
                else:
                    links = td.find_all("a")
                    if links:
                        for a in links:
                            host_text = a.text.strip()
                            if 'emcee' in host_text.lower():
                                continue
                            event_host.append(format_person(host_text))
                    else:
                        event_host = [format_person(td.text.strip())]
                if event_host:
                    print("Formatted Host:", event_host)
                    person_details = scrape_person_list(event_host)
                    for i, (birth_date, birth_country, death_date) in enumerate(person_details):
                        print("(Host) Birth Date:", birth_date)
                        print("(Host) Birth Country:", birth_country)
                        print("(Host) Death Date:", death_date)
                        if i < len(event_host):
                            insert_person([event_host[i]], [birth_date, birth_country, death_date])
                            connections.append((n, event_host[i][0], event_host[i][-1], birth_date, "Host"))

            if "preshow hosts" in header_text.lower():
                positions.append("Preshow Host")
                td = row.find("td")
                # get raw text (stop at the first bracket)
                raw_text = re.split(r'\[', td.get_text(separator="\n").strip(), 1)[0].strip()
                event_preshowhost = []
                li_items = td.find_all("li")
                if li_items:
                    for li in li_items:
                        preshowhost_text = li.get_text(strip=True)
                        if 'emcee' in preshowhost_text.lower():
                            continue
                        formatted_host = format_person(preshowhost_text)
                        if formatted_host:  # Ensure it's not empty
                            event_preshowhost.append(formatted_host)
                else:
                    links = td.find_all("a")
                    if links:
                        for a in links:
                            preshowhost_text = a.text.strip()
                            if 'emcee' in preshowhost_text.lower():
                                continue
                            formatted_host = format_person(preshowhost_text)
                            if formatted_host:
                                event_preshowhost.append(formatted_host)
                    else:
                        # Fallback: use the raw text.
                        formatted_host = format_person(raw_text)
                        if formatted_host:
                            event_preshowhost.append(formatted_host)
                if event_preshowhost:
                    print("Formatted Preshow Host:", event_preshowhost)
                    person_details = scrape_person_list(event_preshowhost)
                    for i, (birth_date, birth_country, death_date) in enumerate(person_details):
                        print("(Preshow Host) Birth Date:", birth_date)
                        print("(Preshow Host) Birth Country:", birth_country)
                        print("(Preshow Host) Death Date:", death_date)
                        if i < len(event_preshowhost):
                            insert_person([event_preshowhost[i]], [birth_date, birth_country, death_date])
                            connections.append((n, event_preshowhost[i][0], event_preshowhost[i][-1], birth_date, "Preshow Host"))

            if "produced by" in header_text.lower():
                positions.append("Producer")
                td = row.find("td")
                event_producer = []
                li_items = td.find_all("li")
                if li_items:
                    for li in li_items:
                        prod_text = li.get_text(strip=True)
                        if 'emcee' in prod_text.lower():
                            continue
                        event_producer.append(format_person(prod_text))
                else:
                    links = td.find_all("a")
                    if links:
                        for a in links:
                            prod_text = a.text.strip()
                            if 'emcee' in prod_text.lower():
                                continue
                            event_producer.append(format_person(prod_text))
                    else:
                        raw_text = td.text.strip()
                        # separate lowercase from uppercase (e.g., KapoorKaty -> Kapoor\nKaty)
                        separated_text = re.sub(r'(?<=[a-z])(?=[A-Z])', r'\n', raw_text)
                        # split names by commas or newlines and format each name individually
                        names = [name.strip() for name in re.split(r'[,\n]+', separated_text) if name.strip()]
                        # apply format_person to each name individually
                        event_producer = [format_person(name) for name in names]
                if event_producer:
                    print("Formatted Producer:", event_producer)
                    person_details = scrape_person_list(event_producer)
                    for i, (birth_date, birth_country, death_date) in enumerate(person_details):
                        print("(Producer) Birth Date:", birth_date)
                        print("(Producer) Birth Country:", birth_country)
                        print("(Producer) Death Date:", death_date)
                        if i < len(event_producer):
                            insert_person([event_producer[i]], [birth_date, birth_country, death_date])
                            connections.append((n, event_producer[i][0], event_producer[i][-1], birth_date, "Producer"))

            if "directed by" in header_text.lower():
                positions.append("Director")
                td = row.find("td")
                event_director = []
                li_items = td.find_all("li")
                if li_items:
                    for li in li_items:
                        prod_text = li.get_text(strip=True)
                        if 'emcee' in prod_text.lower():
                            continue
                        event_director.append(format_person(prod_text))
                else:
                    links = td.find_all("a")
                    if links:
                        for a in links:
                            prod_text = a.text.strip()
                            if 'emcee' in prod_text.lower():
                                continue
                            event_director.append(format_person(prod_text))
                    else:
                        event_director = [format_person(td.text.strip())]
                if event_director:
                    print("Formatted Director:", event_director)
                    person_details = scrape_person_list(event_director, "director")
                    for i, (birth_date, birth_country, death_date) in enumerate(person_details):
                        print("(Director) Birth Date:", birth_date)
                        print("(Director) Birth Country:", birth_country)
                        print("(Director) Death Date:", death_date)
                        if i < len(event_director):
                            insert_person([event_director[i]], [birth_date, birth_country, death_date])
                            connections.append((n, event_director[i][0], event_director[i][-1], birth_date, "Director"))

            if "network" in header_text.lower():
                td = row.find("td")
                # Extract all <a> tags for network names
                links = td.find_all("a")
                event_network = [link.text.strip() for link in links if link.text.strip()]
                print("Network Names:", event_network)

            if "duration" in header_text.lower():
                td = row.find("td")
                raw_duration = td.text.strip()
                event_duration = convert_duration_to_minutes(raw_duration)
                print("Duration:", event_duration, "minutes") 

            if "best picture" in header_text.lower():
                td = row.find("td")
                raw_best_picture = td.text.strip()
                scrape_movie_details(raw_best_picture)

    insert_position(positions)
    insert_award(n, event_date, venue_id, event_duration, event_network) 
    insert_person_connection(connections)   

# function to scrape more detailed data, such as movie infos and nominations
def scrape_detailed_data(n):
    url = f"https://en.wikipedia.org/wiki/{ordinal(n)}_Academy_Awards"
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'lxml')
    

def scrape_data(n):
    scrape_award_info_data(n)


def main():
    #movie_title = "The Artist (film)"
    #scrape_movie_details(movie_title)
    iterations = range(97, 96, -1)  # 97th to 1st
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        futures = [executor.submit(scrape_data, i) for i in iterations]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error in processing a page: {e}")

main()
