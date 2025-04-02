import os
from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup, Tag
import pymysql
from datetime import datetime
import re
import csv
import concurrent.futures
from urllib.parse import unquote

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
        
        if len(venue) == 1:
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
                # Format: [venue_name, neighborhood, state]
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
                # Format: [venue_name, neighborhood, state, country]
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

        # Only compare venue names for duplicates.
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
def insert_person(person_list, person_info=None):
    conn = connect_db()
    cursor = conn.cursor()
    
    flattened_person_list = []
    for person in person_list:
        # Extract only the name, ensuring links are ignored
        if isinstance(person, list):
            person = [p for p in person if not is_link(p)]  # Remove links
        flat_person = flatten(person)  # Convert to a single name string
        if flat_person:
            flattened_person_list.append(flat_person)
    
    for person in flattened_person_list:
        # Remove empty or whitespace-only items.
        parts = person.split()  # Splitting by whitespace
        
        if not parts:
            continue  # Skip empty entries
        
        first_name = parts[0]
        # If first name starts with "#cite", ignore this entry.
        if first_name.startswith("#cite"):
            continue

        middle_name = None
        last_name = ""

        if len(parts) == 3:
            middle_name = parts[1]
            last_name = parts[2]
        elif len(parts) >= 2:
            last_name = parts[1]
        
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

        # Ensure birth_country is not numeric.
        if isinstance(birth_country, (int, float)) or str(birth_country).isdigit():
            birth_country = None

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
    
def is_link(text):
    """Check if a string is a URL or a Wikipedia link (/wiki/ or /w/)."""
    if not isinstance(text, str):  # Ensure text is a string before matching
        return False
    return bool(re.match(r'https?://\S+|^/wiki/|^/w/', text))  # Detects full URLs, /wiki/, and /w/

def flatten(item):
    """Recursively flattens nested lists into a single string, skipping None values."""
    if isinstance(item, list):
        return " ".join(flatten(subitem) for subitem in item if subitem is not None)
    elif isinstance(item, str):
        return item.strip()
    else:
        return str(item)


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
    print("error here?")
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

def insert_movie(movie_name, release_dates, in_language, run_time, country, production_companies):
    conn = connect_db()
    cursor = conn.cursor()
    
    print("WE ARE HEREE")
    # Check if the movie already exists.
    cursor.execute("SELECT * FROM movie WHERE movie_name = %s", (movie_name,))
    if cursor.fetchone() is None:
        cursor.execute(
            "INSERT INTO movie (movie_name, run_time) VALUES (%s, %s)", (movie_name, run_time)
        )
    else: 
        print(f"Movie {movie_name} already exists.") 

    # Retrieve the movie_id (assumed primary key) for later use.
    cursor.execute("SELECT movie_id FROM movie WHERE movie_name = %s", (movie_name,))
    movie_row = cursor.fetchone()
    if movie_row:
        movie_id = movie_row[0]
    else:
        print(f"Failed to retrieve movie_id for {movie_name}.")
        conn.commit()
        cursor.close()
        conn.close()
        return

    # Insert release dates if they exist.
    for release_date in release_dates:
        if not release_date:
            continue
        cursor.execute(
            "SELECT * FROM movie_release_date WHERE movie_id = %s AND release_date = %s", 
            (movie_id, release_date)
        )
        if cursor.fetchone() is None:
            cursor.execute(
                "INSERT INTO movie_release_date (movie_id, release_date) VALUES (%s, %s)",
                (movie_id, release_date)
            )
        else: 
            print(f"Movie {movie_name} and date {release_date} already exists.")
    
    # Insert languages.
    for lang in in_language:
        if not lang:
            continue
        cursor.execute(
            "SELECT * FROM movie_language WHERE movie_id = %s AND in_language = %s", 
            (movie_id, lang)
        )
        if cursor.fetchone() is None:
            cursor.execute(
                "INSERT INTO movie_language (movie_id, in_language) VALUES (%s, %s)",
                (movie_id, lang)
            )
        else: 
            print(f"Movie {movie_name} and lang {lang} already exists.")
    
    # Insert countries.
    for con in country:
        if not con:
            continue
        cursor.execute(
            "SELECT * FROM movie_country WHERE movie_id = %s AND country = %s", 
            (movie_id, con)
        )
        if cursor.fetchone() is None:
            cursor.execute(
                "INSERT INTO movie_country (movie_id, country) VALUES (%s, %s)",
                (movie_id, con)
            )
        else: 
            print(f"Movie {movie_name} and country {con} already exists.")
    
    # Insert production companies.
    for company in production_companies:
        if not company:
            continue
        cursor.execute(
            "SELECT pd_id FROM production_company WHERE company_name = %s", (company,)
        )
        company_row = cursor.fetchone()
        if company_row:
            company_id = company_row[0]
            cursor.execute(
                "SELECT * FROM movie_produced_by WHERE movie_id = %s AND pd_id = %s", 
                (movie_id, company_id)
            )
            if cursor.fetchone() is None:
                cursor.execute(
                    "INSERT INTO movie_produced_by (movie_id, pd_id) VALUES (%s, %s)", 
                    (movie_id, company_id)
                )
            else:
                print(f"Entry for movie_id={movie_id} and pd_id={company_id} already exists.")
        else: 
            print(f"No company with name {company} exists")

    conn.commit()
    cursor.close()
    conn.close()


def insert_noinfobox_movie(movie_title):
    conn = connect_db()
    cursor = conn.cursor()

    cursor.execute(
            "SELECT movie_id FROM movie WHERE movie_name = %s", (movie_title,)
        )
    movie_id = cursor.fetchone()

    if not movie_id:
        cursor.execute(
            "INSERT INTO movie (movie_name) VALUES (%s)", (movie_title,)
        )
    else: 
        print(f"Movie {movie_title} already exists")
    conn.commit()
    cursor.close()
    conn.close()

def insert_category(cat):
    conn = connect_db()
    cursor = conn.cursor()

    # Query for the category.
    cursor.execute("SELECT category_id FROM category WHERE category_name = %s", (cat,))
    row = cursor.fetchone()
    if row is None:
        cursor.execute("INSERT INTO category (category_name) VALUES (%s)", (cat,))
        conn.commit()
        # Requery to get the new category id.
        cursor.execute("SELECT category_id FROM category WHERE category_name = %s", (cat,))
        row = cursor.fetchone()
    else:
        print(f"Category '{cat}' already exists.")
    
    cursor.close()
    conn.close()
    return row[0] if row else None



def insert_production_company(production_companies):
    conn = connect_db()
    cursor = conn.cursor()

    if production_companies:
        for company in production_companies:
            print("Executing query for company:", company)
            print("SELECT * FROM production_company WHERE company_name = %s", (company,))

            cursor.execute(
                "SELECT * FROM production_company WHERE company_name = %s", (company,)
            )
            if cursor.fetchone() is None:
                cursor.execute(
                    "INSERT INTO production_company (company_name) VALUES (%s)", (company,)
                )
            else:
                print(f"Company {company} already exists.")
    else: 
        print ("No Prod Company to add. Skipping.")

    conn.commit()
    cursor.close()
    conn.close()

def award_edition_exists(n):
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute("SELECT award_edition_id FROM award_edition WHERE edition = %s", (n,))
    result = cursor.fetchone()
    conn.commit()
    cursor.close()
    conn.close()

    if result: 
        # If result is already a tuple just return it.
        # But if it is an int, wrap it in a tuple.
        if isinstance(result, int):
            return (result,)
        return result
    return None

def normalize_movie_name(movie_name):
    """
    Normalize movie_name to always be a string.
    If movie_name is a list with one element, return that element.
    If movie_name is a list with multiple elements, you can decide:
      - to join them (e.g., ", ".join(movie_name))
      - or simply pick the first element.
    """
    if isinstance(movie_name, list):
        if len(movie_name) == 1:
            return movie_name[0]
        else:
            # Depending on your needs, you might join them or simply choose the first element.
            # Here we choose the first element.
            return movie_name[0]
    return movie_name


def movie_exists(movie_name):
    # Normalize the movie_name so it's always a string.
    movie_name = normalize_movie_name(movie_name)

    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute("SELECT movie_id FROM movie WHERE movie_name = %s", (movie_name,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    # Return a tuple (or the full row) rather than an int
    if result:
        if isinstance(result, int):
            return (result,)
        return result
    return None


def person_exists(fullname, birthdate, ignore=None):
    conn = connect_db()
    cursor = conn.cursor()
    
    print("Fullname and birthdate:", fullname, birthdate)
    
    # Normalize fullname:
    # If the first element is a list, use it; otherwise, assume fullname is already flat.
    if isinstance(fullname[0], list):
        name_parts = fullname[0]
    else:
        name_parts = fullname

    # Remove any parts that are empty or whitespace.
    name_parts = [part.strip() for part in name_parts if part.strip()]
    
    # Depending on the number of parts, assign first, (optional middle) and last.
    if len(name_parts) == 2:
        fname, lname = name_parts
        mname = None
    elif len(name_parts) >= 3:
        fname, mname, lname = name_parts[0], name_parts[1], name_parts[-1]
    else:
        # If there's only one part, assign it to fname and leave others empty.
        fname = name_parts[0]
        lname = ""
        mname = None

    print("Parsed name -> First:", fname, "Middle:", mname, "Last:", lname)
    
    # Ensure birthdate is a scalar (if it's a tuple/list, take the first element)
    if birthdate is not None and isinstance(birthdate, (tuple, list)):
        birthdate = birthdate[0]
    
    print("Using birthdate:", birthdate)
    
    # Use the birthdate in the query if provided and nonempty.
    if birthdate and birthdate.strip():
        cursor.execute(
            "SELECT person_id FROM person WHERE first_name = %s AND last_name = %s AND birthDate = %s",
            (fname, lname, birthdate)
        )
    else:
        # If no birthdate is provided, include middle_name if available.
        if mname:
            cursor.execute(
                "SELECT person_id FROM person WHERE first_name = %s AND middle_name = %s AND last_name = %s AND birthDate IS NULL",
                (fname, mname, lname)
            )
        else:
            cursor.execute(
                "SELECT person_id FROM person WHERE first_name = %s AND last_name = %s AND birthDate IS NULL",
                (fname, lname)
            )
    
    person_id = cursor.fetchone()  # Fetch result
    cursor.close()
    conn.close()
    
    return person_id[0] if person_id else None

#not used--- not needed
def get_position_id(cat):
    conn = connect_db()
    cursor = conn.cursor()
    cat_lower = cat.lower()

    position_id = None
    if "actor" in cat_lower or "actress" in cat_lower:
        cursor.execute("SELECT position_id FROM positions WHERE title = %s"), ("Star")
        position_id = cursor.fetchone()
    elif "directing" in cat_lower or "international film" in cat_lower:
        cursor.execute("SELECT position_id FROM positions WHERE title = %s"), ("Director")
        position_id = cursor.fetchone()
    elif "writing" in cat_lower:
        cursor.execute("SELECT position_id FROM positions WHERE title = %s"), ("Writer")
        position_id = cursor.fetchone()
    elif "picture" in cat_lower:
        cursor.execute("SELECT position_id FROM positions WHERE title = %s"), ("Producer")
        position_id = cursor.fetchone()

    cursor.close()
    conn.close()
    return position_id

def insert_nomination_one(award_edition_id, movie_id, category_id, won, submitted_by=None):
    """
    Insert a nomination record into the nomination table.
    """
    conn = connect_db()
    cursor = conn.cursor()
    query = """
        INSERT INTO nomination (award_edition_id, movie_id, category_id, won, submitted_by)
        VALUES (%s, %s, %s, %s, %s)
    """
    cursor.execute(query, (award_edition_id, movie_id, category_id, won, submitted_by))
    nomination_id = cursor.lastrowid  # Get the auto-generated nomination_id
    print("nomid:", nomination_id)
    conn.commit()
    cursor.close()
    conn.close()
    return nomination_id

def insert_nomination_person(nomination_id, person_id):
    conn = connect_db()
    cursor = conn.cursor()

    # Check if entry already exists
    cursor.execute(
        "SELECT 1 FROM nomination_person WHERE nomination_id = %s AND person_id = %s",
        (nomination_id, person_id),
    )
    
    if cursor.fetchone():
        print(f"Entry ({nomination_id}, {person_id}) already exists. Skipping insertion.")
    else:
        query = "INSERT INTO nomination_person (nomination_id, person_id) VALUES (%s, %s)"
        cursor.execute(query, (nomination_id, person_id))
        conn.commit()
        print(f"Inserted ({nomination_id}, {person_id}) successfully.")

    cursor.close()
    conn.close()


def insert_nominations(award_no, nominations_by_category, link_by):
    conn = connect_db()
    cursor = conn.cursor()

    # This list collects persons that need scraping.
    persons_to_scrape = []

    cursor.execute(
        "SELECT award_edition_id FROM award_edition WHERE edition = %s", (award_no,)
    )
    award_id_row = cursor.fetchone()
    if not award_id_row:
        print(f"No award edition found for award number {award_no}")
        return
    award_id = award_id_row[0]

    print("nominations_by_category:", nominations_by_category)
    
    for cat, nominations in nominations_by_category.items():
        print("here")
        print(f"Category: {cat}")
        # Insert category and retrieve category_id.
        category_id = insert_category(cat)
        
        # For categories with a different nomination structure.
        if "actor" in cat.lower() or "actress" in cat.lower() or "directing" in cat.lower():
            for i, nomination in enumerate(nominations):
                won_flag = 1 if i == 0 else 0  # First nominee wins, others don't
                if len(nomination) == 4:
                    person_name, movie_name, status, _ = nomination  # discard provided link
                elif len(nomination) == 3:
                    person_name, movie_name, _ = nomination
                    status = None  # default value
                else:
                    print(f"Unexpected format in nomination: {nomination}")
                    continue

                # Check if movie details already exist before scraping.
                movie_id_row = movie_exists(movie_name)
                if not movie_id_row:
                    movie_name = normalize_movie_name(movie_name)
                    editted_mn = re.sub(r'\s*\(.*?\)', '', movie_name)
                    movie_link = link_by.get(editted_mn)
                    scrape_movie_details(movie_title=movie_name, movie_link=movie_link)
                    movie_id_row = movie_exists(movie_name)
                    if not movie_id_row:
                        movie_name_redefined = unquote(movie_link.replace("/wiki/", "").replace("_", " "))
                        movie_id_row = movie_exists(movie_name_redefined)
                        if not movie_id_row:
                            print(f"Failed to get movie id for '{movie_name}'. Skipping nomination.")
                            continue
                else:
                    print(f"Movie '{movie_name}' already exists, skipping scrape.")

                # Extract movie_id from the row.
                movie_id = movie_id_row[0]

                print("Status,", status)

                # Insert the nomination record using the correct won_flag.
                nomination_id = insert_nomination_one(award_id, movie_id, category_id, won_flag, None)
                print(f"Inserted nomination record (ID: {nomination_id}) for movie '{movie_name}' in category '{cat}'.")

                # Always add the person for scraping.
                person_link = link_by.get(person_name)
                formatted_person = format_person(person_name)
                # Split name into parts.
                fname_part, lname_part = (
                    formatted_person.split(" ", 1)
                    if " " in formatted_person else (formatted_person, "")
                )
                persons_to_scrape.append([formatted_person, person_link])
                print(nomination)

                # If we have accumulated persons to scrape, process them.
                if persons_to_scrape:
                    # Scrape and obtain details (including birth_date).
                    person_details = scrape_person_list([p[0] for p in persons_to_scrape], "director")
                    for (formatted_person, p_link), (birth_date, birth_country, death_date) in zip(persons_to_scrape, person_details):
                        name_parts = (
                            formatted_person.split(" ", 1)
                            if " " in formatted_person else (formatted_person, "")
                        )
                        # Ensure that birth_date is a scalar value.
                        bd = birth_date[0] if isinstance(birth_date, (tuple, list)) else birth_date

                        # Now check if this person already exists using the scraped birth_date.
                        person_id = person_exists(name_parts, bd)
                        if person_id:
                            print(f"Person '{formatted_person}' (born {bd}) already exists, skipping insertion.")
                        else:
                            # Insert the new person record if desired.
                            # For example: insert_person(fname, lname, bd, birth_country, death_date)
                            print(f"Inserting person '{formatted_person}' with birth date {bd}")
                        # If person exists, link them with the nomination.
                        if person_id:
                            insert_nomination_person(nomination_id, person_id)
                            print(f"Linked person (ID: {person_id}) with nomination (ID: {nomination_id}).")
                    persons_to_scrape.clear()

        else:
            # For categories where nominations come with a list of persons.
            for i, nomination in enumerate(nominations):
                won_flag = 1 if i == 0 else 0  # First nominee wins, others don't
                if len(nomination) == 4:
                    movie_name, person_list, status, _ = nomination  # discard provided link
                elif len(nomination) == 3:
                    movie_name, person_list, _ = nomination
                    status = None  # default value
                else:
                    print(f"Unexpected format in nomination: {nomination}")
                    continue

                # Check if movie exists before scraping.
                print(movie_name)
                movie_link = link_by.get(movie_name)
                print(movie_link)
                movie_id_row = movie_exists(movie_name)
                if not movie_id_row:
                    print(movie_link)
                    link = movie_link  # default to movie_link if available
                    if movie_link is None:
                        movie_name = normalize_movie_name(movie_name)
                        editted_mn = re.sub(r'\s*\(.*?\)', '', movie_name)
                        movie_link = link_by.get(editted_mn)
                        link = movie_link
                        if movie_link is None:
                            # try finding a link from the person list if movie link is missing.
                            for person in person_list:
                                print("it is here!")
                                link = link_by.get(person)
                                if link:
                                    break
                    print("Link used:", link)
                    scrape_movie_details(movie_title=movie_name, movie_link=link)
                    movie_id_row = movie_exists(movie_name)
                    if not movie_id_row:
                        movie_name_redefined = unquote(movie_link.replace("/wiki/", "").replace("_", " "))
                        movie_id_row = movie_exists(movie_name_redefined)
                        if not movie_id_row:
                            print(f"Failed to get movie id for '{movie_name}'. Skipping nomination.")
                            continue
                else:
                    print(f"Movie '{movie_name}' already exists, skipping scrape.")

                movie_id = movie_id_row[0]
                nomination_id = insert_nomination_one(award_id, movie_id, category_id, won_flag, None)
                print(f"Inserted nomination record (ID: {nomination_id}) for movie '{movie_name}' in category '{cat}'.")

                # Process each person in the list.
                for person in person_list:
                    person_link = link_by.get(person)
                    formatted_person = format_person(person)
                    name_parts = (
                        formatted_person.split(" ", 1)
                        if " " in formatted_person else (formatted_person, "")
                    )
                    persons_to_scrape.append([formatted_person, person_link])

                if persons_to_scrape:
                    person_details = scrape_person_list([p[0] for p in persons_to_scrape], "director")
                    for (formatted_person, p_link), (birth_date, birth_country, death_date) in zip(persons_to_scrape, person_details):
                        name_parts = (
                            formatted_person.split(" ", 1)
                            if " " in formatted_person else (formatted_person, "")
                        )
                        bd = birth_date[0] if isinstance(birth_date, (tuple, list)) else birth_date
                        full_name, _ = name_parts
                        person_id = person_exists(full_name, bd)
                        if person_id:
                            print(f"Person '{formatted_person}' (born {bd}) already exists, skipping insertion.")
                        else:
                            print(f"Inserting person '{formatted_person}' with birth date {bd}")
                        if person_id:
                            insert_nomination_person(nomination_id, person_id)
                            print(f"Linked person (ID: {person_id}) with nomination (ID: {nomination_id}).")
                    persons_to_scrape.clear()
                print(nomination)

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
    # Remove citation references like [2], [3]
    cleaned = re.sub(r'\[\d+\]', '', date_str)

    # Remove ISO date in parentheses (e.g., (2023-5-21))
    cleaned = re.sub(r'\(\d{4}-\d{1,2}-\d{1,2}\)', '', cleaned).strip()

    # Remove any other parenthesized content (e.g., (Tribeca))
    cleaned = re.sub(r'\(.*?\)', '', cleaned).strip()

    # Remove any trailing commas or extra spaces
    cleaned = cleaned.strip(', ')

    # Ensure the cleaned date contains a valid year
    if not re.search(r'\b\d{4}\b', cleaned):
        print(f"Error formatting date '{date_str}': Date format not recognized: {cleaned}")
        return None

    # Try three common formats: complete and incomplete dates.
    possible_formats = ["%B %d, %Y", "%d %B %Y", "%B %Y"]
    for fmt in possible_formats:
        try:
            dt = datetime.strptime(cleaned, fmt)
            return dt.strftime("%Y-%m-%d")  # Always return in YYYY-MM-DD format.
        except ValueError:
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


def format_person(host_str):
    if isinstance(host_str, list):
        formatted_hosts = []
        for host in host_str:
            # remove citations and extra characters
            host_clean = re.sub(r'\[.*?\]', '', host).strip()
            # if the host string starts with '#', ignore it
            if host_clean.startswith("#"):
                continue
            words = host_clean.split()  # split into a list of words
            formatted_hosts.extend(words)  # extend the list instead of appending
        return formatted_hosts
    else:
        host_clean = re.sub(r'\[.*?\]', '', host_str).strip()
        # If the host string starts with '#', return an empty list.
        if host_clean.startswith("#"):
            return []
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
    #print("this right")
    # if movie_title is a list, take the first element.
    if isinstance(movie_title, list):
        movie_title = movie_title[0]
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

def clean_producers(producer_text, movie_title=""):
    """
    Cleans producer names by removing extra spaces, unwanted phrases, 
    and ensuring the movie title is not included.
    """
    # Remove trailing "producer(s)" if present
    producer_text = re.sub(r'\s*(producers?|directors?)$', '', producer_text, flags=re.IGNORECASE).strip()

    # Remove unwanted phrases
    remove_phrases = [
        "music and lyrics by", "production design:", "directed by", 
        "screenplay by", "story by", "set decoration:"
    ]
    for phrase in remove_phrases:
        producer_text = re.sub(rf'(?i){phrase}', '', producer_text)

    # Remove text in parentheses or square brackets (e.g., citations like [32])
    producer_text = re.sub(r'\[.*?\]|\(.*?\)', '', producer_text)

    # Standardize delimiters
    producer_text = producer_text.replace(" and ", ", ")
    
    # Remove movie title if present
    if movie_title:
        producer_text = producer_text.replace(movie_title, "").strip()

    # Remove unwanted symbols
    producer_text = producer_text.replace("–", "").replace("‡", "").replace("*", "").strip()

    # Extract names
    if "," in producer_text:
        producers = [p.strip() for p in producer_text.split(",") if p.strip()]
    else:
        # Extract names in "First Last" or similar format
        producers = re.findall(r'([A-Z][a-zA-Z]*(?:\s+[A-Z][a-zA-Z]+)+)', producer_text)

    # Remove unwanted strings (e.g., "edit" or citation markers)
    producers = [p for p in producers if p.lower() != "edit" and not re.match(r'^\d+$', p)]

    return producers


def clean_text(text):
    """Removes bracketed content like [1], [citation needed] from a string."""
    return re.sub(r'\[.*?\]', '', text).strip()

def clean_category(category_text):
    # Remove any content in square brackets, then strip and lowercase.
    return re.sub(r'\[.*?\]', '', category_text).strip().lower()


def scrape_person_list(person_list, entity_type=None):
    # Remove any empty list entries
    person_list = [p for p in person_list if not (isinstance(p, list) and not p)]
    
    results = []
    for person in person_list:
        provided_url = None
        if isinstance(person, list):
            # Check if the last element is a URL (either starting with "http" or "/")
            if isinstance(person[-1], str) and (person[-1].startswith("http") or person[-1].startswith("/")):
                provided_url = person[-1]
                # Join all preceding parts to form the full name, flattening each part.
                name = "_".join(flatten(part) for part in person[:-1] if part)
            else:
                # Fallback: flatten the list to get the name.
                name = flatten(person)
        else:
            name = person.strip()
        
        print("Person:", person)
        print("Full Name:", name)
        
        # If a provided URL exists and starts with "/", prepend the Wikipedia base URL.
        if provided_url:
            if provided_url.startswith("/"):
                url = "https://en.wikipedia.org" + provided_url
            else:
                url = provided_url
        else:
            url = can_follow_link(entity_type, name)
            # If can_follow_link fails to generate a URL, build one manually.
            if not url:
                # Replace spaces with underscores for the Wikipedia URL.
                url = "https://en.wikipedia.org/wiki/" + name.replace(" ", "_")
        
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
                            # if only a year is provided, append "-01-01" to form a complete date
                            if len(person_birth_date) == 4:
                                person_birth_date = person_birth_date + "-01-01"
                            # if year and month are provided (e.g., "1967-12"), append "-01" to form a complete date
                            elif len(person_birth_date) == 7:
                                person_birth_date = person_birth_date + "-01"
                            print("Birth Date:", person_birth_date)
                        birthplace_div = row.find("div", {'class': 'birthplace'})
                        if birthplace_div:
                            person_birth_country = birthplace_div.text.strip()
                            person_birth_country = re.sub(r'[\[\]\d]', '', person_birth_country).strip()
                            parts = [part.strip() for part in person_birth_country.split(",") if part.strip()]
                            if parts:
                                person_birth_country = parts[-1]
                            else:
                                person_birth_country = None
                            # Remove any trailing closing parenthesis
                            if person_birth_country:
                                person_birth_country = person_birth_country.rstrip(')')
                            # Remove "citation needed" (case-insensitive)
                            if person_birth_country:
                                person_birth_country = re.sub(r'\bcitation needed\b', '', person_birth_country, flags=re.I).strip()
                            # If person_birth_country contains digits or the person's name, set it to None
                            if person_birth_country and (re.search(r'\d', person_birth_country) or name.lower() in person_birth_country.lower()):
                                person_birth_country = None
                        else:
                            born_text = born_cell.get_text(" ", strip=True)
                            if person_birth_date:
                                born_text = born_text.replace(person_birth_date, "").strip()
                            born_text = re.sub(r'\(.*?\)', '', born_text).strip()
                            born_text = re.sub(r'\[.*?\]', '', born_text).strip()
                            parts = [p.strip() for p in born_text.split(",") if p.strip()]
                            if parts:
                                person_birth_country = parts[-1]
                                # Remove any trailing closing parenthesis
                                person_birth_country = person_birth_country.rstrip(')')
                                print("Birth Country (fallback):", person_birth_country)
                                # Remove "citation needed" (case-insensitive)
                                person_birth_country = re.sub(r'\bcitation needed\b', '', person_birth_country, flags=re.I).strip()
                                # If person_birth_country contains digits or the person's name, set it to None
                                if person_birth_country and (re.search(r'\d', person_birth_country) or name.lower() in person_birth_country.lower()):
                                    person_birth_country = None
                    if "Died" in header_text:
                        death_date_span = row.find("span", class_="dday")
                        if death_date_span:
                            person_death_date = death_date_span.text.strip()
                            print("Death Date:", person_death_date)
        else:
            print("No infobox found for", name)

        results.append((person_birth_date, person_birth_country, person_death_date))
    return results


def scrape_movie_details(movie_title=None, movie_link=None):
    if not movie_title and not movie_link:
        print("Empty list. No movies provided.")
        return

    if movie_link:
        url = f"https://en.wikipedia.org{movie_link}"
    else: 
        url = f"https://en.wikipedia.org/wiki/{format_movie_name(movie_title)}"

    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'lxml')

    # Get the movie name from the page's main heading
    movie_name = soup.find("h1", id="firstHeading").text.strip()
    print("Movie Name:", movie_name)

    movie_infobox = soup.find("table", {'class': 'infobox vevent'})
    '''
    if not movie_infobox:
        print(f"Could not find movie infobox for {movie_title} at {url}")
        url = f"https://en.wikipedia.org/wiki/{format_movie_name(movie_title)}_(film)"
        print(f"Using (film) keyword for {movie_title} at {url}")
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'lxml')

        # Get the movie name from the page's main heading
        movie_name = soup.find("h1", id="firstHeading").text.strip()
        print("Movie Name:", movie_name)
        movie_infobox = soup.find("table", {'class': 'infobox vevent'})
    '''

    if not movie_infobox:
        print(f"Movie {movie_title, movie_link} has no infobox. Skipping scrape.")
        insert_noinfobox_movie(movie_title)
        return

    movie_details = movie_infobox.find_all("tr")

    # Initialize lists for details
    in_language = []
    country = []

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
            header_text = header.text.strip().lower()
            td = row.find("td")
            if not td:
                continue

            # --- Directors ---
            if "directed by" in header_text:
                positions.append("Director")
                li_items = td.find_all("li")
                if li_items:
                    for li in li_items:
                        a_tags = li.find_all("a")
                        if a_tags:
                            for a_tag in a_tags:
                                if a_tag.find_parent("sup"):
                                    continue
                                director_text = a_tag.text.strip()
                                link = a_tag.get("href", None)
                                movie_directors.append([format_person(director_text), link])
                        else:
                            director_text = li.get_text(strip=True)
                            movie_directors.append([format_person(director_text), None])
                else:
                    links = td.find_all("a")
                    if links:
                        for a in links:
                            if a.find_parent("sup"):
                                continue
                            director_text = a.text.strip()
                            link = a.get("href", None)
                            movie_directors.append([format_person(director_text), link])
                    else:
                        movie_directors.append([format_person(td.text.strip()), None])
                if movie_directors:
                    print("Formatted Director:", movie_directors)
                    person_details = scrape_person_list(movie_directors, "director")
                    for i, (birth_date, birth_country, death_date) in enumerate(person_details):
                        '''print("(Director) Birth Date:", birth_date)
                        print("(Director) Birth Country:", birth_country)
                        print("(Director) Death Date:", death_date)'''
                        if i < len(movie_directors):
                            insert_person([movie_directors[i]], [birth_date, birth_country, death_date])
                            # Assuming first element is first name and last element is last name
                            connections.append((movie_name, movie_directors[i][0][0], movie_directors[i][0][-1], birth_date, "Director"))

            # --- Writers ---
            if "written by" in header_text:
                positions.append("Writer")
                li_items = td.find_all("li")
                if li_items:
                    for li in li_items:
                        a_tags = li.find_all("a")
                        if a_tags:
                            for a_tag in a_tags:
                                if a_tag.find_parent("sup"):
                                    continue
                                writer_text = a_tag.text.strip()
                                link = a_tag.get("href", None)
                                movie_writers.append([format_person(writer_text), link])
                        else:
                            writer_text = li.get_text(strip=True)
                            movie_writers.append([format_person(writer_text), None])
                else:
                    links = td.find_all("a")
                    if links:
                        for a in links:
                            if a.find_parent("sup"):
                                continue
                            writer_text = a.text.strip()
                            link = a.get("href", None)
                            movie_writers.append([format_person(writer_text), link])
                    else:
                        movie_writers.append([format_person(td.text.strip()), None])
                if movie_writers:
                    print("Formatted Writer:", movie_writers)
                    person_details = scrape_person_list(movie_writers, "writer")
                    for i, (birth_date, birth_country, death_date) in enumerate(person_details):
                        '''print("(Writer) Birth Date:", birth_date)
                        print("(Writer) Birth Country:", birth_country)
                        print("(Writer) Death Date:", death_date)'''
                        if i < len(movie_writers):
                            insert_person([movie_writers[i]], [birth_date, birth_country, death_date])
                            connections.append((movie_name, movie_writers[i][0][0], movie_writers[i][0][-1], birth_date, "Writer"))

            # --- Producers ---
            if "produced by" in header_text:
                positions.append("Producer")
                li_items = td.find_all("li")
                if li_items:
                    for li in li_items:
                        a_tags = li.find_all("a")
                        if a_tags:
                            for a_tag in a_tags:
                                if a_tag.find_parent("sup"):
                                    continue
                                prod_text = a_tag.text.strip()
                                link = a_tag.get("href", None)
                                movie_producers.append([format_person(prod_text), link])
                        else:
                            prod_text = li.get_text(strip=True)
                            movie_producers.append([format_person(prod_text), None])
                else:
                    links = td.find_all("a")
                    if links:
                        for a in links:
                            if a.find_parent("sup"):
                                continue
                            prod_text = a.text.strip()
                            link = a.get("href", None)
                            movie_producers.append([format_person(prod_text), link])
                    else:
                        movie_producers.append([format_person(td.text.strip()), None])
                if movie_producers:
                    print("Am i here")
                    # filter out any producer entries where the formatted name is an empty list.
                    movie_producers = [producer for producer in movie_producers if producer[0]]
                    print("Formatted Producer:", movie_producers)
                    person_details = scrape_person_list(movie_producers, "producer")
                    for i, (birth_date, birth_country, death_date) in enumerate(person_details):
                        '''print("(Producer) Birth Date:", birth_date)
                        print("(Producer) Birth Country:", birth_country)
                        print("(Producer) Death Date:", death_date)'''
                        if i < len(movie_producers):
                            insert_person([movie_producers[i]], [birth_date, birth_country, death_date])
                            connections.append((movie_name, movie_producers[i][0][0], movie_producers[i][0][-1], birth_date, "Producer"))

            # --- Stars ---
            if "starring" in header_text:
                positions.append("Star")
                li_items = td.find_all("li")
                if li_items:
                    for li in li_items:
                        a_tags = li.find_all("a")
                        if a_tags:
                            for a_tag in a_tags:
                                if a_tag.find_parent("sup"):
                                    continue
                                star_text = a_tag.text.strip()
                                link = a_tag.get("href", None)
                                movie_stars.append([format_person(star_text), link])
                        else:
                            star_text = li.get_text(strip=True)
                            movie_stars.append([format_person(star_text), None])
                else:
                    links = td.find_all("a")
                    if links:
                        for a in links:
                            if a.find_parent("sup"):
                                continue
                            star_text = a.text.strip()
                            link = a.get("href", None)
                            movie_stars.append([format_person(star_text), link])
                    else:
                        movie_stars.append([format_person(td.text.strip()), None])
                if movie_stars:
                    print("Formatted Stars:", movie_stars)
                    person_details = scrape_person_list(movie_stars)
                    for i, (birth_date, birth_country, death_date) in enumerate(person_details):
                        '''print("(Star) Birth Date:", birth_date)
                        print("(Star) Birth Country:", birth_country)
                        print("(Star) Death Date:", death_date)'''
                        if i < len(movie_stars):
                            insert_person([movie_stars[i]], [birth_date, birth_country, death_date])
                            connections.append((movie_name, movie_stars[i][0][0], movie_stars[i][0][-1], birth_date, "Star"))
                            
            # --- Cinematography ---
            if "cinematography" in header_text:
                positions.append("Cinematographer")
                li_items = td.find_all("li")
                if li_items:
                    for li in li_items:
                        a_tags = li.find_all("a")
                        if a_tags:
                            for a_tag in a_tags:
                                # Skip if the <a> is within a <sup> element.
                                if a_tag.find_parent("sup"):
                                    continue
                                cine_text = a_tag.text.strip()
                                link = a_tag.get("href", None)
                                movie_cinematography.append([format_person(cine_text), link])
                        else:
                            cine_text = li.get_text(strip=True)
                            movie_cinematography.append([format_person(cine_text), None])
                else:
                    links = td.find_all("a")
                    if links:
                        for a in links:
                            # Skip if the <a> is within a <sup> element.
                            if a.find_parent("sup"):
                                continue
                            cine_text = a.text.strip()
                            link = a.get("href", None)
                            movie_cinematography.append([format_person(cine_text), link])
                    else:
                        movie_cinematography.append([format_person(td.text.strip()), None])
                if movie_cinematography:
                    print("Formatted Cinematographer:", movie_cinematography)
                    person_details = scrape_person_list(movie_cinematography)
                    for i, (birth_date, birth_country, death_date) in enumerate(person_details):
                        '''print("(Cinematographer) Birth Date:", birth_date)
                        print("(Cinematographer) Birth Country:", birth_country)
                        print("(Cinematographer) Death Date:", death_date)'''
                        if i < len(movie_cinematography):
                            insert_person([movie_cinematography[i]], [birth_date, birth_country, death_date])
                            connections.append((movie_name, movie_cinematography[i][0][0], movie_cinematography[i][0][-1], birth_date, "Cinematographer"))

            # --- Editors ---
            if "edited by" in header_text:
                positions.append("Editor")
                li_items = td.find_all("li")
                if li_items:
                    for li in li_items:
                        a_tags = li.find_all("a")
                        if a_tags:
                            for a_tag in a_tags:
                                editor_text = a_tag.text.strip()
                                link = a_tag.get("href", None)
                                movie_editor.append([format_person(editor_text), link])
                        else:
                            editor_text = li.get_text(strip=True)
                            movie_editor.append([format_person(editor_text), None])
                else:
                    links = td.find_all("a")
                    if links:
                        for a in links:
                            editor_text = a.text.strip()
                            link = a.get("href", None)
                            movie_editor.append([format_person(editor_text), link])
                    else:
                        movie_editor.append([format_person(td.text.strip()), None])
                if movie_editor:
                    print("Formatted Editor:", movie_editor)
                    person_details = scrape_person_list(movie_editor, "editor")
                    for i, (birth_date, birth_country, death_date) in enumerate(person_details):
                        '''print("(Editor) Birth Date:", birth_date)
                        print("(Editor) Birth Country:", birth_country)
                        print("(Editor) Death Date:", death_date)'''
                        if i < len(movie_editor):
                            insert_person([movie_editor[i]], [birth_date, birth_country, death_date])
                            # handle the case where format_person returns a list.
                            name_value = movie_editor[i][0]
                            if isinstance(name_value, list):
                                if name_value:  # list is non-empty
                                    fname = name_value[0]
                                    lname = name_value[-1] if len(name_value) > 1 else ""
                                else:
                                    fname, lname = "", ""
                            else:
                                name_parts = name_value.split(" ", 1)
                                fname, lname = name_parts if len(name_parts) == 2 else (name_parts[0], "")
                            connections.append((movie_name, fname, lname, birth_date, "Editor"))
            # --- Composers (Music By) ---
            if "music by" in header_text:
                positions.append("Composer")
                li_items = td.find_all("li")
                if li_items:
                    for li in li_items:
                        a_tags = li.find_all("a")
                        if a_tags:
                            for a_tag in a_tags:
                                # Skip if the <a> is inside a <sup> tag.
                                if a_tag.find_parent("sup"):
                                    continue
                                composer_text = a_tag.text.strip()
                                link = a_tag.get("href", None)
                                movie_music.append([format_person(composer_text), link])
                        else:
                            composer_text = li.get_text(strip=True)
                            movie_music.append([format_person(composer_text), None])
                else:
                    links = td.find_all("a")
                    if links:
                        for a in links:
                            # Skip any <a> that is within a <sup> tag.
                            if a.find_parent("sup"):
                                continue
                            composer_text = a.text.strip()
                            link = a.get("href", None)
                            movie_music.append([format_person(composer_text), link])
                    else:
                        movie_music.append([format_person(td.text.strip()), None])
                if movie_music:
                    print("Formatted Composer:", movie_music)
                    person_details = scrape_person_list(movie_music, "composer")
                    for i, (birth_date, birth_country, death_date) in enumerate(person_details):
                        '''print("(Composer) Birth Date:", birth_date)
                        print("(Composer) Birth Country:", birth_country)
                        print("(Composer) Death Date:", death_date)'''
                        if i < len(movie_music):
                            insert_person([movie_music[i]], [birth_date, birth_country, death_date])
                            connections.append((movie_name, movie_music[i][0][0], movie_music[i][0][-1], birth_date, "Composer"))

            # --- Production Companies ---
            if "production" in header_text:
                production_companies = []  # Ensure it's an empty list before appending
                li_items = td.find_all("li")
                if li_items:
                    for li in li_items:
                        text = li.get_text(strip=True)
                        text = re.sub(r'\[.*?\]', '', text)  # Remove text inside []
                        production_companies.append(text)
                else:
                    links = td.find_all("a")
                    if links:
                        for a in links:
                            text = a.text.strip()
                            text = re.sub(r'\[.*?\]', '', text)  # Remove text inside []
                            production_companies.append(text)
                    else:
                        text = td.text.strip()
                        text = re.sub(r'\[.*?\]', '', text)  # Remove text inside []
                        production_companies = [text]
                if production_companies:
                    print("Formatted Production Companies:", production_companies)

            # --- Release Dates ---
            if "release dates" in header_text:
                td_element = td  # The <td> that contains the release dates
                release_dates = []  # Reset for each movie
                if td_element:
                    ul_element = td_element.find("ul")
                    if ul_element:
                        dates_list = [li.text.strip() for li in ul_element.find_all("li")]
                    else:
                        dates_text = td_element.text.strip()
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

            # --- Running Time ---
            if "running time" in header_text:
                running_time_match = re.search(r'(\d+)\s*minutes?', td.text.strip(), re.IGNORECASE)
                running_time = int(running_time_match.group(1)) if running_time_match else None
                print("Running Time:", running_time)
            
            # --- Languages ---
            if "language" in header_text or "languages" in header_text:
                language_text = td.text.strip()
                language_text = re.sub(r'\[.*?\]', '', language_text)
                # If there is no whitespace, split by capitals; otherwise, use spaces.
                if " " not in language_text:
                    in_language = split_by_capitals(language_text)
                else:
                    in_language = language_text.split()
                print("Language:", in_language)
            
            # --- Countries ---
            if "country" in header_text or "countries" in header_text:
                if td.find("ul"):
                    country = [clean_text(li.get_text(strip=True)) for li in td.find_all("li") if li.get_text(strip=True)]
                else:
                    country_text = td.text.strip()
                    country_text = clean_text(country_text)
                    # If there is no whitespace and splitting by capitals yields multiple parts, use that:
                    if " " not in country_text and len(split_by_capitals(country_text)) > 1:
                        country = split_by_capitals(country_text)
                    else:
                        country = [c.strip() for c in country_text.splitlines() if c.strip()]
                print("Country:", country)

    print(release_dates)
    print(country)
    print(movie_name)
    insert_position(positions)
    insert_production_company(production_companies)
    insert_movie(movie_name, release_dates, in_language, running_time, country, production_companies)
    insert_movie_person(connections)

def split_by_capitals(text):
    # Splits the text whenever a capital letter starts a new word
    return re.findall(r'[A-Z][a-z]*(?=[A-Z]|$)', text)

def scrape_awards(n):
    url = f"https://en.wikipedia.org/wiki/{ordinal(n)}_Academy_Awards"
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'lxml')

    all_tables = soup.find_all("table")
    awards_tables = [
        table for table in all_tables 
        if table.get("class") is not None and set(table.get("class")) == {"wikitable"}
    ]
    
    if len(awards_tables) >= 2:
        awards_table = awards_tables[1]
    elif awards_tables:
        awards_table = awards_tables[0]
    else:
        print("No strictly 'wikitable' found on the page.")
        return {}

    awards_details = awards_table.find_all("tr")
    nominations_by_category = {}
    link_by_person = {}

    # For later Academy Awards, assume the category and nominee details are within <td> elements.
    awards_details = awards_table.find_all("tr")
    for row in awards_details:
        tds = row.find_all("td")
        for td in tds:
            div = td.find("div")
            if div and div.find("b"):
                category = clean_category(div.text.strip())
                if category not in nominations_by_category:
                    nominations_by_category[category] = []
                ul = td.find("ul")
                if ul:
                    nominees = ul.find_all("li")
                    for nominee in nominees:
                        a_tags = nominee.find_all("a")
                        if a_tags:
                            for a_tag in a_tags:
                                link = a_tag.get("href")
                                person_name = a_tag.text.strip()
                                link_by_person[person_name] = link
                        else:
                            person_name = nominee.get_text(strip=True)
                            link_by_person[person_name] = None
                        won_tag = nominee.find("b") or nominee.find("i")
                        if won_tag:
                            movie_title = won_tag.text.strip()
                            if "–" in movie_title and ("‡" in movie_title or "*" in movie_title):
                                parts = movie_title.split("–")
                                if len(parts) > 1:
                                    movie_title = parts[0].strip()
                                    producer_text = parts[1].strip()
                                    producer_list = clean_producers(producer_text)
                                else:
                                    print(f"Unexpected format for movie title: {movie_title}")
                                    producer_list = []
                                nominations_by_category[category].append([movie_title, producer_list, "won", link])
                        normal_tag = nominee.find("ul")
                        if normal_tag:
                            details = normal_tag.text.strip()
                            lines = details.splitlines()
                            for line in lines:
                                parts = re.split(r'\s*–\s*', line, maxsplit=1)
                                if len(parts) == 2:
                                    title = parts[0].strip()
                                    producer_text = parts[1].strip()
                                    producer_list = clean_producers(producer_text)
                                    nominations_by_category[category].append([title, producer_list, link])
                                else:
                                    print(f"Unexpected format for line: {line}")
    if not nominations_by_category:
        print("Switching Method.")
        # Try to find <div> elements within the awards table.
        divs = awards_table.find_all("div")
        if not divs:
            print("No <div> elements found in the awards table; searching entire page.")
            divs = soup.find_all("div")
        # Iterate over found <div> elements.
        for div in divs:
            b_tag = div.find("b")
            # Only consider divs that have a nonempty <b> and a following <ul>
            ul = div.find_next_sibling("ul")
            if b_tag and b_tag.text.strip() and ul:
                header_text = b_tag.text.strip()
                print("Found header div with text:", header_text)
                category = clean_category(header_text)
                if category not in nominations_by_category:
                    nominations_by_category[category] = []
                nominees = ul.find_all("li")
                for nominee in nominees:
                    # Extract nominee name and link.
                    a_tags = nominee.find_all("a")
                    if a_tags:
                        for a_tag in a_tags:
                            link = a_tag.get("href")
                            person_name = a_tag.text.strip()
                            link_by_person[person_name] = link
                    else:
                        person_name = nominee.get_text(strip=True)
                        link_by_person[person_name] = None
                    # Process winning entries.
                    won_tag = nominee.find("b") or nominee.find("i")
                    if won_tag:
                        movie_title = won_tag.text.strip()
                        if "–" in movie_title and ("‡" in movie_title or "*" in movie_title):
                            parts = movie_title.split("–")
                            if len(parts) > 1:
                                movie_title = parts[0].strip()
                                producer_text = parts[1].strip()
                                producer_list = clean_producers(producer_text)
                            else:
                                print(f"Unexpected format for movie title: {movie_title}")
                                producer_list = []
                            nominations_by_category[category].append([movie_title, producer_list, "won", link])
                    # Process additional nomination details if available.
                    normal_tag = nominee.find("ul")
                    if normal_tag:
                        details = normal_tag.text.strip()
                        lines = details.splitlines()
                        for line in lines:
                            parts = re.split(r'\s*–\s*', line, maxsplit=1)
                            if len(parts) == 2:
                                title = parts[0].strip()
                                producer_text = parts[1].strip()
                                producer_list = clean_producers(producer_text)
                                nominations_by_category[category].append([title, producer_list, link])
                            else:
                                print(f"Unexpected format for line: {line}")

    for cat, nominations in nominations_by_category.items():
        print(f"Category: {cat}")
        for nomination in nominations:
            print(nomination)
    
    for person, link in link_by_person.items():
        print(f"Person: {person}, Link: {link}")
    insert_nominations(n, nominations_by_category, link_by_person)
    return nominations_by_category


# actual function to scrape award info data (mainly follows the infobox and gets more data whenever required)
def scrape_award_info_data(n):
    if award_edition_exists(n) is None:
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

                #best picture to be dealt with in scrape_award(n)
                '''if "best picture" in header_text.lower():
                    td = row.find("td")
                    raw_best_picture = td.text.strip()
                    scrape_movie_details(raw_best_picture)'''

        insert_position(positions)
        insert_award(n, event_date, venue_id, event_duration, event_network) 
        insert_person_connection(connections) 
    else:
        print(f"Award edition iteration already completed (award infobox), ",n)  

# function to scrape more detailed data, such as movie infos and nominations
def scrape_detailed_data(n):
    url = f"https://en.wikipedia.org/wiki/{ordinal(n)}_Academy_Awards"
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'lxml')
    

def scrape_data(n):
    scrape_award_info_data(n)
    scrape_awards(n)


def main():
    #movie_title = "Maestro"
    #movie_link = "/wiki/Maestro_(2023_film)"
    #scrape_movie_details(movie_link=movie_link)
    #scrape_awards(92)
    
    iterations = range(97, 96, -1)  # 97th to 1st
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        futures = [executor.submit(scrape_data, i) for i in iterations]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error in processing a page: {e}")

main()
