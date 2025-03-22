import os
from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup, Tag
import pymysql
from datetime import datetime
import re
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
            WHERE LOWER(venue_name) IN (%s, %s)
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
    
    for venue_id in venue_ids:
        cursor.execute(
            "SELECT award_edition_id FROM award_edition WHERE edition = %s AND venue_id = %s AND network = %s",
            (n, venue_id, network)
        )
        if cursor.fetchone() is not None:
            print(f"Award {n} at venue {venue_id} already exists.")
        else:
            cursor.execute(
                "INSERT INTO award_edition (edition, aYear, cDate, venue_id, duration, network) VALUES (%s, %s, %s, %s, %s, %s)",
                (n, datetime.strptime(format_date(event_date), "%Y-%m-%d").year, format_date(event_date), venue_ids[0], duration, ','.join(network) if isinstance(network, list) else network)
            )
    
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
    # remove parenthesized content (e.g., "(1929-05-16)")
    date_full_clean = re.sub(r'\(.*?\)', '', " ".join(date_clean.split()).replace(',', '')).strip()
    dt = datetime.strptime(date_full_clean, '%B %d %Y')
    return dt.strftime("%Y-%m-%d")

# function to format the site location
def format_site(site_str):
    print("Site String:", site_str)
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
def format_host(host_str):
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

# actual function to scrape the data
def scrape_data(n):
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
                print("Raw Site (full text):", raw_text)
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
                td = row.find("td")
                event_host = []
                # First check for <li> tags
                li_items = td.find_all("li")
                if li_items:
                    for li in li_items:
                        host_text = li.get_text(strip=True)
                        if 'emcee' in host_text.lower():
                            continue
                        event_host.append(format_host(host_text))
                else:
                    links = td.find_all("a")
                    if links:
                        for a in links:
                            host_text = a.text.strip()
                            if 'emcee' in host_text.lower():
                                continue
                            event_host.append(format_host(host_text))
                    else:
                        event_host = [format_host(td.text.strip())]
                if event_host:
                    print("Formatted Host:", event_host)
                    person_details = scrape_person_list(event_host)
                    for i, (birth_date, birth_country, death_date) in enumerate(person_details):
                        print("(Host) Birth Date:", birth_date)
                        print("(Host) Birth Country:", birth_country)
                        print("(Host) Death Date:", death_date)
                        if i < len(event_host):
                            insert_person([event_host[i]], [birth_date, birth_country, death_date])

            if "preshow hosts" in header_text.lower():
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
                        formatted_host = format_host(preshowhost_text)
                        if formatted_host:  # Ensure it's not empty
                            event_preshowhost.append(formatted_host)
                else:
                    links = td.find_all("a")
                    if links:
                        for a in links:
                            preshowhost_text = a.text.strip()
                            if 'emcee' in preshowhost_text.lower():
                                continue
                            formatted_host = format_host(preshowhost_text)
                            if formatted_host:
                                event_preshowhost.append(formatted_host)
                    else:
                        # Fallback: use the raw text.
                        formatted_host = format_host(raw_text)
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

            if "produced by" in header_text.lower():
                td = row.find("td")
                event_producer = []
                li_items = td.find_all("li")
                if li_items:
                    for li in li_items:
                        prod_text = li.get_text(strip=True)
                        if 'emcee' in prod_text.lower():
                            continue
                        event_producer.append(format_host(prod_text))
                else:
                    links = td.find_all("a")
                    if links:
                        for a in links:
                            prod_text = a.text.strip()
                            if 'emcee' in prod_text.lower():
                                continue
                            event_producer.append(format_host(prod_text))
                    else:
                        raw_text = td.text.strip()
                        # separate lowercase from uppercase (e.g., KapoorKaty -> Kapoor\nKaty)
                        separated_text = re.sub(r'(?<=[a-z])(?=[A-Z])', r'\n', raw_text)
                        # split names by commas or newlines and format each name individually
                        names = [name.strip() for name in re.split(r'[,\n]+', separated_text) if name.strip()]
                        # apply format_host to each name individually
                        event_producer = [format_host(name) for name in names]
                if event_producer:
                    print("Formatted Producer:", event_producer)
                    person_details = scrape_person_list(event_producer)
                    for i, (birth_date, birth_country, death_date) in enumerate(person_details):
                        print("(Producer) Birth Date:", birth_date)
                        print("(Producer) Birth Country:", birth_country)
                        print("(Producer) Death Date:", death_date)
                        if i < len(event_producer):
                            insert_person([event_producer[i]], [birth_date, birth_country, death_date])

            if "directed by" in header_text.lower():
                td = row.find("td")
                event_director = []
                li_items = td.find_all("li")
                if li_items:
                    for li in li_items:
                        prod_text = li.get_text(strip=True)
                        if 'emcee' in prod_text.lower():
                            continue
                        event_director.append(format_host(prod_text))
                else:
                    links = td.find_all("a")
                    if links:
                        for a in links:
                            prod_text = a.text.strip()
                            if 'emcee' in prod_text.lower():
                                continue
                            event_director.append(format_host(prod_text))
                    else:
                        event_director = [format_host(td.text.strip())]
                if event_director:
                    print("Formatted Director:", event_director)
                    person_details = scrape_person_list(event_director, "director")
                    for i, (birth_date, birth_country, death_date) in enumerate(person_details):
                        print("(Director) Birth Date:", birth_date)
                        print("(Director) Birth Country:", birth_country)
                        print("(Director) Death Date:", death_date)
                        if i < len(event_director):
                            insert_person([event_director[i]], [birth_date, birth_country, death_date])

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

    insert_award(n, event_date, venue_id, event_duration, event_network)    


def main():
    iterations = range(97, 96, -1)  # 97th to 1st
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        futures = [executor.submit(scrape_data, i) for i in iterations]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error in processing a page: {e}")

main()
