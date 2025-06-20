import os
import requests
from bs4 import BeautifulSoup
import time
import re
from urllib.parse import urljoin

# --- Configuration ---
# The starting URL for the scrape.
BASE_URL = "https://web.archive.org/web/20160501142300/http://gmc.yoyogames.com/"
# The root directory to save the scraped data.
OUTPUT_DIR = "forums"
# Delay between requests to be polite to the server.
REQUEST_DELAY_SECONDS = 2

# --- Helper Functions ---

# FIXED: Added a robust URL resolver function to build complete URLs.
def resolve_url(base, href):
    """Creates a full, requestable URL from a link found on an archive page."""
    # If href is already a full valid URL, return it.
    if href.startswith('http'):
        return href
    # If href is a path starting with /web/, it's an absolute path from the archive root.
    if href.startswith('/web/'):
        return f"https://web.archive.org{href}"
    # Otherwise, join it with the base URL of the current page.
    return urljoin(base, href)

def make_soup(url):
    """Fetches a URL and returns a BeautifulSoup object with a retry mechanism."""
    retries = 3
    delay = 5  # Initial delay in seconds
    for attempt in range(retries):
        print(f"Fetching: {url}")
        try:
            time.sleep(REQUEST_DELAY_SECONDS)
            response = requests.get(url, timeout=30)
            response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
            return BeautifulSoup(response.text, 'html.parser')
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            if attempt < retries - 1:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= 2  # Exponential backoff
            else:
                print(f"Max retries exceeded for {url}.")
                return None
    return None


def sanitize_filename(name):
    """Removes invalid characters from a string to make it a valid filename."""
    name = name.strip()
    name = re.sub(r'[\\/*?:"<>|]', "_", name)
    name = re.sub(r'[\s_]+', '_', name)
    # Truncate long filenames to avoid OS limits
    return name[:100]

def write_to_file(filepath, content):
    """Writes content to a file, creating directories if they don't exist."""
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
    except OSError as e:
        print(f"Error writing to file {filepath}: {e}")

# --- Main Scraping Logic ---

def scrape_post_content(topic_url):
    """Scrapes all posts from a single topic page, handling pagination."""
    all_posts_text = []
    current_page_url = topic_url

    while current_page_url:
        soup = make_soup(current_page_url)
        if not soup:
            break

        # The correct selector for the main content of a post is 'div.post.entry-content'.
        post_content_divs = soup.select('div.post.entry-content')

        for post_content_div in post_content_divs:
            # We find the main container for the whole post and then select specific elements within it.
            post_wrap = post_content_div.find_parent('div', class_='post_wrap')
            if not post_wrap:
                continue

            # --- Extract Username and Post Date ---
            author_h3 = post_wrap.select_one('h3.author')
            posted_info_p = post_wrap.select_one('p.posted_info')

            username = author_h3.get_text(strip=True) if author_h3 else "Unknown User"
            post_date = posted_info_p.get_text(strip=True) if posted_info_p else "Unknown Date"
            post_date = re.sub(r'Posted |#\d+', '', post_date).strip() # Clean up the date string

            # --- Extract Post Content ---
            # Remove quotes, code blocks, and images to get clean text
            for block in post_content_div.select('div.blockquote, pre.prettyprint, img'):
                block.decompose()
            
            post_content = post_content_div.get_text("\n", strip=True)

            # --- Extract Signature ---
            signature_content = ""
            # The signature is a sibling to the post_content div's parent container
            signature_div = post_wrap.find('div', class_='signature')
            if signature_div:
                for img in signature_div.find_all('img'):
                    img.decompose()
                signature_content = signature_div.get_text("\n", strip=True).strip()

            # --- Assemble Post Text ---
            all_posts_text.append(f"--- User: {username} | Date: {post_date} ---\n\n{post_content}")
            if signature_content:
                all_posts_text.append(f"\n--- Signature ---\n{signature_content}")
            all_posts_text.append("\n" + "="*80 + "\n")

        # Find the "Next" page link
        next_link = soup.select_one('a[rel="next"]')
        if next_link and next_link['href']:
            # FIXED: Use the new resolver function with the current page URL as the base.
            current_page_url = resolve_url(current_page_url, next_link['href'])
        else:
            current_page_url = None

    return "\n".join(all_posts_text)


def scrape_topic_listing(forum_url, current_path):
    """Scrapes all topics within a forum, handling pagination."""
    topic_listing = []
    current_page_url = forum_url

    while current_page_url:
        soup = make_soup(current_page_url)
        if not soup:
            break
        
        # More specific selector for topic rows to avoid header/footer rows
        topic_rows = soup.select('table.ipb_table tr[class^="row"]')

        for row in topic_rows:
            # A more robust selector for the topic link
            topic_link_tag = row.select_one('td.col_f_topic a[href*="showtopic="]')
            if not topic_link_tag:
                continue

            topic_title = topic_link_tag.get_text(strip=True)
            # FIXED: Use the new resolver function with the current page URL as the base.
            topic_url = resolve_url(current_page_url, topic_link_tag['href'])

            last_post_cell = row.select_one('td.col_f_lastact')
            last_post_date_str = last_post_cell.get_text(strip=True) if last_post_cell else "nodate"
            
            # Simple date extraction from text like "25 December 2019"
            match = re.search(r'(\d{1,2}\s\w+\s\d{4})', last_post_date_str)
            date_prefix = match.group(1).replace(" ","-") if match else "unknown_date"
            
            print(f"  Scraping Topic: {topic_title}")
            post_content = scrape_post_content(topic_url)
            
            if post_content:
                sanitized_title = sanitize_filename(topic_title)
                filename = f"{date_prefix}-{sanitized_title}.txt"
                filepath = os.path.join(current_path, filename)
                write_to_file(filepath, post_content)
                topic_listing.append(f"{topic_title} | {topic_url}")

        # The next page link in topic lists often has rel="next"
        next_link = soup.select_one('a[rel="next"]')
        if next_link and next_link['href']:
             # FIXED: Use the new resolver function with the current page URL as the base.
             current_page_url = resolve_url(current_page_url, next_link['href'])
        else:
            current_page_url = None

    listing_filepath = os.path.join(current_path, "listing.txt")
    write_to_file(listing_filepath, "\n".join(topic_listing))


def scrape_forum_index(url, current_path):
    """Scrapes the main index page for all forums and kicks off sub-scraping."""
    soup = make_soup(url)
    if not soup:
        return []

    all_forums_found = []
    
    # Categories are separated by tables with class 'ipb_table'
    category_tables = soup.select('table.ipb_table')

    for category_table in category_tables:
        # Selecting rows within each category table directly is more reliable
        rows = category_table.select('tr')
        for row in rows:
            main_forum_link_tag = row.select_one('td h4 a[href*="showforum="]')
            if not main_forum_link_tag:
                continue

            forum_name = main_forum_link_tag.get_text(strip=True)
            # FIXED: Use the new resolver function with the index page URL as the base.
            forum_url = resolve_url(url, main_forum_link_tag['href'])
            
            if any(forum_url in f for f in all_forums_found):
                continue
                
            print(f"\nProcessing Forum: {forum_name}")
            all_forums_found.append(f"{forum_name} | {forum_url}")

            sanitized_name = sanitize_filename(forum_name)
            forum_path = os.path.join(current_path, sanitized_name)
            scrape_topic_listing(forum_url, forum_path)
            
            # Sub-forums are in a <span class="desc">
            parent_td = main_forum_link_tag.find_parent('td')
            if parent_td:
                desc_span = parent_td.find('span', class_='desc')
                if desc_span:
                    subforum_links = desc_span.select('a[href*="showforum="]')
                    for subforum_link in subforum_links:
                         subforum_name = subforum_link.get_text(strip=True)
                         if not subforum_name: continue

                         # FIXED: Use the new resolver function with the index page URL as the base.
                         subforum_url = resolve_url(url, subforum_link['href'])
                         
                         print(f"  Processing Sub-Forum: {subforum_name}")
                         all_forums_found.append(f"  - {subforum_name} | {subforum_url}")

                         subforum_sanitized_name = sanitize_filename(subforum_name)
                         subforum_path = os.path.join(forum_path, subforum_sanitized_name)
                         scrape_topic_listing(subforum_url, subforum_path)
    
    return all_forums_found

# --- Main Execution ---
if __name__ == "__main__":
    print("Starting scrape of the main forum page...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    # Start scraping from the main index page
    all_forums = scrape_forum_index(BASE_URL + "index.php", OUTPUT_DIR)
    
    if all_forums:
        forums_filepath = os.path.join(OUTPUT_DIR, "listing.txt")
        write_to_file(forums_filepath, "\n".join(all_forums))
        print("\nScraping complete!")
        print(f"All data saved in the '{OUTPUT_DIR}' directory.")
    else:
        print("\nCould not find any forums to scrape. The script will now exit.")
