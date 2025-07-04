import bs4
import requests
import string
import logging
import os
from supabase import create_client, Client
MAX_ITERATIONS = 1000


# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
logger = logging.getLogger(__name__)

parent_url = ""
queue = [[
    "https://dev.to",
    "https://www.reddit.com/r/programming",
    "https://hackernoon.com",
    "https://myanimelist.net",
    "https://anilist.co",
    "https://www.reddit.com/r/anime/"
]]
visited = set()
filler_words = set([
    "a", "an", "the", "and", "or", "but",
    "is", "are", "was", "were", "be", "been", "being",
    "to", "of", "in", "for", "on", "with", "at",
    "by", "from", "that", "which", "who", "whom",
    "this", "these", "those", "it", "its", "they", "their",
    "he", "she", "his", "her", "we", "us", "our",
    "you", "your", "me", "my", "him", "them", "they",
    "there", "here", "where", "when", "why", "how",
    "what", "which", "who", "whom", "whose", "if", "than",
    "so", "such", "as", "like", "just", "only", "more",
    "some", "any", "all", "every", "no", "not", "never",
    "always", "often", "sometimes", "usually", "rarely",  
    "i"
])

def main():
    i = 0
    logger.info("Starting web crawler...")
    
    url: str = os.environ.get("SUPABASE_URL")
    key: str = os.environ.get("SUPABASE_KEY")
    supabase: Client = create_client(url, key)
    
    while True:
        logger.info(f"Iteration {i}: Processing URLs in the queue...")
        for url in queue[i]:
            if url not in visited:
                logger.info(f"Visiting: {url}")
                try:
                    response = requests.get(url)
                    soup = bs4.BeautifulSoup(response.text, 'lxml')
                    
                    content = soup.getText() 
                    
                    translator = str.maketrans('', '', string.punctuation + string.whitespace + '–—•·“”‘’…’‘’')
                    content_no_symbols = ''.join(char if char.isalnum() or char.isspace() else ' ' for char in content)
                    words = content_no_symbols.split()
                    
                    weight_map = {}
                    
                    for word in words:
                        if word.lower() not in filler_words:
                            if word.lower() in weight_map:
                                weight_map[word.lower()] += 1
                            else:
                                weight_map[word.lower()] = 1
                            
                    h1s = soup.find_all('h1') 
                    h2s = soup.find_all('h2') 
                    
                    for h in h1s + h2s:
                        text = h.get_text()
                        if text.strip().lower() not in filler_words:
                            if text.strip().lower() in weight_map:
                                weight_map[text.strip().lower()] += 1
                            else:
                                weight_map[text.strip().lower()] = 1
                    
                    title = soup.title.string if soup.title else ''
                    for word in title.split():
                        if word.lower() not in filler_words:
                            if word.lower() in weight_map:
                                weight_map[word.lower()] += 1
                            else:
                                weight_map[word.lower()] = 1
                    
                    sorted_weight_map = sorted(weight_map.items(), key=lambda x: x[1], reverse=True)
                    
                    link_tags = soup.find_all('a', href=True)
                    links = []
                    for link in link_tags:
                        href = link['href']
                        if href.startswith('http') and href not in visited:
                            links.append([href, url])  # Store as [link, parent_url]
                    
                    queue.append([link[0] for link in links])  # Add new links to the queue
                    
                    logger.info(f"Top words in {url}:")
                    for word, weight in sorted_weight_map[:10]:
                        logger.info(f"{word}: {weight}")
                    
                    logger.info(f"Found {len(links)} new links.")
                    
                    final_data = {
                        "title": title,
                        "url": url,
                        "keywords": weight_map[:20],
                        "links": links
                    } 
                    try:
                        # Insert data into Supabase
                        logger.info(f"Inserting data into Supabase for {url}...")   
                        response = (    
                            supabase.table("links")
                                .insert(final_data)    
                                .execute()
                        )  
                    except Exception as e:
                        logger.error(f"Error inserting data into Supabase: {response.error}")
                    finally:                           
                        logger.info(f"Data successfully inserted into Supabase for {url}")
                        
                except Exception as e:
                    logger.error(f"Error visiting {url}: {e}")
                finally:
                    logger.info(f"Finished visiting: {url}")
                    visited.add(url)

        i += 1
        if i >= MAX_ITERATIONS:
            logger.info("No more URLs to visit.")
            break
        
if __name__ == "__main__":
    main()