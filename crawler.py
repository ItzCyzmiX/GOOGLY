import os
import math
import string
import logging
import requests
import bs4
import spacy
from supabase import create_client, Client
import dotenv

MAX_ITERATIONS = 1000

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
logger = logging.getLogger(__name__)

START_URLS = [
    "https://dev.to",
    "https://www.reddit.com/r/programming",
    "https://hackernoon.com",
    "https://myanimelist.net",
    "https://anilist.co",
    "https://www.reddit.com/r/anime/"
]

FILLER_WORDS = {
    "a", "an", "the", "and", "or", "but", "is", "are", "was", "were", "be", "been", "being",
    "to", "of", "in", "for", "on", "with", "at", "by", "from", "that", "which", "who", "whom",
    "this", "these", "those", "it", "its", "they", "their", "he", "she", "his", "her", "we", "us",
    "our", "you", "your", "me", "my", "him", "them", "there", "here", "where", "when", "why", "how",
    "what", "whose", "if", "than", "so", "such", "as", "like", "just", "only", "more", "some", "any",
    "all", "every", "no", "not", "never", "always", "often", "sometimes", "usually", "rarely", "i"
}

def load_supabase_client():
    dotenv.load_dotenv()
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    return create_client(url, key)

def clean_content(text):
    # Remove punctuation and non-alphanumeric symbols
    allowed = string.ascii_letters + string.digits + ' '
    return ''.join(char if char in allowed else ' ' for char in text)

def extract_keywords(doc, filler_words, stopwords):
    words = [
        token.text.lower()
        for token in doc
        if token.pos_ in {'NOUN', 'PROPN', 'ADJ'} and not token.is_stop
    ]
    return [w for w in words if w not in filler_words and w not in stopwords]

def compute_tfidf(words, global_word_counts, doc_count):
    tf_map = {}
    for word in words:
        tf_map[word] = tf_map.get(word, 0) + 1
        global_word_counts[word] = global_word_counts.get(word, 0) + 1

    idf_map = {
        word: math.log(doc_count / global_word_counts[word]) if global_word_counts[word] > 0 else 0
        for word in tf_map
    }
    weight_map = {word: min(tf_map[word] * idf_map[word], 50) for word in tf_map}
    return weight_map

def crawl():
    supabase = load_supabase_client()
    nlp = spacy.load("en_core_web_sm")
    stopwords = set(nlp.Defaults.stop_words)
    visited = set()
    queue = [START_URLS]
    global_word_counts = {}
    doc_count = 0

    for i in range(MAX_ITERATIONS):
        if i >= len(queue):
            logger.info("No more URLs to visit.")
            break

        logger.info(f"Iteration {i}: Processing URLs in the queue...")
        for url in queue[i]:
            if url in visited:
                continue
            logger.info(f"Visiting: {url}")
            doc_count += 1
            try:
                response = requests.get(url)
                soup = bs4.BeautifulSoup(response.text, 'lxml')
                content = clean_content(soup.getText())
                doc = nlp(content)
                keywords = extract_keywords(doc, FILLER_WORDS, stopwords)
                weight_map = compute_tfidf(keywords, global_word_counts, doc_count)
                sorted_keywords = sorted(weight_map.items(), key=lambda x: x[1], reverse=True)[:10]
                keyword_dict = dict(sorted_keywords)

                title = soup.title.string if soup.title else ''
                links = [
                    [link['href'], url]
                    for link in soup.find_all('a', href=True)
                    if link['href'].startswith('http') and link['href'] not in visited
                ]
                queue.append([l[0] for l in links])

                logger.info(f"Top words in {url}: {keyword_dict}")
                logger.info(f"Found {len(links)} new links.")

                final_data = {
                    "title": title,
                    "url": url,
                    "keywords": keyword_dict,
                    "links": links
                }
                try:
                    logger.info(f"Inserting data into Supabase for {url}...")
                    supabase.table("links").insert(final_data).execute()
                except Exception as e:
                    logger.error(f"Error inserting data into Supabase: {e}")
                else:
                    logger.info(f"Data inserted successfully for {url}.")
            except Exception as e:
                logger.error(f"Error visiting {url}: {e}")
            finally:
                visited.add(url)
                logger.info(f"Finished visiting: {url}")

if __name__ == "__main__":
    crawl()