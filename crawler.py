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
    word_counts = {}
    for token in doc:
        if token.pos_ in {'NOUN', 'PROPN', 'ADJ'} and not token.is_stop:
            w = token.lemma_.lower()
            if w not in filler_words and w not in stopwords:
                word_counts[w] = word_counts.get(w, 0) + 1
    return [[word, count] for word, count in word_counts.items() if count > 5]


def crawl():
    supabase = load_supabase_client()
    nlp = spacy.load("en_core_web_sm")
    stopwords = set(nlp.Defaults.stop_words)
    visited = set()
    queue = [START_URLS]

    words={}

    for i in range(MAX_ITERATIONS):
        if visited.__len__() >= 500:
            logger.info("Visited 500 URLs, stopping crawl.")
            break
        for url in queue[i]:
            
            print(url)
            if url in visited:
                continue
            visited.add(url)
            logger.info(f"Crawling: {url}")
            
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
            except requests.RequestException as e:
                logger.error(f"Failed to fetch {url}: {e}")
                continue
            
            soup = bs4.BeautifulSoup(response.text, 'html.parser')
            content = soup.get_text()
            cleaned_content = clean_content(content)
            
            if not cleaned_content.strip():
                continue
            
            doc = nlp(cleaned_content)
            keywords = extract_keywords(doc, FILLER_WORDS, stopwords)
            
            if not keywords:
                continue
            
            for word in keywords:
                if words.get(word[0]):
                    words[word[0]].append([url, word[1]])
                else:
                    words[word[0]] = [[url, word[1]]]
            
            links = soup.find_all('a', href=True)    
            new_urls = set()
            for link in links:
                href = link['href']
                if href.startswith('http') and href not in visited:
                    new_urls.add(href)  
            
            if new_urls:
                queue.append(list(new_urls))
            
            
    for word in words:
        try:
            for a in words.get(word):
                res = supabase.table("words").insert({
                    "word": word,
                    "url": a[0],
                    "score": a[1]
                }).execute()

        except Exception as e:
            logger.error(f"Exception while inserting word {word}: {e}")
        finally:
            logger.info(f"Finished processing word: {word}")


if __name__ == "__main__":
    crawl()