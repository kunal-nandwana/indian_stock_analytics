"""Simple scraper for Moneycontrol news lists.

This module fetches a Moneycontrol listing page (for example:
https://www.moneycontrol.com/news/business/stocks/...) and extracts
headline text stored in an <h2> tag that sits inside <li> items within
the target section.

Assumptions / notes:
- You asked for the section "id mid-contener contener clearfix". That
  looks like a class attribute (space-separated) rather than a single
  id. This scraper tries multiple strategies: section with id
  "mid-contener", section with class names "mid-contener contener
  clearfix", or a section whose id or class contains the token
  "mid-contener".
- It extracts text from <h2> tags that are children (direct or nested)
  of <li> elements inside that section.
- The module is intentionally small and dependency-light (requests + bs4).
"""

from typing import List, Optional
import logging
import os
import requests
from bs4 import BeautifulSoup
import csv
import json
from urllib.parse import urljoin
from datetime import date, timedelta

logging.basicConfig(level=logging.INFO)

today = date.today()
current_date    = today.strftime("%d-%m-%Y")

def create_session(user_agent: str = None) -> requests.Session:
    s = requests.Session()
    ua = user_agent or (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36"
    )
    # More complete headers to appear like a real browser
    s.headers.update({
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
        "Connection": "keep-alive",
    })
    return s


def _find_target_section(soup: BeautifulSoup):
    # Try by id first
    sec = soup.find('section', id='mid-contener')
    if sec:
        return sec

    # Try by classes (space-separated tokens)
    candidates = soup.find_all('section')
    for c in candidates:
        cls = c.get('class') or []
        if isinstance(cls, str):
            cls = cls.split()
        if 'mid-contener' in cls or ('contener' in cls and 'clearfix' in cls):
            return c

    # Fallback: any element (section or div) with id or class containing 'mid-contener'
    fallback = soup.find(lambda tag: (tag.name in ('section', 'div')) and (
        ('mid-contener' in (tag.get('id') or '')) or
        any('mid-contener' in (c or '') for c in tag.get('class') or [])
    ))
    return fallback


def extract_headlines_from_section(section) -> List[str]:
    if not section:
        return []
    headlines = []
    # find all li elements and then look for h2 inside them
    for li in section.find_all('li'):
        h2 = li.find('h2')
        if h2:
            text = h2.get_text(strip=True)
            if text:
                headlines.append(text)
    return headlines


def fetch_article(article_url: str, session: Optional[requests.Session] = None, timeout: int = 15) -> dict:
    """Fetch an article URL and return structured data:
    { 'text': combined_paragraphs, 'schedule': {'spans': [...], 'schedule_text': '...'} }
    The schedule fields are extracted from a header container with class
    tokens like 'clearfix articlename_join_follow' and its child 'article_schedule'.
    """
    sess = session or create_session()
    try:
        resp = sess.get(article_url, timeout=timeout)
        resp.raise_for_status()
    except Exception:
        logging.exception('Failed to fetch article %s', article_url)
        return {'text': '', 'schedule': {}}

    a_soup = BeautifulSoup(resp.text, 'html.parser')
    # Find a div/section with class containing either 'content_wrapper' or 'arti-flow'
    article_node = a_soup.find(lambda tag: (tag.name in ('div', 'section')) and (
        any('content_wrapper' in (c or '') for c in tag.get('class') or []) or
        any('arti-flow' in (c or '') for c in tag.get('class') or [])
    ))

    # Fallback: look for a container with 'article' token
    if article_node is None:
        article_node = a_soup.find(lambda tag: (tag.name in ('div', 'section')) and (
            any('article' in (c or '') for c in tag.get('class') or [])
        ))

    if article_node is None:
        logging.debug('Article container not found for %s', article_url)
        # try to return all paragraph text on page as a fallback
        ps = [p.get_text(strip=True) for p in a_soup.find_all('p') if p.get_text(strip=True)]
        return {'text': '\n\n'.join(ps), 'schedule': {}}

    paragraphs = [p.get_text(strip=True) for p in article_node.find_all('p') if p.get_text(strip=True)]
    article_text = '\n\n'.join(paragraphs)

    # Extract schedule metadata from header container
    schedule_data = {'spans': [], 'schedule_text': ''}
    header_container = a_soup.find(lambda tag: (tag.name == 'div') and (
        any('articlename_join_follow' in (c or '') for c in tag.get('class') or []) or
        ('articlename_join_follow' in (tag.get('id') or '')) or
        (all(tok in (tag.get('class') or []) for tok in ['clearfix', 'articlename_join_follow']) if tag.get('class') else False)
    ))

    if header_container:
        sched = header_container.find(lambda tag: (tag.name == 'div') and any('article_schedule' in (c or '') for c in tag.get('class') or []))
        if sched:
            # collect spans and schedule div text
            spans = [s.get_text(strip=True) for s in sched.find_all('span') if s.get_text(strip=True)]
            schedule_text = sched.get_text(separator=' ', strip=True)
            schedule_data['spans'] = spans
            schedule_data['schedule_text'] = schedule_text

    return {'text': article_text, 'schedule': schedule_data}


def fetch_headlines(url: str, session: Optional[requests.Session] = None, timeout: int = 15) -> List[dict]:
    """Fetch a Moneycontrol page and return a list of items: {'headline','url','article'}.

    Returns a list of dicts (may be empty).
    """
    sess = session or create_session()
    logging.info("Fetching %s", url)

    try:
        resp = sess.get(url, timeout=timeout)
        resp.raise_for_status()
    except requests.exceptions.HTTPError as he:
        # Common site blocking shows up as 403 Forbidden — try a couple of
        # alternate browser User-Agents before giving up.
        if getattr(he.response, 'status_code', None) == 403:
            logging.warning("Got 403 for %s — retrying with alternate User-Agents", url)
            alt_uas = [
                # macOS Safari
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15",
                # Older Chrome UA
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
                # Mobile UA
                "Mozilla/5.0 (Linux; Android 10; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Mobile Safari/537.36",
            ]
            for ua in alt_uas:
                try:
                    alt_sess = create_session(user_agent=ua)
                    resp = alt_sess.get(url, timeout=timeout)
                    resp.raise_for_status()
                    break
                except requests.exceptions.HTTPError:
                    logging.debug("UA %s also failed with status %s", ua, getattr(resp, 'status_code', None))
                    continue
            else:
                # all retries failed, re-raise the original exception
                raise
        else:
            raise

    soup = BeautifulSoup(resp.text, 'html.parser')

    section = _find_target_section(soup)
    if section is None:
        logging.warning("Target section not found on page: %s", url)
        return []

    items = []
    for li in section.find_all('li'):
        h2 = li.find('h2')
        if not h2:
            continue
        headline_text = h2.get_text(strip=True)
        a_tag = li.find('a', href=True) or h2.find('a', href=True)
        article_url = ''
        article_text = ''
        if a_tag:
            href = a_tag.get('href')
            if href:
                article_url = urljoin(url, href)
                article_text = fetch_article(article_url, session=sess, timeout=timeout)

        items.append({'headline': headline_text, 'url': article_url, 'article': article_text})

    return items


def _extract_page_number_from_url(url: str):
    """Return (base_url, current_page) where base_url can be formatted with page number.

    Examples:
    - https://.../page-2/ -> ('https://.../page-{n}/', 2)
    - https://.../ -> ('https://.../page-{n}/', 1)
    """
    import re
    m = re.search(r'(.*?/)(?:page-)(\d+)(/?)$', url)
    if m:
        prefix = m.group(1)
        return (prefix + 'page-{n}/', int(m.group(2)))
    # No page in URL: ensure trailing slash
    if not url.endswith('/'):
        url = url + '/'
    return (url + 'page-{n}/', 1)


def fetch_headlines_pages(start_url: str, max_pages: int = 1, until_empty: bool = False) -> List[str]:
    """Fetch headlines across multiple paginated listing pages.

    - start_url: any page URL (may contain page-N).
    - max_pages: how many pages to fetch (ignored if until_empty True which stops when a page yields no headlines).
    """
    base_template, start_page = _extract_page_number_from_url(start_url)
    all_headlines = []
    page = start_page
    pages_fetched = 0
    while True:
        url = base_template.format(n=page)
        try:
            headlines = fetch_headlines(url)
        except Exception:
            logging.exception('Failed to fetch page %s', url)
            break

        # Print page-level details to CLI as we fetch each page
        logging.info('Page %s -> %d headlines', page, len(headlines))
        print(f"\n=== Page {page} — {len(headlines)} headlines ===")
        if headlines:
            # tag each item with its page number
            for item in headlines:
                try:
                    item['page'] = page
                except Exception:
                    pass
                    hl = item.get('headline')
                    url_item = item.get('url')
                    art_blob = item.get('article') or {}
                    art_text = art_blob.get('text') if isinstance(art_blob, dict) else art_blob
                    schedule = art_blob.get('schedule') if isinstance(art_blob, dict) else {}

                    print('-', hl)
                    if url_item:
                        print('  (url:', url_item + ')')

                    # Print schedule metadata if present
                    if schedule:
                        spans = schedule.get('spans') or []
                        sched_txt = schedule.get('schedule_text') or ''
                        if spans:
                            print('  Schedule spans:', '; '.join(spans))
                        if sched_txt:
                            print('  Schedule text:', sched_txt)

                    if art_text:
                        paras = art_text.split('\n\n')
                        if paras:
                            print('  Preview:', paras[0])
                        for p in paras:
                            for line in p.splitlines():
                                print('    ' + line)
                    print('')
            all_headlines.extend(headlines)
        else:
            print(f"(No headlines found on page {page})")
            if until_empty:
                print(f"Stopping: reached empty page {page}.")
                break

        pages_fetched += 1
        if not until_empty and pages_fetched >= max_pages:
            break

        page += 1
    return all_headlines


def save_items_to_csv(items: List[dict], path: str):
    """Flatten items and write to CSV with columns:
    page, headline, url, article_text, schedule_spans, schedule_text
    """
    if not items:
        logging.info('No items to save to CSV')
        return

    fieldnames = ['page', 'headline', 'url', 'article_text', 'schedule_spans', 'schedule_text']
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for it in items:
            art_blob = it.get('article') or {}
            art_text = art_blob.get('text') if isinstance(art_blob, dict) else (art_blob or '')
            schedule = art_blob.get('schedule') if isinstance(art_blob, dict) else {}
            spans = schedule.get('spans') if schedule else []
            sched_txt = schedule.get('schedule_text') if schedule else ''
            row = {
                'page': it.get('page') or '',
                'headline': it.get('headline') or '',
                'url': it.get('url') or '',
                'article_text': art_text or '',
                'schedule_spans': '; '.join(spans) if spans else '',
                'schedule_text': sched_txt or '',
            }
            writer.writerow(row)
    logging.info('Saved %d items to CSV %s', len(items), path)


def save_headlines_csv(headlines: List[str], path: str):
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['headline'])
        for h in headlines:
            writer.writerow([h])


def save_headlines_json(headlines: List[str], path: str):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump({'headlines': headlines}, f, ensure_ascii=False, indent=2)


if __name__ == '__main__':
    # Run without CLI arguments: traverse pages one-by-one until an empty page is found.
    # Start URL priority: MONEYCONTROL_URL env var > sensible default.
    default_url = 'https://www.moneycontrol.com/news/business/stocks/'
    start_url = os.environ.get('MONEYCONTROL_URL') or default_url

    try:
        # traverse pages 1..5 only for now
        headlines = fetch_headlines_pages(start_url, max_pages=5, until_empty=False)
    except Exception as e:
        logging.exception('Failed to fetch or parse pages starting at %s: %s', start_url, e)
        raise

    print(f"Found {len(headlines)} total headlines")
    for h in headlines:
        print('-', h)

    
    newpath = f'/Users/kunal.nandwana/Library/CloudStorage/OneDrive-OneWorkplace/Documents/Personal_Projects/Data/Indian Stock Analytics/news_data/{current_date}' 
    if not os.path.exists(newpath):
        os.makedirs(newpath)


    # Save to CSV if requested via env var
    out_csv = os.environ.get('MONEYCONTROL_OUT_CSV') or f"{newpath}/moneycontrol_headlines.csv"
    try:
        save_items_to_csv(headlines, out_csv)
    except Exception:
        logging.exception('Failed to save items to CSV %s', out_csv)

