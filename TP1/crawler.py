# -*- coding: utf-8 -*-
"""
TP1 - Developpement d'un web crawler
ENSAI 2026
Version Python 3.11
"""

import urllib.request
import urllib.parse
import urllib.robotparser
from urllib.error import HTTPError, URLError
from bs4 import BeautifulSoup
import json
import time
from queue import PriorityQueue
import ssl


class WebCrawler:
    def __init__(self, start_url, max_pages=50, delay=1):
        """
        Initialise le crawler avec l'URL de depart
        """
        self.start_url = start_url
        parsed_url = urllib.parse.urlparse(start_url)
        self.base_domain = parsed_url.netloc or parsed_url.hostname
        self.max_pages = max_pages
        self.delay = delay

        # File d'attente prioritaire
        self.url_queue = PriorityQueue()

        # URLs deja visitees
        self.visited_urls = set()

        # Resultats stockes
        self.results = []

        # Parser pour robots.txt
        self.robot_parser = urllib.robotparser.RobotFileParser()
        self.robots_txt_loaded = False

    def fetch_robots_txt(self):
        """
        Recupere et parse le fichier robots.txt du site
        """
        robots_url = "https://" + self.base_domain + "/robots.txt"
        self.robot_parser.set_url(robots_url)
        try:
            # Creer un contexte SSL qui ignore la verification
            context = ssl._create_unverified_context()
            # Utiliser urllib.request pour contourner l'erreur SSL
            req = urllib.request.Request(robots_url, headers={'User-Agent': 'Mozilla/5.0'})
            response = urllib.request.urlopen(req, context=context, timeout=10)
            content = response.read().decode('utf-8', errors='ignore')

            # Parser le contenu manuellement
            self.robot_parser.parse(content.splitlines())
            self.robots_txt_loaded = True
            print("robots.txt recupere depuis " + robots_url)
        except Exception as e:
            print("Impossible de lire robots.txt (on continue sans): " + str(e))
            self.robots_txt_loaded = False

    def can_fetch(self, url):
        """
        Verifie si le crawler peut acceder a l'URL selon robots.txt
        """
        if not self.robots_txt_loaded:
            return True  # Si robots.txt n'est pas charge, on continue

        try:
            return self.robot_parser.can_fetch("*", url)
        except:
            return True

    def make_request(self, url):
        """
        Effectue une requete HTTP avec gestion d'erreurs et politesse
        """
        time.sleep(self.delay)

        try:
            # Creer un contexte SSL qui ignore la verification des certificats
            context = ssl._create_unverified_context()

            headers = {'User-Agent': 'Mozilla/5.0 (WebCrawler ENSAI)'}
            req = urllib.request.Request(url, headers=headers)
            response = urllib.request.urlopen(req, timeout=10, context=context)

            content_type = response.getheader('Content-Type', '')
            if 'text/html' not in content_type:
                print("  -> Contenu non HTML: " + content_type)
                return None

            html_bytes = response.read()

            # Essayer differents encodages
            encodings = ['utf-8', 'iso-8859-1', 'windows-1252']
            for encoding in encodings:
                try:
                    return html_bytes.decode(encoding)
                except UnicodeDecodeError:
                    continue

            # Si aucun encoding ne fonctionne
            return html_bytes.decode('utf-8', errors='ignore')

        except HTTPError as e:
            print("  -> Erreur HTTP: " + str(e.code))
            return None
        except URLError as e:
            print("  -> Erreur URL: " + str(e.reason))
            return None
        except Exception as e:
            print("  -> Erreur: " + str(e))
            return None

    def extract_content(self, html_content, url):
        """
        Extrait le titre, premier paragraphe et liens d'une page HTML
        """
        soup = BeautifulSoup(html_content, 'html.parser')

        title_tag = soup.find('title')
        title = title_tag.text.strip() if title_tag else "Sans titre"

        first_paragraph = ""
        for p in soup.find_all('p'):
            text = p.text.strip()
            if text and len(text) > 20:
                first_paragraph = text
                break

        if not first_paragraph:
            first_p = soup.find('p')
            if first_p:
                first_paragraph = first_p.text.strip()

        links = []
        body = soup.find('body')
        if body:
            for link_tag in body.find_all('a', href=True):
                link_url = link_tag['href']
                link_text = link_tag.text.strip()[:100]

                # Eviter les liens vides ou javascript
                if not link_url or link_url.startswith(('javascript:', 'mailto:', 'tel:', '#')):
                    continue

                full_link = urllib.parse.urljoin(url, link_url)

                parsed_link = urllib.parse.urlparse(full_link)
                if parsed_link.netloc and parsed_link.netloc != self.base_domain:
                    continue

                # Nettoyer l'URL (enlever les fragments #)
                clean_link = urllib.parse.urlunparse((
                    parsed_link.scheme,
                    parsed_link.netloc,
                    parsed_link.path,
                    parsed_link.params,
                    parsed_link.query,
                    ''  # Supprimer le fragment
                ))

                links.append({
                    'url': clean_link,
                    'text': link_text if link_text else "Lien sans texte"
                })

        return {
            'title': title,
            'url': url,
            'first_paragraph': first_paragraph,
            'links': links
        }

    def get_priority(self, url):
        """
        Attribue une priorite a une URL
        Priorite plus basse = plus haute priorite
        """
        if 'product' in url.lower():
            return 0
        return 1

    def add_url_to_queue(self, url):
        """
        Ajoute une URL a la file d'attente avec priorite
        """
        if url in self.visited_urls:
            return

        # Verifier si l'URL est deja dans la file
        for item in list(self.url_queue.queue):
            if item[1] == url:
                return

        priority = self.get_priority(url)
        self.url_queue.put((priority, url))

    def crawl(self):
        """
        Fonction principale qui execute le crawling
        """
        print("Debut du crawling a partir de: " + self.start_url)
        print("Maximum " + str(self.max_pages) + " pages a visiter")

        # Essayer de recuperer robots.txt, mais continuer meme en cas d'echec
        print("Tentative de recuperation de robots.txt...")
        self.fetch_robots_txt()

        if not self.robots_txt_loaded:
            print("ATTENTION: robots.txt non charge. Le crawler continuera sans restrictions.")

        self.add_url_to_queue(self.start_url)

        pages_crawled = 0

        while not self.url_queue.empty() and pages_crawled < self.max_pages:
            priority, current_url = self.url_queue.get()

            # Verifier que l'URL n'a pas deja ete visitee (double verification)
            if current_url in self.visited_urls:
                continue

            self.visited_urls.add(current_url)

            print("\n[" + str(pages_crawled + 1) + "/" + str(self.max_pages) + "] " + current_url)
            print("   Priorite: " + str(priority))

            # Verifier robots.txt (retourne True si non charge)
            if not self.can_fetch(current_url):
                print("   -> Bloque par robots.txt")
                continue

            html_content = self.make_request(current_url)

            if html_content:
                page_data = self.extract_content(html_content, current_url)
                self.results.append(page_data)

                # Ajouter les nouveaux liens a la file
                new_links_count = 0
                for link in page_data['links']:
                    if link['url'] not in self.visited_urls:
                        self.add_url_to_queue(link['url'])
                        new_links_count += 1

                pages_crawled += 1
                print("   -> OK: " + str(len(page_data['links'])) + " liens trouves (" + str(new_links_count) + " nouveaux)")
            else:
                print("   -> ECHEC: impossible de recuperer la page")

        print("\nCrawling termine. " + str(len(self.results)) + " pages visitees.")

    def save_results(self, filename="crawler_results.json"):
        """
        Sauvegarde les resultats dans un fichier JSON
        """
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.results, f, ensure_ascii=False, indent=2)
            print("\nResultats sauvegardes dans " + filename)
            return True
        except Exception as e:
            print("\nErreur lors de la sauvegarde: " + str(e))
            return False

def main():
    """
    Fonction principale pour executer le crawler
    """
    start_url = "https://web-scraping.dev/products"

    crawler = WebCrawler(
        start_url=start_url,
        max_pages=50,
        delay=0.5
    )

    crawler.crawl()
    crawler.save_results()

    print("\n" + "="*50)
    print("RESUME")
    print("="*50)
    print("Pages visitees: " + str(len(crawler.results)))

    if crawler.results:
        product_pages = sum(1 for page in crawler.results
                           if 'product' in page['url'].lower())
        print("Pages produits: " + str(product_pages))

        print("\nURLs visitees:")
        for i, page in enumerate(crawler.results, 1):
            product_indicator = " [PRODUIT]" if 'product' in page['url'].lower() else ""
            print(str(i).rjust(3) + ". " + page['url'] + product_indicator)

        print("\n" + "="*50)
        print("PREMIER RESULTAT DETAILLE")
        print("="*50)
        first_result = crawler.results[0]
        print("Titre: " + first_result['title'])
        print("URL: " + first_result['url'])
        print("Premier paragraphe: " + first_result['first_paragraph'][:200] + "...")
        print("Nombre de liens trouves: " + str(len(first_result['links'])))

        if first_result['links']:
            print("\n5 premiers liens:")
            for i, link in enumerate(first_result['links'][:5], 1):
                print("  " + str(i) + ". " + link['text'][:50] + "... -> " + link['url'])
    else:
        print("Aucune page visitee. Verifiez la connexion Internet et les permissions.")


if __name__ == "__main__":
    main()