import json
import re
from typing import Dict, List, Any, Set
import os
import string


class IndexBuilder:
    def __init__(self):
        self.stopwords = set([
            'a', 'an', 'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to',
            'for', 'of', 'with', 'by', 'is', 'are', 'was', 'were', 'be',
            'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did',
            'will', 'would', 'shall', 'should', 'may', 'might', 'must',
            'can', 'could', 'i', 'you', 'he', 'she', 'it', 'we', 'they',
            'me', 'him', 'her', 'us', 'them', 'my', 'your', 'his', 'its',
            'our', 'their', 'mine', 'yours', 'hers', 'ours', 'theirs'
        ])

    def clean_text(self, text: str) -> List[str]:
        """Clean and tokenize text, removing stopwords and punctuation."""
        if not text:
            return []

        # Remove punctuation and convert to lowercase
        text = text.lower()
        text = text.translate(str.maketrans('', '', string.punctuation))

        # Tokenize by spaces
        tokens = text.split()

        # Remove stopwords
        tokens = [token for token in tokens if token not in self.stopwords]

        return tokens

    def parse_jsonl(self, filepath: str) -> List[Dict[str, Any]]:
        """Parse JSONL file and return list of documents."""
        documents = []
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    documents.append(json.loads(line))
        return documents

    def extract_product_id_from_url(self, url: str) -> str:
        """Extract product ID from URL."""
        # Extract the number after /product/
        match = re.search(r'/product/(\d+)', url)
        if match:
            return match.group(1)
        return ""

    # ==================== INDEXES PRINCIPAUX ====================

    def create_brand_index(self, documents: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """Create brand index from product features."""
        brand_index = {}

        for doc in documents:
            url = doc.get('url', '')
            features = doc.get('product_features', {})

            # Look for brand in features
            brand = None

            # Check for brand field (case-insensitive)
            for key, value in features.items():
                if 'brand' in key.lower():
                    brand = str(value).strip()
                    break

            # If no brand found in features, try to extract from title or description
            if not brand:
                title = doc.get('title', '')
                # Simple heuristic: look for brand-like words in title
                title_tokens = self.clean_text(title)
                if title_tokens:
                    # Assume first token might be brand (for this dataset)
                    brand = title_tokens[0].title() if title_tokens else ""

            if brand:
                # Normalize brand name
                brand = brand.lower().strip()
                if brand not in brand_index:
                    brand_index[brand] = []

                # Add URL to brand index
                if url not in brand_index[brand]:
                    brand_index[brand].append(url)

        # Sort URLs for each brand
        for brand in brand_index:
            brand_index[brand].sort()

        return brand_index

    def create_origin_index(self, documents: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """Create origin index from product features (looking for 'made in' or similar)."""
        origin_index = {}

        for doc in documents:
            url = doc.get('url', '')
            features = doc.get('product_features', {})

            # Look for origin/made in information
            origin = None

            # Check for origin-related fields (case-insensitive)
            for key, value in features.items():
                key_lower = key.lower()
                if any(origin_keyword in key_lower for origin_keyword in
                       ['origin', 'made in', 'country', 'manufactured', 'produced']):
                    origin = str(value).strip()
                    break

            # If no origin found in features, check description
            if not origin:
                description = doc.get('description', '').lower()
                # Simple pattern matching for "made in [country]"
                made_in_match = re.search(r'made in\s+([a-zA-Z\s]+)', description)
                if made_in_match:
                    origin = made_in_match.group(1).strip()

            if origin:
                # Normalize origin name
                origin = origin.lower().strip()
                if origin not in origin_index:
                    origin_index[origin] = []

                # Add URL to origin index
                if url not in origin_index[origin]:
                    origin_index[origin].append(url)

        # Sort URLs for each origin
        for origin in origin_index:
            origin_index[origin].sort()

        return origin_index

    def create_title_position_index(self, documents: List[Dict[str, Any]]) -> Dict[str, Dict[str, List[int]]]:
        """Create title index with positions."""
        title_index = {}

        for doc in documents:
            url = doc.get('url', '')
            title = doc.get('title', '')

            tokens = self.clean_text(title)

            for position, token in enumerate(tokens):
                if token not in title_index:
                    title_index[token] = {}

                if url not in title_index[token]:
                    title_index[token][url] = []

                title_index[token][url].append(position)

        return title_index

    def create_description_position_index(self, documents: List[Dict[str, Any]]) -> Dict[str, Dict[str, List[int]]]:
        """Create description index with positions."""
        description_index = {}

        for doc in documents:
            url = doc.get('url', '')
            description = doc.get('description', '')

            tokens = self.clean_text(description)

            for position, token in enumerate(tokens):
                if token not in description_index:
                    description_index[token] = {}

                if url not in description_index[token]:
                    description_index[token][url] = []

                description_index[token][url].append(position)

        return description_index

    def create_reviews_index(self, documents: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Create reviews index with summary statistics."""
        reviews_index = {}

        for doc in documents:
            url = doc.get('url', '')
            reviews = doc.get('product_reviews', [])

            if reviews:
                total_reviews = len(reviews)

                # Calculate average rating
                total_rating = sum(review.get('rating', 0) for review in reviews)
                avg_rating = total_rating / total_reviews if total_reviews > 0 else 0

                # Get latest review (assuming reviews are sorted by date)
                latest_review = None
                if reviews:
                    # Sort by date if available
                    sorted_reviews = sorted(
                        reviews,
                        key=lambda x: x.get('date', ''),
                        reverse=True
                    )
                    latest_review = sorted_reviews[0].get('rating', 0)

                reviews_index[url] = {
                    'total_reviews': total_reviews,
                    'average_rating': round(avg_rating, 2),
                    'latest_rating': latest_review
                }

        return reviews_index

    # ==================== FEATURES SUPPLÉMENTAIRES ====================

    def create_material_index(self, documents: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """Create material index from product features."""
        material_index = {}

        for doc in documents:
            url = doc.get('url', '')
            features = doc.get('product_features', {})

            # Look for material in features
            material = None

            # Check for material field (case-insensitive)
            for key, value in features.items():
                if 'material' in key.lower():
                    material = str(value).strip()
                    break

            if material:
                # Normalize material name
                material = material.lower().strip()
                if material not in material_index:
                    material_index[material] = []

                # Add URL to material index
                if url not in material_index[material]:
                    material_index[material].append(url)

        # Sort URLs for each material
        for material in material_index:
            material_index[material].sort()

        return material_index

    def create_size_index(self, documents: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """Create size index from product features."""
        size_index = {}

        for doc in documents:
            url = doc.get('url', '')
            features = doc.get('product_features', {})

            # Look for sizes in features
            sizes = []

            # Check for size fields (case-insensitive)
            for key, value in features.items():
                if any(size_keyword in key.lower() for size_keyword in ['size', 'sizes']):
                    size_text = str(value).strip()
                    # Parse sizes (could be "small, medium, large" or "6,7,8,9")
                    size_parts = re.split(r'[,|/]|and', size_text)
                    for part in size_parts:
                        clean_size = part.strip().lower()
                        if clean_size:
                            sizes.append(clean_size)
                    break

            # Also check variant in URL for size information
            if 'variant=' in url:
                variant_match = re.search(r'variant=([a-zA-Z0-9-]+)', url)
                if variant_match:
                    variant = variant_match.group(1)
                    # Check if variant contains size information
                    for size_keyword in ['small', 'medium', 'large', 'xs', 's', 'm', 'l', 'xl']:
                        if size_keyword in variant.lower():
                            sizes.append(size_keyword)

            # Add to size index
            for size in sizes:
                if size not in size_index:
                    size_index[size] = []
                if url not in size_index[size]:
                    size_index[size].append(url)

        # Sort URLs for each size
        for size in size_index:
            size_index[size].sort()

        return size_index

    def create_color_index(self, documents: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """Create color index from product features."""
        color_index = {}

        # Common colors to look for
        common_colors = ['red', 'blue', 'green', 'yellow', 'black', 'white',
                         'orange', 'purple', 'pink', 'brown', 'gray', 'grey',
                         'beige', 'silver', 'gold', 'navy', 'teal', 'maroon']

        for doc in documents:
            url = doc.get('url', '')
            features = doc.get('product_features', {})

            # Look for colors in features
            colors = []

            # Check for color fields (case-insensitive)
            for key, value in features.items():
                if any(color_keyword in key.lower() for color_keyword in
                       ['color', 'colors', 'flavor', 'flavors']):
                    color_text = str(value).strip().lower()
                    # Parse colors from text
                    for color in common_colors:
                        if color in color_text:
                            colors.append(color)

            # Also check variant in URL for color information
            if 'variant=' in url:
                variant_match = re.search(r'variant=([a-zA-Z0-9-]+)', url)
                if variant_match:
                    variant = variant_match.group(1)
                    for color in common_colors:
                        if color in variant.lower():
                            colors.append(color)

            # Add to color index
            for color in colors:
                if color not in color_index:
                    color_index[color] = []
                if url not in color_index[color]:
                    color_index[color].append(url)

        # Sort URLs for each color
        for color in color_index:
            color_index[color].sort()

        return color_index

    def create_category_index(self, documents: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """Create category index by analyzing product content."""
        category_index = {}

        # Category keywords mapping
        category_keywords = {
            'food': ['chocolate', 'candy', 'potion', 'drink', 'beverage', 'cola', 'berry', 'flavor'],
            'footwear': ['sneakers', 'shoes', 'boots', 'sandals', 'heel', 'footbed', 'sole'],
            'clothing': ['beanie', 'hat', 'dress', 'shirt', 'pants', 'wardrobe', 'outfit'],
            'electronics': ['led', 'light', 'battery', 'electronic', 'digital'],
            'accessories': ['accessory', 'bag', 'wallet', 'belt', 'watch'],
            'sports': ['hiking', 'outdoor', 'running', 'sport', 'active', 'performance'],
            'beauty': ['cream', 'perfume', 'cosmetic', 'beauty', 'skin'],
            'home': ['furniture', 'decor', 'kitchen', 'home', 'garden']
        }

        for doc in documents:
            url = doc.get('url', '')
            title = doc.get('title', '').lower()
            description = doc.get('description', '').lower()

            # Determine category based on keywords
            categories_found = []
            for category, keywords in category_keywords.items():
                for keyword in keywords:
                    if keyword in title or keyword in description:
                        categories_found.append(category)
                        break

            # If no category found, use default
            if not categories_found:
                categories_found = ['other']

            # Add to category index
            for category in categories_found:
                if category not in category_index:
                    category_index[category] = []
                if url not in category_index[category]:
                    category_index[category].append(url)

        # Sort URLs for each category
        for category in category_index:
            category_index[category].sort()

        return category_index

    def create_price_range_index(self, documents: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """Create price range index by analyzing reviews and descriptions."""
        price_index = {
            'budget': [],
            'midrange': [],
            'premium': []
        }

        # Keywords indicating price ranges
        price_keywords = {
            'budget': ['affordable', 'cheap', 'inexpensive', 'budget', 'value', 'economical'],
            'premium': ['expensive', 'luxury', 'premium', 'high-end', 'exclusive', 'pricey']
        }

        for doc in documents:
            url = doc.get('url', '')
            description = doc.get('description', '').lower()
            reviews = doc.get('product_reviews', [])

            # Collect all text to analyze
            all_text = description

            # Add review text
            for review in reviews:
                all_text += " " + review.get('text', '').lower()

            # Determine price range based on keywords
            price_range = 'midrange'  # default

            # Check for budget keywords
            for keyword in price_keywords['budget']:
                if keyword in all_text:
                    price_range = 'budget'
                    break

            # Check for premium keywords (only if not already budget)
            if price_range == 'midrange':
                for keyword in price_keywords['premium']:
                    if keyword in all_text:
                        price_range = 'premium'
                        break

            # Add to price index
            if url not in price_index[price_range]:
                price_index[price_range].append(url)

        # Sort URLs for each price range
        for price_range in price_index:
            price_index[price_range].sort()

        return price_index

    def create_features_index(self, documents: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """Create special features index from product features."""
        features_index = {}

        # Common special features to look for
        special_features_keywords = [
            'waterproof', 'led', 'light-up', 'adjustable', 'cushioned',
            'breathable', 'durable', 'premium', 'genuine', 'sustainable',
            'eco-friendly', 'recyclable', 'washable', 'foldable', 'portable'
        ]

        for doc in documents:
            url = doc.get('url', '')
            features = doc.get('product_features', {})
            description = doc.get('description', '').lower()

            # Collect all features text
            all_features_text = ""
            for key, value in features.items():
                all_features_text += " " + str(value).lower()
            all_features_text += " " + description

            # Look for special features
            for feature in special_features_keywords:
                if feature in all_features_text:
                    # Get the full feature phrase if possible
                    feature_phrase = feature
                    # Try to find a more complete phrase
                    words = all_features_text.split()
                    for i, word in enumerate(words):
                        if feature in word and i < len(words) - 1:
                            # Get context around the feature word
                            start = max(0, i - 1)
                            end = min(len(words), i + 2)
                            feature_phrase = " ".join(words[start:end])
                            break

                    if feature_phrase not in features_index:
                        features_index[feature_phrase] = []
                    if url not in features_index[feature_phrase]:
                        features_index[feature_phrase].append(url)

        # Sort URLs for each feature
        for feature in features_index:
            features_index[feature].sort()

        return features_index

    def save_index_to_json(self, index: Dict, filename: str):
        """Save index to JSON file."""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(index, f, indent=2, ensure_ascii=False)

    def load_index_from_json(self, filename: str) -> Dict:
        """Load index from JSON file."""
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)


def main():
    # Initialize index builder
    builder = IndexBuilder()

    # Define paths
    input_file = os.path.join('TP2', 'input', 'products.jsonl')
    output_dir = 'TP2'

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    try:
        # Parse JSONL file
        print(f"Parsing {input_file}...")
        documents = builder.parse_jsonl(input_file)
        print(f"Loaded {len(documents)} documents\n")

        # ==================== INDEXES PRINCIPAUX ====================
        print("=" * 50)
        print("CRÉATION DES INDEXES PRINCIPAUX")
        print("=" * 50)

        print("1. Creating brand index...")
        brand_index = builder.create_brand_index(documents)
        builder.save_index_to_json(brand_index, os.path.join(output_dir, 'brand_index.json'))
        print(f"Brand index created with {len(brand_index)} brands")

        print("2. Creating origin index...")
        origin_index = builder.create_origin_index(documents)
        builder.save_index_to_json(origin_index, os.path.join(output_dir, 'origin_index.json'))
        print(f"Origin index created with {len(origin_index)} origins")

        print("3. Creating title position index...")
        title_index = builder.create_title_position_index(documents)
        builder.save_index_to_json(title_index, os.path.join(output_dir, 'title_index.json'))
        print(f"Title index created with {len(title_index)} tokens")

        print("4. Creating description position index...")
        description_index = builder.create_description_position_index(documents)
        builder.save_index_to_json(description_index, os.path.join(output_dir, 'description_index.json'))
        print(f"Description index created with {len(description_index)} tokens")

        print("5. Creating reviews index...")
        reviews_index = builder.create_reviews_index(documents)
        builder.save_index_to_json(reviews_index, os.path.join(output_dir, 'reviews_index.json'))
        print(f"Reviews index created with {len(reviews_index)} products")

        # ==================== FEATURES SUPPLÉMENTAIRES ====================
        print("\n" + "=" * 50)
        print("CRÉATION DES FEATURES SUPPLÉMENTAIRES")
        print("=" * 50)

        print("6. Creating material index...")
        material_index = builder.create_material_index(documents)
        builder.save_index_to_json(material_index, os.path.join(output_dir, 'material_index.json'))
        print(f"Material index created with {len(material_index)} materials")

        print("7. Creating size index...")
        size_index = builder.create_size_index(documents)
        builder.save_index_to_json(size_index, os.path.join(output_dir, 'size_index.json'))
        print(f"Size index created with {len(size_index)} sizes")

        print("8. Creating color index...")
        color_index = builder.create_color_index(documents)
        builder.save_index_to_json(color_index, os.path.join(output_dir, 'color_index.json'))
        print(f"Color index created with {len(color_index)} colors")

        print("9. Creating category index...")
        category_index = builder.create_category_index(documents)
        builder.save_index_to_json(category_index, os.path.join(output_dir, 'category_index.json'))
        print(f"Category index created with {len(category_index)} categories")

        print("10. Creating price range index...")
        price_index = builder.create_price_range_index(documents)
        builder.save_index_to_json(price_index, os.path.join(output_dir, 'price_index.json'))
        print(f"Price range index created with 3 price ranges")

        print("11. Creating special features index...")
        features_index = builder.create_features_index(documents)
        builder.save_index_to_json(features_index, os.path.join(output_dir, 'features_index.json'))
        print(f"Special features index created with {len(features_index)} features")

        # ==================== RÉSUMÉ ====================
        print("\n" + "=" * 50)
        print("RÉSUMÉ DE L'INDEXATION")
        print("=" * 50)
        print(f"✓ Total documents traités : {len(documents)}")
        print(f"✓ Total index générés : 11")
        print(f"✓ Fichiers sauvegardés dans : {output_dir}/")
        print("\nListe des fichiers générés :")
        print("  [INDEXES PRINCIPAUX]")
        print("  - brand_index.json")
        print("  - origin_index.json")
        print("  - title_index.json")
        print("  - description_index.json")
        print("  - reviews_index.json")
        print("\n  [FEATURES SUPPLÉMENTAIRES]")
        print("  - material_index.json")
        print("  - size_index.json")
        print("  - color_index.json")
        print("  - category_index.json")
        print("  - price_index.json")
        print("  - features_index.json")

        # ==================== EXEMPLES ====================
        print("\n" + "=" * 50)
        print("EXEMPLES D'UTILISATION")
        print("=" * 50)

        # Afficher quelques exemples
        print("\nExemple 1 - Marques trouvées :")
        for i, (brand, urls) in enumerate(list(brand_index.items())[:5]):
            print(f"  • {brand}: {len(urls)} produits")

        print("\nExemple 2 - Matériaux trouvés :")
        for i, (material, urls) in enumerate(list(material_index.items())[:5]):
            material_display = material[:30] + "..." if len(material) > 30 else material
            print(f"  • {material_display}: {len(urls)} produits")

        print("\nExemple 3 - Catégories trouvées :")
        for category, urls in category_index.items():
            print(f"  • {category}: {len(urls)} produits")

        print("\nIndexation terminée avec succès ! 🎉")

    except FileNotFoundError:
        print(f"Erreur : Fichier {input_file} non trouvé.")
        print("Veuillez vous assurer que le fichier existe à l'emplacement correct.")
    except json.JSONDecodeError as e:
        print(f"Erreur JSON : {str(e)}")
        print("Veuillez vérifier le format du fichier products.jsonl")
    except Exception as e:
        print(f"Erreur inattendue : {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()