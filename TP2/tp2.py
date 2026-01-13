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
                # This is a simple example - you might need more sophisticated logic
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
        print(f"Loaded {len(documents)} documents")

        # Create indexes
        print("Creating brand index...")
        brand_index = builder.create_brand_index(documents)
        builder.save_index_to_json(brand_index, os.path.join(output_dir, 'brand_index.json'))
        print(f"Brand index created with {len(brand_index)} brands")

        print("Creating origin index...")
        origin_index = builder.create_origin_index(documents)
        builder.save_index_to_json(origin_index, os.path.join(output_dir, 'origin_index.json'))
        print(f"Origin index created with {len(origin_index)} origins")

        print("Creating title position index...")
        title_index = builder.create_title_position_index(documents)
        builder.save_index_to_json(title_index, os.path.join(output_dir, 'title_index.json'))
        print(f"Title index created with {len(title_index)} tokens")

        print("Creating description position index...")
        description_index = builder.create_description_position_index(documents)
        builder.save_index_to_json(description_index, os.path.join(output_dir, 'description_index.json'))
        print(f"Description index created with {len(description_index)} tokens")

        print("Creating reviews index...")
        reviews_index = builder.create_reviews_index(documents)
        builder.save_index_to_json(reviews_index, os.path.join(output_dir, 'reviews_index.json'))
        print(f"Reviews index created with {len(reviews_index)} products")

        print("\nAll indexes created successfully!")
        print("Files saved in TP2 directory:")
        print("- brand_index.json")
        print("- origin_index.json")
        print("- title_index.json")
        print("- description_index.json")
        print("- reviews_index.json")

        # Show sample of brand and origin indexes
        print("\nSample of brand index:")
        for i, (brand, urls) in enumerate(list(brand_index.items())[:3]):
            print(f"  {brand}: {urls[:2]}...")

        print("\nSample of origin index:")
        for i, (origin, urls) in enumerate(list(origin_index.items())[:3]):
            print(f"  {origin}: {urls[:2]}...")

    except FileNotFoundError:
        print(f"Error: File {input_file} not found.")
        print("Please ensure the file exists in the correct location.")
    except Exception as e:
        print(f"Error: {str(e)}")


if __name__ == "__main__":
    main()