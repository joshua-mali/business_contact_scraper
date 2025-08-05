import csv
import json
import os
import re
import time
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

GOOGLE_PLACES_API = os.getenv("GOOGLE_PLACES_API")

class BusinessContactScraper:
    def __init__(self, google_api_key):
        self.google_api_key = google_api_key
        self.places_base_url = "https://maps.googleapis.com/maps/api/place"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
    def search_businesses(self, query, location, radius=5000, max_results=20):
        """
        Search for businesses using Google Places API
        """
        businesses = []
        
        # Initial search
        search_url = f"{self.places_base_url}/textsearch/json"
        params = {
            'query': f"{query} in {location}",
            'key': self.google_api_key,
            'radius': radius
        }
        
        try:
            response = requests.get(search_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data['status'] != 'OK':
                print(f"API Error: {data.get('status', 'Unknown error')}")
                if 'error_message' in data:
                    print(f"Error details: {data['error_message']}")
                print(f"API Key being used: {self.google_api_key[:10]}..." if self.google_api_key else "No API key provided")
                return businesses
                
            # Process results
            for place in data.get('results', []):
                if len(businesses) >= max_results:
                    break
                    
                business_info = self.get_place_details(place['place_id'])
                if business_info:
                    businesses.append(business_info)
                    
                # Rate limiting
                time.sleep(0.1)
                
        except requests.RequestException as e:
            print(f"Error searching businesses: {e}")
            
        return businesses
    
    def get_place_details(self, place_id):
        """
        Get detailed information for a specific place
        """
        details_url = f"{self.places_base_url}/details/json"
        params = {
            'place_id': place_id,
            'fields': 'name,formatted_address,formatted_phone_number,website,business_status,types',
            'key': self.google_api_key
        }
        
        try:
            response = requests.get(details_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data['status'] == 'OK':
                result = data['result']
                return {
                    'name': result.get('name', ''),
                    'address': result.get('formatted_address', ''),
                    'phone': result.get('formatted_phone_number', ''),
                    'website': result.get('website', ''),
                    'business_types': ', '.join(result.get('types', [])),
                    'emails': []  # Will be populated later
                }
        except requests.RequestException as e:
            print(f"Error getting place details: {e}")
            
        return None
    
    def extract_emails_from_website(self, website_url, max_pages=3):
        """
        Extract email addresses from a business website
        """
        if not website_url:
            return []
            
        emails = set()
        pages_to_check = [website_url]
        
        # Add common contact pages
        base_url = f"{urlparse(website_url).scheme}://{urlparse(website_url).netloc}"
        contact_pages = [
            urljoin(base_url, '/contact'),
            urljoin(base_url, '/contact-us'),
            urljoin(base_url, '/about'),
            urljoin(base_url, '/about-us')
        ]
        pages_to_check.extend(contact_pages)
        
        pages_checked = 0
        for page_url in pages_to_check[:max_pages]:
            if pages_checked >= max_pages:
                break
                
            try:
                response = self.session.get(page_url, timeout=10)
                response.raise_for_status()
                
                # Parse HTML
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Extract emails using regex
                email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
                page_text = soup.get_text()
                found_emails = re.findall(email_pattern, page_text)
                
                # Also check mailto links
                mailto_links = soup.find_all('a', href=re.compile(r'^mailto:'))
                for link in mailto_links:
                    email = link['href'].replace('mailto:', '').split('?')[0]
                    found_emails.append(email)
                
                # Filter out common non-business emails
                excluded_domains = ['example.com', 'test.com', 'gmail.com', 'yahoo.com', 'hotmail.com']
                for email in found_emails:
                    domain = email.split('@')[1].lower()
                    if domain not in excluded_domains and email.lower() not in emails:
                        emails.add(email.lower())
                
                pages_checked += 1
                time.sleep(1)  # Be respectful with requests
                
            except Exception as e:
                print(f"Error scraping {page_url}: {e}")
                continue
        
        return list(emails)
    
    def scrape_business_contacts(self, business_types, location, max_results=20):
        """
        Main method to scrape business contacts
        """
        all_businesses = []
        
        for business_type in business_types:
            print(f"Searching for {business_type} businesses in {location}...")
            businesses = self.search_businesses(business_type, location, max_results=max_results//len(business_types))
            
            for business in businesses:
                print(f"Processing: {business['name']}")
                
                # Extract emails from website
                if business['website']:
                    emails = self.extract_emails_from_website(business['website'])
                    business['emails'] = emails
                    print(f"  Found {len(emails)} email(s)")
                else:
                    print("  No website available")
                
                all_businesses.append(business)
                
                # Rate limiting
                time.sleep(2)
        
        return all_businesses
    
    def save_to_csv(self, businesses, filename='business_contacts.csv'):
        """
        Save business contacts to CSV file
        """
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['name', 'address', 'phone', 'website', 'business_types', 'emails', 'email_count']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for business in businesses:
                business_copy = business.copy()
                business_copy['emails'] = '; '.join(business['emails'])
                business_copy['email_count'] = len(business['emails'])
                writer.writerow(business_copy)
        
        print(f"Saved {len(businesses)} businesses to {filename}")

def main():
    # Configuration
    GOOGLE_API_KEY = GOOGLE_PLACES_API  # Replace with your actual API key
    LOCATION = "Caringbah, NSW"  # Change to your target location
    BUSINESS_TYPES = [
        "marketing agency",
        "law firm", 
        "accounting firm",
        "consulting firm",
        "real estate agency"
    ]
    MAX_RESULTS = 20
    
    # Initialize scraper
    scraper = BusinessContactScraper(GOOGLE_API_KEY)
    
    # Scrape business contacts
    print("Starting business contact scraping...")
    businesses = scraper.scrape_business_contacts(BUSINESS_TYPES, LOCATION, MAX_RESULTS)
    
    # Save results
    scraper.save_to_csv(businesses)
    
    # Print summary
    total_emails = sum(len(b['emails']) for b in businesses)
    businesses_with_emails = sum(1 for b in businesses if b['emails'])
    
    print(f"\n=== SCRAPING SUMMARY ===")
    print(f"Total businesses found: {len(businesses)}")
    print(f"Businesses with emails: {businesses_with_emails}")
    print(f"Total emails found: {total_emails}")
    print(f"Average emails per business: {total_emails/len(businesses):.1f}" if businesses else "Average emails per business: 0.0")

if __name__ == "__main__":
    # Required packages: requests, beautifulsoup4
    # Install with: pip install requests beautifulsoup4
    main()