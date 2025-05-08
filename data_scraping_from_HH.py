import requests
import pandas as pd
import time
import random
import logging
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_session():
    """Create requests session with retry strategy"""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[403, 429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def get_vacancies(session, city, vacancy, page):
    """Fetch vacancies from HH API for given city and position"""
    url = 'https://api.hh.ru/vacancies'
    params = {
        'text': f"{vacancy} {city}",
        'area': city,
        'specialization': 1,
        'per_page': 100,
        'page': page
    }
    response = session.get(url, params=params)
    response.raise_for_status()
    return response.json()

def get_vacancy_skills(session, vacancy_id):
    """Extract skills list for specific vacancy"""
    url = f'https://api.hh.ru/vacancies/{vacancy_id}'
    response = session.get(url)
    response.raise_for_status()
    data = response.json()
    skills = [skill['name'] for skill in data.get('key_skills', [])]
    return ', '.join(skills)

def get_industry(session, company_id):
    """Get industry for employer if available"""
    if company_id is None:
        return 'Unknown'
        
    url = f'https://api.hh.ru/employers/{company_id}'
    response = session.get(url)
    if response.status_code == 404:
        return 'Unknown'
    response.raise_for_status()
    data = response.json()
    return data['industries'][0].get('name') if data.get('industries') else 'Unknown'

def scrape_vacancies():
    """Main scraping function collecting up to 1000 vacancies"""
    cities = {'Moscow': 1, 'Saint Petersburg': 2}
    vacancies = [
        'Data Scientist', 'Data Engineer', 'Data Analyst', 
        'Machine Learning Engineer', 'Python Developer'
    ]
    
    all_vacancies = []
    session = create_session()

    for city, city_id in cities.items():
        for vacancy in vacancies:
            page = 0
            while len(all_vacancies) < 1000:
                try:
                    data = get_vacancies(session, city_id, vacancy, page)
                    if not data.get('items'):
                        break

                    for item in data['items']:
                        if len(all_vacancies) >= 1000:
                            break
                        if vacancy.lower() not in item['name'].lower():
                            continue
                            
                        # Collect vacancy details
                        vacancy_data = {
                            'city': city,
                            'company': item['employer']['name'],
                            'industry': get_industry(session, item['employer'].get('id')),
                            'title': item['name'],
                            'skills': get_vacancy_skills(session, item['id']),
                            'salary': item['salary'].get('from', '') if item['salary'] else 'Not specified',
                            'url': item['alternate_url']
                        }
                        all_vacancies.append(vacancy_data)
                        
                        # Progress logging
                        if len(all_vacancies) % 100 == 0:
                            logging.info(f"Collected {len(all_vacancies)} vacancies")
                        
                        time.sleep(random.uniform(2, 4))  # Rate limiting

                    page += 1
                    time.sleep(random.uniform(3, 5))  # Delay between pages
                
                except requests.HTTPError as e:
                    logging.error(f"API error: {e}")
                    time.sleep(10)
                    continue

    # Save results
    df = pd.DataFrame(all_vacancies)
    df.to_csv('vacancies.csv', index=False)
    logging.info(f"Saved {len(df)} vacancies to CSV")
    return df

if __name__ == "__main__":
    df = scrape_vacancies()
    print("\nSample data:")
    print(df.head())
