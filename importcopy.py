import csv
import subprocess
import concurrent.futures
import json
import pandas as pd
import time
from urllib.parse import urlparse
import logging
from datetime import datetime
import os
from typing import List, Dict, Any, Optional
import backoff
from ratelimit import limits, sleep_and_retry

# Configuration
CONFIG = {
    'API_URL': 'https://api-lr.agent.ai/api/company/lite',
    'SERVER_URL': 'http://51.12.241.183:80',
    'AUTH': {
        'username': 'ricarda',
        'password': '4712YYu'
    },
    'HEADERS': {
        'accept': '*/*',
        'accept-language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/json',
        'origin': 'https://agent.ai',
        'priority': 'u=1, i',
        'referer': 'https://agent.ai/',
        'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
    },
    'MAX_WORKERS': 1000,
    'MAX_RETRIES': 3
}

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=f'api_calls_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
)

class TokenManager:
    def __init__(self):
        self.token = None
        self.last_refresh = None
        self.refresh_interval = 3600  # 1 hour

    def get_token(self) -> str:
        """Retrieve the authentication token.

        This function checks if the current token is valid based on its
        existence and the time elapsed since the last refresh. If the token is
        missing or has expired, it calls the `refresh_token` method to obtain a
        new token. Finally, it returns the current token.

        Returns:
            str: The current authentication token.
        """

        if (not self.token or 
            not self.last_refresh or 
            time.time() - self.last_refresh > self.refresh_interval):
            self.refresh_token()
        return self.token

    def refresh_token(self) -> None:
        """Refresh the authentication token.

        This method sends a request to the server to refresh the authentication
        token. It constructs a curl command with the necessary headers and data,
        executes it, and updates the instance's token with the new access token
        received in the response. If the token refresh is successful, it also
        updates the last refresh time.
        """

        try:
            command = [
                'curl', '--location', f'{CONFIG["SERVER_URL"]}/login',
                '--header', 'Content-Type: application/x-www-form-urlencoded',
                '--data-urlencode', f'username={CONFIG["AUTH"]["username"]}',
                '--data-urlencode', f'password={CONFIG["AUTH"]["password"]}'
            ]
            
            result = subprocess.run(command, capture_output=True, text=True, check=True)
            response = json.loads(result.stdout)
            self.token = response.get('access_token')
            self.last_refresh = time.time()
            logging.info("Token refreshed successfully")
        except Exception as e:
            logging.error(f"Error refreshing token: {str(e)}")
            raise

class Report:
    def __init__(self):
        self.stats = {
            "success_count": 0,
            "error_count": 0,
            "start_time": datetime.now(),
            "last_save_time": None,
            "processed_domains": set(),
            "failed_domains": set(),
            "retry_count": {}
        }
    
    def update(self, success: bool, domain: Optional[str] = None, retries: int = 0) -> None:
        """Update the statistics based on the success of an operation.

        This method updates the internal statistics of the object, incrementing
        the success or error counts based on the outcome of an operation. If the
        operation was successful, it increments the success count and adds the
        domain to the set of processed domains if provided. If the operation
        failed, it increments the error count and adds the domain to the set of
        failed domains if provided. Additionally, if there are retries, it
        updates the retry count for the specified domain.

        Args:
            success (bool): Indicates whether the operation was successful.
            domain (Optional[str]): The domain associated with the operation, if any.
            retries (int): The number of retries attempted for the operation.

        Returns:
            None: This method does not return any value.
        """

        if success:
            self.stats["success_count"] += 1
            if domain:
                self.stats["processed_domains"].add(domain)
        else:
            self.stats["error_count"] += 1
            if domain:
                self.stats["failed_domains"].add(domain)
        
        if retries > 0:
            self.stats["retry_count"][domain] = retries

def clean_domain(url: str) -> tuple[str, str]:
    """Extract and clean domain from a given URL.

    This function takes a URL as input, ensures it has the correct scheme
    (http or https), and then parses the URL to extract the domain. If the
    domain starts with 'www.', it removes that prefix. The function returns
    both the cleaned domain and the full URL.

    Args:
        url (str): The URL from which to extract the domain.

    Returns:
        tuple[str, str]: A tuple containing the cleaned domain (in lowercase)
        and the full URL.
    """
    try:
        if not url.startswith(('http://', 'https://')):
            full_url = 'https://' + url
        else:
            full_url = url
        
        parsed = urlparse(full_url)
        domain = parsed.netloc
        
        if domain.startswith('www.'):
            domain = domain[4:]
            
        return domain.lower().strip(), full_url
    except Exception as e:
        logging.error(f"Error cleaning domain {url}: {str(e)}")
        return url, url

@sleep_and_retry
@limits(calls=1000, period=60)
@backoff.on_exception(
    backoff.expo,
    (subprocess.CalledProcessError, json.JSONDecodeError),
    max_tries=CONFIG['MAX_RETRIES']
)
def call_api(website: str, report: Report) -> Optional[Dict[str, Any]]:
    """Call an external API with the specified website and report.

    This function constructs a command to call an external API using the
    `curl` command. It prepares the necessary data, executes the command,
    and processes the response. If the response is valid, it enriches the
    response data with additional information such as the original URL and
    clean domain. The function also handles various error scenarios, logging
    errors and updating the provided report object accordingly.

    Args:
        website (str): The website URL to be processed.
        report (Report): An object used to log the success or failure of the API call.

    Returns:
        Optional[Dict[str, Any]]: A dictionary containing the API response data if successful,
        or None if there was an error.
    """

    try:
        clean_website, full_url = clean_domain(website)
        data = {
            "domain": clean_website,
            "report_component": "harmonic_funding_and_web_traffic",
            "user_id": None
        }

        command = [
            'curl', CONFIG['API_URL'],
            '-X', 'POST',
            *sum((['-H', f'{k}: {v}'] for k, v in CONFIG['HEADERS'].items()), []),
            '--data-raw', json.dumps(data)
        ]
        
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        
        try:
            # Log raw response for debugging
            logging.debug(f"Raw API response for {website}: {result.stdout}")
            
            if not result.stdout.strip():
                logging.error(f"Empty response for {website}")
                report.update(success=False, domain=clean_website)
                return None
            
            try:
                response_data = json.loads(result.stdout)
            except json.JSONDecodeError as e:
                logging.error(f"Failed to parse JSON for {website}: {str(e)}")
                report.update(success=False, domain=clean_website)
                return None
            
            # Verify response is a dictionary
            if not isinstance(response_data, dict):
                logging.error(f"Invalid response type for {website}: {type(response_data)}")
                report.update(success=False, domain=clean_website)
                return None
            
            # Add URL information
            response_data['original_url'] = website
            response_data['clean_domain'] = clean_website
            response_data['full_url'] = full_url
            
            logging.info(f"Successfully processed {website}")
            report.update(success=True, domain=clean_website)
            return response_data
            
        except Exception as e:
            logging.error(f"Error processing response for {website}: {str(e)}")
            report.update(success=False, domain=clean_website)
            return None

    except Exception as e:
        logging.error(f"Error calling API for {website}: {str(e)}")
        report.update(success=False, domain=clean_website)
        return None

def map_company_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Map agent.ai data to server format.

    This function takes a dictionary containing company data from agent.ai
    and maps it to a specific server format. It validates the input data,
    ensuring that the necessary fields are present and of the correct type.
    The function extracts relevant information such as company name,
    location, contact details, and social media links, returning a new
    dictionary with the mapped data. If any errors occur during processing,
    the function logs the error and returns a default dictionary with empty
    fields.

    Args:
        data (Dict[str, Any]): A dictionary containing company data from agent.ai.

    Returns:
        Dict[str, Any]: A dictionary containing the mapped company data in the
        specified server format.

    Raises:
        TypeError: If the input data is not of type dict.
    """
    try:
        # Validate input
        if not isinstance(data, dict):
            logging.error(f"Invalid data type in map_company_data: {type(data)}")
            raise TypeError(f"Expected dict, got {type(data)}")
        
        company_data = data.get('company_data', {})
        if not isinstance(company_data, dict):
            logging.error(f"Invalid company_data type: {type(company_data)}")
            company_data = {}
            
        company = company_data.get('company', {})
        if not isinstance(company, dict):
            logging.error(f"Invalid company type: {type(company)}")
            company = {}
        
        # Get domain information with validation
        clean_domain = str(data.get('clean_domain', ''))
        full_url = str(data.get('full_url', ''))
        
        # Safely get nested values
        location = company.get('location', {}) if isinstance(company.get('location'), dict) else {}
        site = company.get('site', {}) if isinstance(company.get('site'), dict) else {}
        linkedin = company.get('linkedin', {}) if isinstance(company.get('linkedin'), dict) else {}
        facebook = company.get('facebook', {}) if isinstance(company.get('facebook'), dict) else {}
        twitter = company.get('twitter', {}) if isinstance(company.get('twitter'), dict) else {}
        metrics = company.get('metrics', {}) if isinstance(company.get('metrics'), dict) else {}
        category = company.get('category', {}) if isinstance(company.get('category'), dict) else {}
        identifiers = company.get('identifiers', {}) if isinstance(company.get('identifiers'), dict) else {}
        
        # Safely get arrays
        email_addresses = site.get('emailAddresses', []) if isinstance(site.get('emailAddresses'), list) else []
        phone_numbers = site.get('phoneNumbers', []) if isinstance(site.get('phoneNumbers'), list) else []
        tags = company.get('tags', []) if isinstance(company.get('tags'), list) else []
        
        return {
            'company_name': str(company.get('name', '')),
            'firstCompanyName': '',
            'street_NO': str(location.get('street', '')),
            'domain': clean_domain,
            'city': str(location.get('city', '')),
            'email': str(email_addresses[0]) if email_addresses else '',
            'linkedin': str(linkedin.get('handle', '')),
            'logo': str(company.get('logo', '')),
            'founded_on': str(company.get('foundedYear', '')),
            'sourcefound': 'agent.ai',
            'zip': str(location.get('postalCode', '')),
            'category': str(category.get('industry', '')),
            'slogan': str(company.get('description', '')),
            'pressphoto': '',
            'tags': ','.join(str(tag) for tag in tags),
            'ceo': '',
            'ceoid': '',
            'news': '',
            'awards': '',
            'futurepredictions': '',
            'financials': json.dumps(metrics),
            'Company_Short': str(company.get('name', ''))[:50] if company.get('name') else '',
            'phone': str(phone_numbers[0]) if phone_numbers else '',
            'Rechtsform': str(company.get('type', '')),
            'cat-tag-1-trustedshops': '',
            'cat-tag-2-trustedshops': '',
            'private-gov': '',
            'Description': str(company.get('description', '')),
            'link_agb': '',
            'link_daten': '',
            'tag_cat_linkedin': str(linkedin.get('industry', '')),
            'linkedinurl': str(linkedin.get('handle', '')),
            'facebookurl': str(facebook.get('handle', '')),
            'instagramurl': '',
            'Twitter': str(twitter.get('handle', '')),
            'TAX-ID': str(identifiers.get('usEIN', '')),
            'country': str(location.get('country', ''))
        }
    except Exception as e:
        logging.error(f"Error mapping company data: {str(e)}")
        # Return empty data with required fields
        return {
            'company_name': '',
            'firstCompanyName': '',
            'street_NO': '',
            'domain': str(data.get('clean_domain', '')) if isinstance(data, dict) else '',
            'city': '',
            'email': '',
            'linkedin': '',
            'logo': '',
            'founded_on': '',
            'sourcefound': 'agent.ai',
            'zip': '',
            'category': '',
            'slogan': '',
            'pressphoto': '',
            'tags': '',
            'ceo': '',
            'ceoid': '',
            'news': '',
            'awards': '',
            'futurepredictions': '',
            'financials': '{}',
            'Company_Short': '',
            'phone': '',
            'Rechtsform': '',
            'cat-tag-1-trustedshops': '',
            'cat-tag-2-trustedshops': '',
            'private-gov': '',
            'Description': '',
            'link_agb': '',
            'link_daten': '',
            'tag_cat_linkedin': '',
            'linkedinurl': '',
            'facebookurl': '',
            'instagramurl': '',
            'Twitter': '',
            'TAX-ID': '',
            'country': ''
        }

@backoff.on_exception(
    backoff.expo,
    Exception,
    max_tries=CONFIG['MAX_RETRIES']
)
def send_to_server(data: Dict[str, Any], token_manager: TokenManager) -> bool:
    """Send data to the server with authentication.

    This function constructs a curl command to send data to a specified
    server endpoint. It retrieves an authentication token from the provided
    TokenManager and includes it in the request headers. The data is sent as
    URL-encoded form data. If the request is successful, it logs a success
    message; otherwise, it logs an error message.

    Args:
        data (Dict[str, Any]): A dictionary containing the data to be sent to the server.
        token_manager (TokenManager): An instance of TokenManager used to retrieve the authentication token.

    Returns:
        bool: True if the data was successfully sent to the server, False otherwise.
    """
    try:
        token = token_manager.get_token()
        
        command = [
            'curl', '--location', f'{CONFIG["SERVER_URL"]}/crawler/institution',
            '--header', 'Content-Type: application/x-www-form-urlencoded',
            '--header', f'Authorization: {token}',
            *sum((['--data-urlencode', f'{k}={v}'] for k, v in data.items()), [])
        ]
        
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        logging.info(f"Successfully sent data to server for company: {data.get('company_name')}")
        return True

    except Exception as e:
        logging.error(f"Error sending data to server: {str(e)}")
        return False

def process_website(website: str, report: Report, token_manager: TokenManager) -> None:
    """Process a single website and send data to server.

    This function takes a website URL and a report object, calls an API to
    retrieve data related to the website, maps the retrieved data to a
    specific format, and sends the mapped data to a server using the
    provided token manager. If any error occurs during this process, it logs
    an error message indicating the failure.

    Args:
        website (str): The URL of the website to process.
        report (Report): The report object containing relevant information.
        token_manager (TokenManager): The token manager used for authentication.
    """
    try:
        result = call_api(website, report)
        if result:
            mapped_data = map_company_data(result)
            send_to_server(mapped_data, token_manager)
    except Exception as e:
        logging.error(f"Error in process_website for {website}: {str(e)}")

def process_websites(websites: List[str], token_manager: TokenManager) -> None:
    """Process websites with concurrent execution.

    This function takes a list of website URLs and processes each website
    concurrently using a thread pool. It utilizes a `TokenManager` to manage
    tokens during the processing of each website. A report is generated
    during the processing, which can be used to track the status and results
    of each website processed.

    Args:
        websites (List[str]): A list of website URLs to be processed.
        token_manager (TokenManager): An instance of TokenManager to manage tokens
            during the website processing.

    Returns:
        None: This function does not return any value.
    """
    report = Report()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONFIG['MAX_WORKERS']) as executor:
        futures = [
            executor.submit(process_website, website, report, token_manager)
            for website in websites
        ]
        concurrent.futures.wait(futures)

def main() -> None:
    """Run the main processing workflow for website data.

    This function orchestrates the main logic of the application, which
    includes fetching an authentication token, reading website URLs from a
    CSV file in chunks, and processing these URLs in batches. It logs the
    progress and execution time of the entire operation. The function
    handles any exceptions that may occur during the process and logs an
    error message before re-raising the exception.
    """

    try:
        token_manager = TokenManager()
        # Initial token fetch to ensure we can connect to the server
        token_manager.get_token()
        
        start_time = time.time()
        chunk_size = 5000
        
        for chunk_index, chunk in enumerate(pd.read_csv('urls.csv', chunksize=chunk_size, encoding='utf-8')):
            logging.info(f"Processing chunk {chunk_index + 1}")
            websites = chunk.iloc[:, 0].tolist()
            
            # Process websites in larger batches
            batch_size = 500
            for i in range(0, len(websites), batch_size):
                batch = websites[i:i + batch_size]
                logging.info(f"Processing batch {i//batch_size + 1} of chunk {chunk_index + 1}")
                process_websites(batch, token_manager)
        
        execution_time = time.time() - start_time
        logging.info(f"Processing completed in {execution_time:.2f} seconds")
        
    except Exception as e:
        logging.error(f"Main process error: {str(e)}")
        raise

if __name__ == '__main__':
    main()