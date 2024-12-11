# API Data Processing System Documentation

## Table of Contents

1. [Introduction](#introduction)
2. [Configuration](#configuration)
3. [Logging](#logging)
4. [Token Management (`TokenManager` class)](#token-management-tokenmanager-class)
5. [Report Management (`Report` class)](#report-management-report-class)
6. [Domain Cleaning (`clean_domain` function)](#domain-cleaning-clean_domain-function)
7. [API Interaction (`call_api` function)](#api-interaction-call_api-function)
8. [Data Mapping (`map_company_data` function)](#data-mapping-map_company_data-function)
9. [Server Communication (`send_to_server` function)](#server-communication-send_to_server-function)
10. [Website Processing (`process_website`, `process_websites` functions)](#website-processing-process_website-process_websites-functions)
11. [Main Execution (`main` function)](#main-execution-main-function)


<a name="introduction"></a>
## 1. Introduction

This document details the internal workings of the API data processing system.  The system retrieves website data from `agent.ai` API, transforms it, and sends it to a designated server.  It utilizes concurrency for efficient processing and incorporates retry mechanisms and rate limiting to handle potential errors and API restrictions.


<a name="configuration"></a>
## 2. Configuration

The system's configuration is stored in the `CONFIG` dictionary.  This includes API URLs, server URLs, authentication credentials, HTTP headers, concurrency limits, and retry limits.

| Key             | Value                                                                           | Description                                                              |
|-----------------|-----------------------------------------------------------------------------------|--------------------------------------------------------------------------|
| `API_URL`       | `https://api-lr.agent.ai/api/company/lite`                                      | URL for the `agent.ai` API.                                             |
| `SERVER_URL`    | `http://51.12.241.183:80`                                                         | URL for the destination server.                                          |
| `AUTH`          | Dictionary containing username and password                                      | Authentication credentials for the server.                               |
| `HEADERS`       | Dictionary containing HTTP headers for API requests                               | HTTP headers to be included in API requests.                           |
| `MAX_WORKERS`   | `1000`                                                                          | Maximum number of concurrent workers for website processing.            |
| `MAX_RETRIES`   | `3`                                                                             | Maximum number of retries for API calls and server communication.       |


<a name="logging"></a>
## 3. Logging

The system uses the `logging` module to record events. Log files are named `api_calls_YYYYMMDD_HHMMSS.log`, where YYYYMMDD_HHMMSS represents the timestamp of creation.  The log level is set to `INFO`.


<a name="token-management-tokenmanager-class"></a>
## 4. Token Management (`TokenManager` class)

The `TokenManager` class manages authentication tokens for the server.

*   **`get_token()`:** This method retrieves a valid token. It checks if the existing token is valid (not expired). If not, it calls `refresh_token()` to get a new one.
*   **`refresh_token()`:**  This method uses `curl` via `subprocess` to make a POST request to `/login` on the server URL specified in `CONFIG`, using the username and password from the `CONFIG` file. The response (containing the `access_token`) is parsed using `json.loads()`. If successful, the `token` and `last_refresh` attributes are updated, and a success message is logged.  Errors during the refresh process are caught and logged.


<a name="report-management-report-class"></a>
## 5. Report Management (`Report` class)

The `Report` class maintains statistics on the processing.

*   **`update()`:** This method updates the report statistics based on the success or failure of an operation. It increments success/error counts, adds domains to processed/failed sets, and updates retry counts.


<a name="domain-cleaning-clean_domain-function"></a>
## 6. Domain Cleaning (`clean_domain` function)

The `clean_domain` function cleans and extracts the domain from a given URL.

1.  Ensures the URL starts with `http://` or `https://`.
2.  Parses the URL using `urllib.parse.urlparse`.
3.  Removes `www.` prefix if present.
4.  Returns the lowercase cleaned domain and the original URL.
5.  Handles exceptions during parsing and logs errors.


<a name="api-interaction-call_api-function"></a>
## 7. API Interaction (`call_api` function)

The `call_api` function interacts with the `agent.ai` API.

1.  Cleans the domain using `clean_domain`.
2.  Constructs a `curl` command to make a POST request to the `API_URL` with the cleaned domain, report component, and user ID.
3.  Executes the `curl` command using `subprocess.run`.
4.  Parses the JSON response using `json.loads`.
5.  Adds `original_url`, `clean_domain`, and `full_url` to the response data.
6.  Logs the raw response for debugging.
7.  Handles various exceptions (empty responses, JSON parsing errors, invalid response types) and updates the `report`.
8.  Uses `@backoff.on_exception`, `@sleep_and_retry`, and `@limits` decorators for retrying failed API calls with exponential backoff and rate limiting.


<a name="data-mapping-map_company_data-function"></a>
## 8. Data Mapping (`map_company_data` function)

The `map_company_data` function transforms the data from the `agent.ai` API response to the format expected by the server.  It handles potential missing keys and invalid types gracefully by using `get()` method with default values and type checking. It logs errors if data is of incorrect type or if necessary keys are missing.


<a name="server-communication-send_to_server-function"></a>
## 9. Server Communication (`send_to_server` function)

The `send_to_server` function sends data to the server.

1.  Retrieves a token from `token_manager`.
2.  Constructs a `curl` command to make a POST request to `/crawler/institution` endpoint.
3.  Includes the token in the Authorization header.
4.  Sends data as URL-encoded form data.
5.  Logs success or error messages.
6. Uses `@backoff.on_exception` decorator for retrying failed server communication with exponential backoff.


<a name="website-processing-process_website-process_websites-functions"></a>
## 10. Website Processing (`process_website`, `process_websites` functions)

*   **`process_website()`:** This function processes a single website. It calls `call_api`, maps the data using `map_company_data`, and sends the data to the server using `send_to_server`. It logs errors that occur during the process.
*   **`process_websites()`:** This function processes a list of websites concurrently using `concurrent.futures.ThreadPoolExecutor`. It creates a `Report` object to track the processing status and uses the provided `token_manager` to manage tokens during concurrent website processing.


<a name="main-execution-main-function"></a>
## 11. Main Execution (`main` function)

The `main` function orchestrates the entire process.

1.  Initializes `TokenManager`.
2.  Reads websites from `urls.csv` in chunks using `pandas`.
3.  Processes each chunk in batches of 500 websites using `process_websites`.
4.  Logs the processing progress and total execution time.
5.  Handles exceptions and logs errors.

The websites are read from `urls.csv` in chunks to manage memory efficiently and processed in batches to optimize API calls and server communication.  The entire process is monitored and logged.
