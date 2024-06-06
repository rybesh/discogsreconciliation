"""
An OpenRefine reconciliation service for the Discogs API.

This code is adapted from Michael Stephens:
https://github.com/mikejs/reconcile-demo
"""

from flask import Flask, request, jsonify, make_response
from flask_cors import CORS
import json
from operator import itemgetter
import urllib.parse
from fuzzywuzzy import fuzz
import requests
import time

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Discogs API credentials
personal_token = 'YOUR_DISCOGS_API_TOKEN'

# Discogs API base URL
api_base_url = 'https://api.discogs.com/database/search'
discogs_base_url = 'https://api.discogs.com/'

# Custom User-Agent string
USER_AGENT = "YourAppName/1.0 +http://yourwebsite.com"

# Track the time of the last request and rate limits
last_request_time = 0
rate_limit_remaining = 60  # Initial assumption, will be updated based on headers
rate_limit_reset_time = 60  # Initial assumption, will be updated based on headers

# Service metadata
metadata = {
    "name": "Discogs Reconciliation Service",
    "identifierSpace": "http://www.discogs.com/",
    "schemaSpace": "http://www.schema.org/",
    "defaultTypes": [
        {"id": "/discogs/artist", "name": "Artist"},
        {"id": "/discogs/release", "name": "Release"},
        {"id": "/discogs/master", "name": "Master"},
        {"id": "/discogs/label", "name": "Label"},
        {"id": "/discogs/track", "name": "Track"},
        {"id": "/discogs/genre", "name": "Genre"},
        {"id": "/discogs/style", "name": "Style"},
        {"id": "/discogs/country", "name": "Country"},
        {"id": "/discogs/year", "name": "Year"},
        {"id": "/discogs/format", "name": "Format"},
        {"id": "/discogs/catno", "name": "Catalog Number"},
        {"id": "/discogs/barcode", "name": "Barcode"},
        {"id": "/discogs/submitter", "name": "Submitter"},
        {"id": "/discogs/contributor", "name": "Contributor"}
    ],
    "view": {
        "url": "{{id}}"
    },
    "preview": {
        "url": "{{id}}/preview",
        "width": 400,
        "height": 300
    }
}

def make_uri(entity_type, discogs_id):
    """
    Prepare a Discogs url from the ID returned by the API.
    """
    return f"https://www.discogs.com/{entity_type}/{discogs_id}"

def jsonpify(obj):
    """
    Helper to support JSONP
    """
    try:
        callback = request.args['callback']
        response = app.make_response(f"{callback}({json.dumps(obj)})")
        response.mimetype = "text/javascript"
        return response
    except KeyError:
        return jsonify(obj)

def rate_limited_request(url, headers):
    global last_request_time, rate_limit_remaining, rate_limit_reset_time

    current_time = time.time()
    elapsed_time = current_time - last_request_time

    if rate_limit_remaining <= 0:
        app.logger.debug(f"Rate limit exceeded, sleeping for {rate_limit_reset_time:.2f} seconds")
        time.sleep(rate_limit_reset_time)
        rate_limit_remaining = 60  # Reset remaining requests count after sleeping

    if elapsed_time < 1:
        sleep_time = 1 - elapsed_time
        app.logger.debug(f"Sleeping for {sleep_time:.2f} seconds to respect rate limit")
        time.sleep(sleep_time)

    response = requests.get(url, headers=headers)
    last_request_time = time.time()

    if response.status_code == 200:
        rate_limit_remaining = int(response.headers.get('X-Discogs-Ratelimit-Remaining', 60))
        rate_limit_reset_time = int(response.headers.get('X-Discogs-Ratelimit-Reset', 60))
        app.logger.debug(f"Rate limit remaining: {rate_limit_remaining}, reset in: {rate_limit_reset_time} seconds")
    elif response.status_code == 429:
        rate_limit_reset_time = int(response.headers.get('Retry-After', 60))
        app.logger.warning(f"Rate limit exceeded, sleeping for {rate_limit_reset_time:.2f} seconds")
        time.sleep(rate_limit_reset_time)
        response = rate_limited_request(url, headers)
    
    return response

def search(query, query_type='/discogs/artist'):
    """
    Hit the Discogs API for names.
    """
    out = []
    query_type_meta = next((item for item in metadata["defaultTypes"] if item['id'] == query_type), None)
    entity_type = query_type_meta["id"].split('/')[-1] if query_type_meta else 'artist'

    try:
        # Discogs API URL
        url = f"{api_base_url}?q={urllib.parse.quote(query)}&type={entity_type}&token={personal_token}"
        app.logger.debug("Discogs API url is " + url)
        headers = {
            'Authorization': f'Discogs token={personal_token}',
            'User-Agent': USER_AGENT
        }
        resp = rate_limited_request(url, headers)
        results = resp.json()
        app.logger.debug("Discogs API response: " + json.dumps(results, indent=2))
    except Exception as e:
        app.logger.warning(e)
        return out

    for item in results.get('results', []):
        match = False
        name = item.get('title') if entity_type != 'artist' else item.get('name')
        discogs_id = item.get('id')
        discogs_uri = make_uri(entity_type, discogs_id)
        catno = item.get('catno', 'N/A')
        score = fuzz.token_sort_ratio(query, name) if name else 0
        if name and query.lower() == name.lower():
            match = True

        resource = {
            "id": discogs_uri,
            "name": name or 'Unknown',
            "score": score,
            "match": match,
            "type": [{"id": query_type_meta["id"], "name": query_type_meta["name"]}],
            "catno": catno
        }
        out.append(resource)

    # Sort this list by score
    sorted_out = sorted(out, key=itemgetter('score'), reverse=True)
    # Refine only will handle top three matches.
    return sorted_out[:3]

@app.route("/reconcile", methods=['POST', 'GET'])
def reconcile():
    # Single queries have been deprecated. This can be removed.
    # Look first for form-param requests.
    query = request.form.get('query')
    if query is None:
        # Then normal get params.
        query = request.args.get('query')
        query_type = request.args.get('type', '/discogs/artist')
    if query:
        # If the 'query' param starts with a "{" then it is a JSON object
        # with the search string as the 'query' member. Otherwise,
        # the 'query' param is the search string itself.
        if query.startswith("{"):
            query = json.loads(query)['query']
        results = search(query, query_type=query_type)
        return jsonpify({"result": results})
    # If a 'queries' parameter is supplied then it is a dictionary
    # of (key, query) pairs representing a batch of queries. We
    # should return a dictionary of (key, results) pairs.
    queries = request.form.get('queries')
    if queries:
        queries = json.loads(queries)
        results = {}
        for (key, query) in queries.items():
            qtype = query.get('type', '/discogs/artist')
            data = search(query['query'], query_type=qtype)
            results[key] = {"result": data}
        return jsonpify(results)
    # If neither a 'query' nor 'queries' parameter is supplied then
    # we should return the service metadata.
    return jsonpify(metadata)

@app.route("/<entity_type>/<discogs_id>/preview", methods=['GET'])
def preview(entity_type, discogs_id):
    """
    Fetch detailed information for the preview window.
    """
    try:
        url = f"{discogs_base_url}{entity_type}s/{discogs_id}"
        headers = {
            'Authorization': f'Discogs token={personal_token}',
            'User-Agent': USER_AGENT
        }
        resp = rate_limited_request(url, headers)
        details = resp.json()
        app.logger.debug("Discogs details response: " + json.dumps(details, indent=2))

        label_info = details.get('labels', [])
        label_names = ', '.join([label['name'] for label in label_info])
        catno = label_info[0]['catno'] if label_info and 'catno' in label_info[0] else 'N/A'
        artist_names = ', '.join([artist['name'] for artist in details.get('artists', [])])

        preview_html = f"""
        <html>
        <body>
           <h1>{details.get('title', 'No Title')}</h1>
           	<p><strong>Artist:</strong> {artist_names}</p>
           	<p><strong>Label:</strong> {label_names}</p>
            <p><strong>Catalog Number:</strong> {catno}</p>
            <p><strong>Year:</strong> {details.get('year', 'Unknown')}</p>
            <p><strong>Genres:</strong> {', '.join(details.get('genres', []))}</p>
            <p><strong>Styles:</strong> {', '.join(details.get('styles', []))}</p
        
        </body>
        </html>
        """
        return make_response(preview_html, 200)

    except Exception as e:
        app.logger.warning(e)
        return make_response(f"Error fetching preview: {str(e)}", 500)

if __name__ == '__main__':
    from optparse import OptionParser
    oparser = OptionParser()
    oparser.add_option('-d', '--debug', action='store_true', default=False)
    opts, args = oparser.parse_args()
    app.debug = opts.debug
    app.run(host='0.0.0.0', port=3000)