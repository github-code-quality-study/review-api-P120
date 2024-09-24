import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.corpus import stopwords
from urllib.parse import parse_qs, urlparse
import json
import pandas as pd
from datetime import datetime
import uuid
import os
from typing import Callable, Any
from wsgiref.simple_server import make_server

nltk.download('vader_lexicon', quiet=True)
nltk.download('punkt', quiet=True)
nltk.download('averaged_perceptron_tagger', quiet=True)
nltk.download('stopwords', quiet=True)

adj_noun_pairs_count = {}
sia = SentimentIntensityAnalyzer()
stop_words = set(stopwords.words('english'))

reviews = pd.read_csv('data/reviews.csv').to_dict('records')

TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S'

# Create a set of acceptable valid locations from existing reviews
valid_locations = set()
for r in reviews:
    valid_locations.add(r['Location'])


class ReviewAnalyzerServer:
    def __init__(self) -> None:
        # This method is a placeholder for future initialization logic
        pass

    def analyze_sentiment(self, review_body):
        sentiment_scores = sia.polarity_scores(review_body)
        return sentiment_scores

    def __call__(self, environ: dict[str, Any], start_response: Callable[..., Any]) -> bytes:
        """
        The environ parameter is a dictionary containing some useful
        HTTP request information such as: REQUEST_METHOD, CONTENT_LENGTH, QUERY_STRING,
        PATH_INFO, CONTENT_TYPE, etc.
        """

        if environ["REQUEST_METHOD"] == "GET":
            # Create the response body from the reviews and convert to a JSON byte string
            response_body = json.dumps(reviews, indent=2).encode("utf-8")
            
            # Write your code here
            for r in reviews:
                r['sentiment'] = self.analyze_sentiment(r["ReviewBody"])
            filtered_reviews = sorted(reviews, key=lambda x: x['sentiment']['compound'], reverse=True)
            
            # Extract GET parameters
            location= parse_qs(environ['QUERY_STRING']).get('location',None)
            start_date= parse_qs(environ['QUERY_STRING']).get('start_date',None)
            end_date= parse_qs(environ['QUERY_STRING']).get('end_date',None)

            # Filter reviews
            if location:
                location = location[0]
                filtered_reviews = [r for r in filtered_reviews if r["Location"]==location]
            
            if start_date:
                start_date = start_date[0]
                filtered_reviews = [r for r in filtered_reviews if datetime.strptime(r['Timestamp'], TIMESTAMP_FORMAT)>=datetime.fromisoformat(start_date)]
            
            if end_date:
                end_date = end_date[0]
                filtered_reviews = [r for r in filtered_reviews if datetime.strptime(r['Timestamp'], TIMESTAMP_FORMAT)<=datetime.fromisoformat(end_date)]


            response_body = json.dumps(filtered_reviews, indent=2).encode("utf-8")
            # Set the appropriate response headers
            start_response("200 OK", [
            ("Content-Type", "application/json"),
            ("Content-Length", str(len(response_body)))
             ])
            
            return [response_body]


        if environ["REQUEST_METHOD"] == "POST":
            # Write your code here
            request_body_len = int(environ['CONTENT_LENGTH'])
            request_body = environ['wsgi.input'].read(request_body_len)

            location =  parse_qs(request_body.decode()).get("Location", None)
            review_body =  parse_qs(request_body.decode()).get("ReviewBody", None) 

            if location:
                location = location[0]
                if location not in valid_locations:
                    start_response('400 Bad Request', [
                        ('Content-type', 'text/plain')
                        ])
                    return [b"Invalid Location"]
            else:
                start_response('400 Bad Request', [
                        ('Content-type', 'text/plain')
                        ])
                return [b"Missing Location"]
            
            if review_body:
                review_body = review_body[0]
            else:
                start_response('400 Bad Request', [
                        ('Content-type', 'text/plain')
                        ])
                return [b"Missing Review Body"]
            
            review_id = str(uuid.uuid4())
            
            timestamp = datetime.now().strftime(TIMESTAMP_FORMAT)

            review = {
                "ReviewId": review_id,
                "Timestamp": timestamp,
                "ReviewBody": review_body,
                "Location": location
            }

            reviews.append(review)

            response_body = json.dumps(review, indent=2).encode("utf-8")

            start_response("201 OK", [
            ("Content-Type", "application/json"),
            ("Content-Length", str(len(response_body)))
             ])
            
            return [response_body]

if __name__ == "__main__":
    app = ReviewAnalyzerServer()
    port = os.environ.get('PORT', 8000)
    with make_server("", port, app) as httpd:
        print(f"Listening on port {port}...")
        httpd.serve_forever()