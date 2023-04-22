import pandas as pd
from datetime import datetime

def transform_nyt_json(data:dict) -> pd.DataFrame:
        fields_of_interest = ["id",
                              "published_date",
                              "section", "adx_keywords",
                              "byline", 
                              "type",
                              "title",
                              "abstract"
        ]
        filtered_results = [
            {key: result[key] for key in fields_of_interest} for result in data["results"]
        ]
        df = pd.DataFrame(filtered_results)
        df['timestamp_api_call'] = int(datetime.now().strftime('%Y%m%d%H%M%S'))
        return df