import pandas as pd


def transform_nyt_json(data:dict) -> pd.DataFrame:
        fields_of_interest = ["id", "published_date", "section", "adx_keywords", "byline", "type","title", "abstract"]
        filtered_results = [{key: result[key] for key in fields_of_interest} for result in data["results"]]
        return pd.DataFrame(filtered_results)