import os, time, pathlib
import requests as rq
from requests.exceptions import RequestException, ConnectionError
import polars as pl
from dotenv import load_dotenv
import tenacity

@tenacity.retry(
    retry=tenacity.retry_if_exception_type(RequestException),
    wait=tenacity.wait_exponential(multiplier=5, min=4, max=16),
    stop=tenacity.stop_after_attempt(5),
    reraise=True
)
def request(issn, from_date, to_date, page=1, per_page=200, mail_to="you@example.com"):
    results = []
    referenced_works = []
    cited_by_links = []
    while True:
        r = rq.get(f"https://api.openalex.org/works?page={page}&per-page={per_page}&filter=primary_location.source.issn:{issn},from_publication_date:{from_date},to_publication_date:{to_date}&sort=publication_year&mailto={mail_to}")
        r.raise_for_status()
        results.extend(r.json()["results"])
        if page*per_page >= r.json()["meta"]["count"]:
            break
        page += 1

    works = [
                {
                    "id": work["id"].split("/")[-1], 
                    "doi": work["doi"], 
                    "openalex_url": work["id"], 
                    "title": work["title"], 
                    "publication_date": work["publication_date"], 
                    "publication_year": work["publication_year"], 
                    "volume": work["biblio"]["volume"], 
                    "issue": work["biblio"]["issue"], 
                    "type": work["type"], 
                    "citation_count": work["cited_by_count"], 
                    "reference_count": work["referenced_works_count"]
                } 
                for work in results
            ]
    
    works_df = pl.DataFrame(works)
    works_df = works_df.unique().sort(["publication_year", "volume", "issue"])
    works_df.write_csv("data/works.csv")
    
    authors =   [
                    {
                        "id": authorship["author"]["id"].split("/")[-1],
                        "name": authorship["author"]["display_name"],
                        "affiliation": authorship["raw_affiliation_string"]
                    } 
                    for work in results 
                    for authorship in work["authorships"]
                ]
    
    authors_df = pl.DataFrame(authors)
    authors_df = authors_df.unique().sort(["name", "id"])
    authors_df.write_csv("data/authors.csv")

    works_authors = [
                        {
                            "work_id": work["id"].split("/")[-1],
                            "author_id": authorship["author"]["id"].split("/")[-1],
                            "position": authorship["author_position"],
                        } 
                        for work in results 
                        for authorship in work["authorships"]
                    ]
    
    works_authors_df = pl.DataFrame(works_authors)
    works_authors_df = works_authors_df.unique().sort(["work_id", "author_id"])
    works_authors_df.write_csv("data/works_authors.csv")
    
    institutions =  [
                        {
                            "id": institution["id"].split("/")[-1],
                            "name": institution["display_name"],
                            "country_code": institution["country_code"],
                            "type": institution["type"],
                        } 
                        for work in results 
                        for authorship in work["authorships"] 
                        for institution in authorship["institutions"]
                    ]
    
    institutions_df = pl.DataFrame(institutions)
    institutions_df = institutions_df.unique().sort(["name", "id"])
    institutions_df.write_csv("data/institutions.csv")
    
    works_authors_institutions = [
                                    {
                                        "work_id": work["id"].split("/")[-1],
                                        "author_id": authorship["author"]["id"].split("/")[-1],
                                        "institution_id": institution["id"].split("/")[-1],
                                    } 
                                    for work in results 
                                    for authorship in work["authorships"] 
                                    for institution in authorship["institutions"]
                                ]
    
    works_authors_institutions_df = pl.DataFrame(works_authors_institutions)
    works_authors_institutions_df = works_authors_institutions_df.unique().sort(["work_id", "author_id", "institution_id"])
    works_authors_institutions_df.write_csv("data/works_authors_institutions.csv")

def main():
    load_dotenv()
    env = os.environ
    if not (env.get("ISSN") and env.get("FROM") and env.get("TO")):
        raise Exception("Environment variables not loaded correctly.")
    
    pathlib.Path("./data").mkdir(exist_ok=True)
    request(env.get("ISSN", ""), env.get("FROM", "2000-01-01"), env.get("TO", "2023-12-31"), mail_to=env.get("EMAIL"))
    
if __name__=="__main__":
    start_time = time.time()
    main()
    print(f"{'-'*10}Extraction complete{'-'*10}")
    print(f"Elapsed time: {time.time()-start_time}")