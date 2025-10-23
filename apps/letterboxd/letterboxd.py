import csv
from apps.app.meilisearch_client import client
from apps.app.models import Movie

def parse_user_watched(file):
    index = client.index("movies")
    csv_as_dicts = csv.DictReader(file.read().decode('utf8').splitlines())
    result = {'found': {}, 'not_found': []}

    # collect ids from Meili hits (we'll try to detect what field contains the id)
    found_ids = []
    found_imdb_ids = []

    def run_search(query, opts):
        """Run search and safely return hits list; print response for debugging."""
        try:
            res = index.search(query, opts)
        except Exception as e:
            print(f"[meilisearch] search error for query={query!r} opts={opts!r}: {e}")
            return []
        # Inspect structure: try common keys
        hits = res.get("hits") or res.get("results") or res.get("hits")  # defensive
        #print(f"[meilisearch] query={query!r} opts={opts!r} -> hits_count={len(hits) if hits else 0}")
        # Print a short summary of top hits for debugging
        #for i, h in enumerate((hits or [])[:3]):
        #    print(f"  hit[{i}]: keys={list(h.keys())} snippet_title={h.get('title') or h.get('original_title')}")
        return hits or []

    for row in csv_as_dicts:
        title = row['Name'].strip()
        year = row['Year'].strip()
        #print(f"\nSearching for: title={title!r}, year={year!r}")

        # Try 1: filter by year (best case) — requires year to be filterable & present in documents
        opts = {"limit": 3}
        if year.isdigit():
            opts["filter"] = f'year >= "{year}-01-01" AND year <= "{year}-12-31"'
        hits = run_search(title, opts)

        # Try fallback(s) if no hits
        if not hits:
            # Try without any filter
        #    print(" No hits with year filter — trying without filter")
            hits = run_search(title, {"limit": 3})

        if not hits and year.isdigit():
            # Try searching for "title year" as the query
            combined = f"{title} {year}"
        #    print(" Still no hits — trying combined title+year query")
            hits = run_search(combined, {"limit": 3})

        # If still nothing, append to not_found and continue
        if not hits:
        #    print(f" No Meili hits for {title} ({year}) — marking not_found")
            result['not_found'].append({"query": {"title": title, "year": year}})
            continue

        # We have hits: pick top hit (or optionally inspect top 3)
        top = hits[0]

        # Determine id field(s) present in hit
        # Common possibilities: 'id', '_id', 'imdb_id'
        if "id" in top:
            found_ids.append(top["id"])
        #    print(f" Collected id (field 'id') = {top['id']}")
        if "_id" in top:
            found_ids.append(top["_id"])
        #    print(f" Collected id (field '_id') = {top['_id']}")
        if "imdb_id" in top:
            found_imdb_ids.append(top["imdb_id"])
        #    print(f" Collected imdb_id = {top['imdb_id']}")

        # As an additional heuristic, if the hit has no id-like field but has 'imdb_id' inside nested data,
        # adjust extraction here as needed.

    # Deduplicate ids
    found_ids = list(dict.fromkeys(found_ids))
    found_imdb_ids = list(dict.fromkeys(found_imdb_ids))
    #print(f"\nFinal collected ids (pk candidates): {found_ids}")
    #print(f"Final collected imdb_ids: {found_imdb_ids}")

    # Prefer querying by pk if we have IDs, otherwise try imdb_id
    if found_ids:
        matches = Movie.objects(pk__in=found_ids).only(
            'guessed_country', 'imdb_id', 'id', 'original_title',
            'release_date', 'poster_path', 'vote_average', 'vote_count'
        )
    elif found_imdb_ids:
        matches = Movie.objects(imdb_id__in=found_imdb_ids).only(
            'guessed_country', 'imdb_id', 'id', 'original_title',
            'release_date', 'poster_path', 'vote_average', 'vote_count'
        )
    else:
        matches = []

    # Build 'found' structure grouped by guessed_country
    for match in matches:
        country = match.guessed_country or "unknown"
        result['found'].setdefault(country, []).append({
            'imdb_id': match.imdb_id,
            'id': str(match.id),
            'original_title': match.original_title,
            'release_date': match.release_date,
            'poster_path': match.poster_path,
            'vote_average': match.vote_average,
            'vote_count': match.vote_count,
            'country_code': country
        })

    # If we collected ids but matches is empty, log that — likely id mismatch between Meili and Mongo
    #if (found_ids or found_imdb_ids) and not matches:
    #    print("WARNING: We collected ids from Meilisearch, but Mongo query returned no documents. Possible causes:")
    #    print(" - Meili 'id' values do not match Mongo pk values (string vs ObjectId vs different field).")
    #    print(" - You need to query by 'imdb_id' instead of pk, or convert types (ObjectId).")
    #    print(" - The movies in Meili may not exist in this Mongo DB instance or in the same collection.")

    return result