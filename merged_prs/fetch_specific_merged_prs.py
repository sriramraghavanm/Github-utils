import requests
import datetime
import sys
import os
import csv
import configparser
import fnmatch

CONFIG_FILE = "config.properties"
CSV_DIR = r"C:\temp\github\reports"
CSV_FILE = f"merged_prs_{datetime.date.today()}.csv"
HEADERS = {}
GRAPHQL_URL = "https://api.github.com/graphql"


def load_config():
    config = configparser.ConfigParser()
    with open(CONFIG_FILE, 'r') as f:
        config.read_string("[DEFAULT]\n" + f.read())

    token = config.get("DEFAULT", "github_token", fallback=None)
    owner = config.get("DEFAULT", "repo_owner", fallback=None)
    repo = config.get("DEFAULT", "repo_name", fallback=None)
    included_paths = config.get("DEFAULT", "included_paths", fallback="").split(',')
    start_date = config.get("DEFAULT", "start_date", fallback=None)
    end_date = config.get("DEFAULT", "end_date", fallback=None)

    if not all([token, owner, repo, start_date, end_date]):
        sys.exit("❌ One or more required config fields are missing in config.properties")

    global HEADERS
    HEADERS = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    return owner, repo, included_paths, start_date, end_date


def ensure_directory(path):
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"📁 Created directory: {path}")


def matches_included_paths(filename, patterns):
    return any(fnmatch.fnmatch(filename, pattern) for pattern in patterns)


def fetch_merged_prs_graphql(owner, repo, start_date, end_date, included_paths):
    print("🔍 Fetching merged pull requests using GraphQL API...")
    merged_prs = []
    has_next_page = True
    cursor = None

    while has_next_page:
        after_clause = f', after: "{cursor}"' if cursor else ''

        query = {
            "query": f"""
            query {{
              repository(name: "{repo}", owner: "{owner}") {{
                pullRequests(first: 50{after_clause}, states: MERGED, orderBy: {{field: UPDATED_AT, direction: DESC}}) {{
                  pageInfo {{
                    hasNextPage
                    endCursor
                  }}
                  nodes {{
                    number
                    title
                    url
                    author {{ login }}
                    mergedAt
                    mergedBy {{ login }}
                    mergeCommit {{ oid }}
                    files(first: 100) {{
                      nodes {{ path }}
                    }}
                    reviews(first: 50, states: APPROVED) {{
                      nodes {{
                        author {{ login }}
                      }}
                    }}
                  }}
                }}
              }}
            }}
            """
        }

        response = requests.post(GRAPHQL_URL, headers=HEADERS, json=query)

        try:
            data = response.json()
        except ValueError:
            print("❌ Failed to parse JSON. Raw response:")
            print(response.text)
            sys.exit(1)

        if "errors" in data:
            print("❌ GraphQL returned errors:")
            for error in data["errors"]:
                print(f"- {error.get('message')}")
            sys.exit(1)

        if "data" not in data or not data["data"].get("repository"):
            print("❌ Unexpected GraphQL response structure:")
            print(data)
            sys.exit(1)

        pr_data = data["data"]["repository"]["pullRequests"]
        has_next_page = pr_data["pageInfo"]["hasNextPage"]
        cursor = pr_data["pageInfo"]["endCursor"] if has_next_page else None

        older_pr_count = 0
        for pr in pr_data["nodes"]:
            merged_at = pr.get("mergedAt")
            if not merged_at:
                continue

            merged_at_date = merged_at[:10]

            if merged_at_date < start_date:
                older_pr_count += 1
                continue

            if merged_at_date > end_date:
                continue  # skip future PRs (if any)

            files = [f["path"] for f in pr["files"]["nodes"]]
            if not any(matches_included_paths(f, included_paths) for f in files):
                continue

            approvers = list({review["author"]["login"] for review in pr["reviews"]["nodes"] if review["author"]})

            merged_prs.append({
                "url": pr["url"],
                "number": pr["number"],
                "title": pr["title"],
                "author": pr["author"]["login"] if pr["author"] else "N/A",
                "merged_by": pr["mergedBy"]["login"] if pr["mergedBy"] else "N/A",
                "merge_commit_sha": pr["mergeCommit"]["oid"] if pr["mergeCommit"] else "N/A",
                "merged_at": merged_at,
                "approved_by": ", ".join(approvers),
                "files": ", ".join(files)
            })

        if older_pr_count == len(pr_data["nodes"]):
            print("⏹ All PRs on this page are older than START_DATE. Stopping early.")
            has_next_page = False

        print(f"📦 Total PRs collected so far: {len(merged_prs)}")

    return merged_prs


def write_to_csv(prs):
    ensure_directory(CSV_DIR)
    filepath = os.path.join(CSV_DIR, CSV_FILE)
    with open(filepath, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([
            "PR URL", "PR Number", "Title", "Author", "Merged By",
            "Merge Commit SHA", "Merged At", "Approved By", "Modified Files"
        ])
        for pr in prs:
            writer.writerow([
                pr["url"],
                pr["number"],
                pr["title"],
                pr["author"],
                pr["merged_by"],
                pr["merge_commit_sha"],
                pr["merged_at"],
                pr["approved_by"],
                pr["files"]
            ])
    print(f"✅ Report written to: {filepath}")


def main():
    owner, repo, included_paths, start_date, end_date = load_config()
    merged_prs = fetch_merged_prs_graphql(owner, repo, start_date, end_date, included_paths)
    if not merged_prs:
        print("ℹ️ No matching merged pull requests found in the given date range and paths.")
    else:
        print(f"✅ Total filtered merged PRs: {len(merged_prs)}")
        write_to_csv(merged_prs)


if __name__ == "__main__":
    main()
