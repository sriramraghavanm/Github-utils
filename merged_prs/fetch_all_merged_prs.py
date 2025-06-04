import requests
import datetime
import sys
import os
import csv
import configparser

CONFIG_FILE = "config.properties"
CSV_DIR = r"C:\temp\github\reports"
CSV_FILE = f"all_merged_prs_{datetime.date.today()}.csv"
HEADERS = {}

def load_config():
    config = configparser.ConfigParser()
    with open(CONFIG_FILE, 'r') as f:
        config.read_string("[DEFAULT]\n" + f.read())  # allow .properties format

    token = config.get("DEFAULT", "github_token", fallback=None)
    owner = config.get("DEFAULT", "repo_owner", fallback=None)
    repo = config.get("DEFAULT", "repo_name", fallback=None)

    if not all([token, owner, repo]):
        sys.exit("❌ One or more required config fields are missing in config.properties")

    global HEADERS
    HEADERS = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json"
    }

    return owner, repo

def ensure_directory(path):
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"📁 Created directory: {path}")

def get_all_merged_pull_requests(owner, repo):
    print("🔍 Fetching all merged pull requests...")
    merged_prs = []
    page = 1
    while True:
        url = f"https://api.github.com/repos/{owner}/{repo}/pulls"
        params = {
            "state": "closed",
            "per_page": 100,
            "page": page,
            "sort": "updated",
            "direction": "desc"
        }
        response = requests.get(url, headers=HEADERS, params=params)
        if response.status_code != 200:
            sys.exit(f"❌ GitHub API error: {response.status_code} - {response.text}")
        data = response.json()
        if not data:
            break

        for pr in data:
            if pr.get("merged_at"):
                merged_prs.append(pr)

        print(f"📦 Page {page} fetched. Merged PRs so far: {len(merged_prs)}")
        page += 1

    return merged_prs

def get_merged_by_user(owner, repo, pr_number):
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}"
    response = requests.get(url, headers=HEADERS)
    if response.status_code != 200:
        print(f"⚠️ Could not fetch merged_by for PR #{pr_number}")
        return "N/A"
    data = response.json()
    merged_by = data.get("merged_by")
    return merged_by["login"] if merged_by else "N/A"

def get_approved_by(owner, repo, pr_number):
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/reviews"
    response = requests.get(url, headers=HEADERS)
    if response.status_code != 200:
        print(f"⚠️ Could not fetch reviews for PR #{pr_number}")
        return []
    reviews = response.json()
    approvers = list({review["user"]["login"] for review in reviews if review.get("state") == "APPROVED"})
    return approvers

def get_modified_files(owner, repo, pr_number):
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/files"
    response = requests.get(url, headers=HEADERS)
    if response.status_code != 200:
        print(f"⚠️ Could not fetch files for PR #{pr_number}")
        return []
    files = response.json()
    return [file["filename"] for file in files]

def write_to_csv(owner, repo, prs):
    ensure_directory(CSV_DIR)
    filepath = os.path.join(CSV_DIR, CSV_FILE)
    with open(filepath, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([
            "PR URL", "PR Number", "Title", "Author", "Merged By",
            "Merge Commit SHA", "Merged At", "Approved By", "Modified Files"
        ])

        for pr in prs:
            pr_number = pr["number"]
            approved_by = ", ".join(get_approved_by(owner, repo, pr_number))
            modified_files = ", ".join(get_modified_files(owner, repo, pr_number))
            merged_by = get_merged_by_user(owner, repo, pr_number)

            writer.writerow([
                pr["html_url"],
                pr_number,
                pr.get("title", "N/A"),
                pr.get("user", {}).get("login", "N/A"),
                merged_by,
                pr.get("merge_commit_sha", "N/A"),
                pr.get("merged_at", "N/A"),
                approved_by,
                modified_files
            ])

    print(f"✅ Report written to: {filepath}")

def main():
    owner, repo = load_config()
    merged_prs = get_all_merged_pull_requests(owner, repo)

    if not merged_prs:
        print("ℹ️ No merged pull requests found.")
    else:
        print(f"✅ Total merged pull requests: {len(merged_prs)}")
        write_to_csv(owner, repo, merged_prs)

if __name__ == "__main__":
    main()
