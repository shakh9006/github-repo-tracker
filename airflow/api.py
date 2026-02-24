import json
import logging
import time
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

BACKOFF_SECONDS = [5, 10, 20]
MAX_RETRIES = 3


def get_github_repos():
    popular_languages = [
        "python", "javascript", "java", "c", "c++", "c#", "php", "ruby",
        "swift", "kotlin", "go", "rust", "elixir", "erlang", "haskell", "sql",
    ]
    repos = []

    headers = {
        "Authorization": "Bearer ",
        "X-GitHub-Api-Version": "2022-11-28",
    }
 
    for language in popular_languages:
        url = f"https://api.github.com/search/repositories?q=language:{language}&sort=stars&order=desc&per_page=10"
        attempt = 0

        while attempt < MAX_RETRIES:
            attempt += 1
            logger.info("Attempt %s/%s for language: %s", attempt, MAX_RETRIES, language)

            response = requests.get(url, headers=headers)
            json_response = response.json()

            if response.status_code == 200:
                repos.extend(json_response["items"])
                logger.info("Success for language: %s, repos: %s", language, len(json_response["items"]))
                break

            if response.status_code == 403:
                message = json_response.get("message", "")
                if "secondary rate limit" in message:
                    if attempt >= MAX_RETRIES:
                        logger.error("Max retries reached for language: %s", language)
                        raise Exception(f"Error getting repositories for language: {language} (secondary rate limit)")
                    delay = BACKOFF_SECONDS[attempt - 1]
                    logger.warning(
                        "Secondary rate limit for language: %s. Retry reason: %s. Waiting %s seconds.",
                        language, message, delay,
                    )
                    time.sleep(delay)
                    continue

            logger.error("Error for language: %s: %s", language, json_response)
            raise Exception(f"Error getting repositories for language: {language}")

        time.sleep(1)

    return repos


if __name__ == "__main__":
    data = get_github_repos()
    out_path = "github_repos.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info("Saved %s repositories to %s", len(data), out_path)