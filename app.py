from flask import Flask, request, jsonify
import json
import logging
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast
from dataclasses import dataclass
import requests

app = Flask(__name__)

@dataclass(frozen=True, kw_only=True)
class Settings:
    endpoint: str
    api_version: str
    subscription_key: str | None = None
    aad_token: str | None = None
    analyzer_id: str
    file_location: str

    def __post_init__(self):
        if not self.subscription_key and not self.aad_token:
            raise ValueError("Either 'subscription_key' or 'aad_token' must be provided")

    @property
    def token_provider(self) -> Callable[[], str] | None:
        return (lambda: self.aad_token) if self.aad_token else None

class AzureContentUnderstandingClient:
    def __init__(self, endpoint: str, api_version: str, subscription_key: str | None = None,
                 token_provider: Callable[[], str] | None = None, x_ms_useragent: str = "cu-sample-code") -> None:
        if not subscription_key and token_provider is None:
            raise ValueError("Either subscription key or token provider must be provided")
        self._endpoint = endpoint.rstrip("/")
        self._api_version = api_version
        self._headers = self._get_headers(subscription_key, token_provider() if token_provider else None, x_ms_useragent)

    def begin_analyze(self, analyzer_id: str, file_location: str):
        if Path(file_location).exists():
            with open(file_location, "rb") as file:
                data = file.read()
            headers = {"Content-Type": "application/octet-stream"}
        elif file_location.startswith("http"):
            data = {"url": file_location}
            headers = {"Content-Type": "application/json"}
        else:
            raise ValueError("File location must be a valid path or URL.")
        headers.update(self._headers)
        response = requests.post(
            url=self._get_analyze_url(analyzer_id),
            headers=headers,
            json=data if isinstance(data, dict) else None,
            data=None if isinstance(data, dict) else data
        )
        response.raise_for_status()
        return response

    def poll_result(self, response: requests.Response, timeout_seconds: int = 120, polling_interval_seconds: int = 2) -> dict[str, Any]:
        operation_location = response.headers.get("operation-location", "")
        if not operation_location:
            raise ValueError("Operation location not found in response headers.")
        start_time = time.time()
        while True:
            if time.time() - start_time > timeout_seconds:
                raise TimeoutError("Operation timed out.")
            poll_response = requests.get(operation_location, headers=self._headers)
            poll_response.raise_for_status()
            result = poll_response.json()
            status = result.get("status", "").lower()
            if status == "succeeded":
                return result
            elif status == "failed":
                raise RuntimeError("Request failed.")
            time.sleep(polling_interval_seconds)

    def _get_analyze_url(self, analyzer_id: str) -> str:
        return f"{self._endpoint}/contentunderstanding/analyzers/{analyzer_id}:analyze?api-version={self._api_version}"

    def _get_headers(self, subscription_key: str | None, api_token: str | None, x_ms_useragent: str) -> dict[str, str]:
        headers = {"Ocp-Apim-Subscription-Key": subscription_key} if subscription_key else {"Authorization": f"Bearer {api_token}"}
        headers["x-ms-useragent"] = x_ms_useragent
        return headers

@app.route('/analyze', methods=['POST'])
def analyze():
    data = request.json
    file_url = data.get('file_url')
    analyzer_id = data.get('analyzer_id')

    settings = Settings(
        endpoint="https://ai-admin5049ai587084069384.services.ai.azure.com/",
        api_version="2024-12-01-preview",
        subscription_key="4MytYG6tDJp5p3WbMmWXmGm22j3NyKzgSCMvKrHCj3UEzwn9KPD6JQQJ99BEACfhMk5XJ3w3AAAAACOGTnyc",
        analyzer_id=analyzer_id,
        file_location=file_url
    )

    client = AzureContentUnderstandingClient(
        endpoint=settings.endpoint,
        api_version=settings.api_version,
        subscription_key=settings.subscription_key,
        token_provider=settings.token_provider
    )

    response = client.begin_analyze(settings.analyzer_id, settings.file_location)
    result = client.poll_result(response, timeout_seconds=3600, polling_interval_seconds=1)
    return jsonify(result)

if __name__ == '__main__':
    app.run(debug=True)
