import httpx
from fastapi import HTTPException


USER_SERVICE_URL = "http://user-service:8000"
TASK_SERVICE_URL = "http://task-service:8000"
NOTIFICATION_SERVICE_URL = "http://notification-service:8000"


class APIRequestError(Exception):
    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        self.detail = detail
        super().__init__(detail)


def api_request(
    base_url: str,
    method: str,
    path: str,
    token: str,
    payload: dict | None = None,
):
    try:
        response = httpx.request(
            method,
            f"{base_url}{path}",
            headers={"Authorization": f"Bearer {token}"},
            json=payload,
            timeout=5,
        )
    except httpx.RequestError as exc:
        raise APIRequestError(503, "A required REST service is unavailable") from exc

    if not response.is_success:
        try:
            body = response.json()
            detail = body.get("detail", "REST request failed")
        except ValueError:
            detail = "REST request failed"
        raise APIRequestError(response.status_code, str(detail)[:500])

    if not response.content:
        return None
    return response.json()


def require_bearer_token(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Bearer token required")
    token = authorization[len("Bearer "):].strip()
    if not token:
        raise HTTPException(status_code=401, detail="Bearer token required")
    return token


def get_current_user(token: str) -> dict:
    users = api_request(USER_SERVICE_URL, "GET", "/users", token)
    if not isinstance(users, list) or not users or not isinstance(users[0], dict):
        raise HTTPException(status_code=401, detail="Could not verify current user")
    user = users[0]
    if not user.get("id") or not user.get("email"):
        raise HTTPException(status_code=401, detail="Could not verify current user")
    return user


def token_subject(token: str) -> str:
    prefix = "fake-jwt-token-"
    if not token.startswith(prefix) or not token[len(prefix):]:
        raise HTTPException(status_code=401, detail="Invalid token")
    return token[len(prefix):]
