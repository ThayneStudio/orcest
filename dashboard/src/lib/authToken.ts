export function dashboardTokenFromSearch(search: string): string | null {
  const token = new URLSearchParams(search).get("token")?.trim() || "";
  return token || null;
}

let retainedDashboardToken: string | null = null;

export function resetDashboardAuthTokenForTesting(): void {
  retainedDashboardToken = null;
}

function currentDashboardToken(): string | null {
  if (typeof window === "undefined") return null;
  return dashboardTokenFromSearch(window.location.search);
}

export function dashboardAuthToken(): string | null {
  const token = currentDashboardToken();
  if (token) {
    retainedDashboardToken = token;
    return token;
  }
  return retainedDashboardToken;
}

export function addDashboardToken(params: URLSearchParams): URLSearchParams {
  const token = dashboardAuthToken();
  if (token) params.set("token", token);
  return params;
}

export function bootstrapDashboardAuthCookie(): Promise<void> {
  const token = dashboardAuthToken();
  if (!token || typeof fetch === "undefined") return Promise.resolve();

  return fetch(`/api/auth/bootstrap?token=${encodeURIComponent(token)}`, {
    credentials: "same-origin",
  }).then((response) => {
    if (response.ok && retainedDashboardToken === token) {
      retainedDashboardToken = null;
    }
  }).catch(() => undefined);
}
