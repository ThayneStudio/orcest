import { afterEach, describe, expect, it, vi } from "vitest";
import { dashboardTokenFromSearch } from "./authToken";

async function importAuthToken() {
  vi.resetModules();
  return import("./authToken");
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("dashboardTokenFromSearch", () => {
  it("extracts and trims dashboard tokens", () => {
    expect(dashboardTokenFromSearch("?token=dev-token")).toBe("dev-token");
    expect(dashboardTokenFromSearch("?tab=kanban&token=%20dev-token%20")).toBe("dev-token");
  });

  it("returns null for missing or blank tokens", () => {
    expect(dashboardTokenFromSearch("")).toBeNull();
    expect(dashboardTokenFromSearch("?tab=overview")).toBeNull();
    expect(dashboardTokenFromSearch("?token=%20%20")).toBeNull();
  });
});

describe("dashboardAuthToken", () => {
  it("retains the current URL token until bootstrap succeeds", async () => {
    vi.stubGlobal("window", { location: { search: "?token=first-token" } });
    const { dashboardAuthToken, addDashboardToken } = await importAuthToken();

    expect(dashboardAuthToken()).toBe("first-token");

    vi.stubGlobal("window", { location: { search: "?tab=overview" } });
    expect(dashboardAuthToken()).toBe("first-token");

    const params = addDashboardToken(new URLSearchParams({ worker_id: "worker-1" }));
    expect(params.toString()).toBe("worker_id=worker-1&token=first-token");
  });

  it("uses a newer signed URL when it is loaded in the same session", async () => {
    vi.stubGlobal("window", { location: { search: "?token=first-token" } });
    const { dashboardAuthToken } = await importAuthToken();

    expect(dashboardAuthToken()).toBe("first-token");

    vi.stubGlobal("window", { location: { search: "?token=rotated-token" } });
    expect(dashboardAuthToken()).toBe("rotated-token");

    vi.stubGlobal("window", { location: { search: "" } });
    expect(dashboardAuthToken()).toBe("rotated-token");
  });
});

describe("bootstrapDashboardAuthCookie", () => {
  it("bootstraps the HttpOnly cookie from the current URL token", async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true });
    vi.stubGlobal("window", { location: { search: "?token=dev-token" } });
    vi.stubGlobal("fetch", fetchMock);
    const { bootstrapDashboardAuthCookie } = await importAuthToken();

    await bootstrapDashboardAuthCookie();

    expect(fetchMock).toHaveBeenCalledWith("/api/auth/bootstrap?token=dev-token", {
      credentials: "same-origin",
    });
  });

  it("continues using a retained token after URL sanitization until bootstrap succeeds", async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true });
    vi.stubGlobal("window", { location: { search: "?token=dev-token" } });
    vi.stubGlobal("fetch", fetchMock);
    const { bootstrapDashboardAuthCookie, dashboardAuthToken, addDashboardToken } =
      await importAuthToken();

    expect(dashboardAuthToken()).toBe("dev-token");
    vi.stubGlobal("window", { location: { search: "" } });

    const paramsBeforeBootstrap = addDashboardToken(new URLSearchParams());
    expect(paramsBeforeBootstrap.toString()).toBe("token=dev-token");

    await bootstrapDashboardAuthCookie();

    expect(fetchMock).toHaveBeenCalledWith("/api/auth/bootstrap?token=dev-token", {
      credentials: "same-origin",
    });
    expect(addDashboardToken(new URLSearchParams()).toString()).toBe("");
  });

  it("keeps the retained token when bootstrap fails", async () => {
    const fetchMock = vi.fn().mockRejectedValue(new Error("network down"));
    vi.stubGlobal("window", { location: { search: "?token=dev-token" } });
    vi.stubGlobal("fetch", fetchMock);
    const { bootstrapDashboardAuthCookie, dashboardAuthToken } = await importAuthToken();

    expect(dashboardAuthToken()).toBe("dev-token");
    vi.stubGlobal("window", { location: { search: "" } });

    await bootstrapDashboardAuthCookie();

    expect(dashboardAuthToken()).toBe("dev-token");
  });
});
