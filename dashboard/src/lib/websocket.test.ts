import { describe, expect, it } from "vitest";
import { abnormalCloseMessage } from "./websocket";

describe("abnormalCloseMessage", () => {
  it("waits for repeated abnormal closes before surfacing an error", () => {
    expect(abnormalCloseMessage(1006, 4, 5, "Dashboard")).toBeNull();
    expect(abnormalCloseMessage(1006, 5, 5, "Dashboard")).toBe(
      "Dashboard connection could not be opened. Refresh with a valid dashboard token or check connectivity.",
    );
  });

  it("ignores ordinary terminal and retryable close codes", () => {
    expect(abnormalCloseMessage(1000, 5, 5, "Task output")).toBeNull();
    expect(abnormalCloseMessage(1013, 5, 5, "Task output")).toBeNull();
  });
});
