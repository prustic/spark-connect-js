import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { parseConnectionString } from "./connection-string.js";
import { InvalidConfigError } from "@spark-connect-js/core";

describe("parseConnectionString: host and port", () => {
  it("parses sc://host:port", () => {
    const r = parseConnectionString("sc://localhost:15002");
    assert.equal(r.host, "localhost");
    assert.equal(r.port, 15002);
    assert.equal(r.useSsl, false);
  });

  it("defaults port to 15002 when omitted", () => {
    const r = parseConnectionString("sc://example.com");
    assert.equal(r.host, "example.com");
    assert.equal(r.port, 15002);
  });

  it("accepts a trailing slash with no params", () => {
    const r = parseConnectionString("sc://localhost:15002/");
    assert.equal(r.host, "localhost");
    assert.equal(r.port, 15002);
  });

  it("parses IPv6 host literals", () => {
    const r = parseConnectionString("sc://[::1]:15002");
    assert.equal(r.host, "::1");
    assert.equal(r.port, 15002);
  });

  it("parses IPv6 host with default port", () => {
    const r = parseConnectionString("sc://[2001:db8::1]");
    assert.equal(r.host, "2001:db8::1");
    assert.equal(r.port, 15002);
  });

  it("falls back to bare host:port without sc:// for backward compatibility", () => {
    const r = parseConnectionString("localhost:15002");
    assert.equal(r.host, "localhost");
    assert.equal(r.port, 15002);
  });
});

describe("parseConnectionString: reserved params", () => {
  it("parses use_ssl=true", () => {
    const r = parseConnectionString("sc://localhost:15002/;use_ssl=true");
    assert.equal(r.useSsl, true);
  });

  it("parses use_ssl=false explicitly", () => {
    const r = parseConnectionString("sc://localhost:15002/;use_ssl=false");
    assert.equal(r.useSsl, false);
  });

  it("token implicitly enables SSL", () => {
    const r = parseConnectionString("sc://example.com:15002/;token=abc123");
    assert.equal(r.token, "abc123");
    assert.equal(r.useSsl, true);
  });

  it("explicit use_ssl=true together with token is fine, in either order", () => {
    const a = parseConnectionString("sc://h:1/;token=t;use_ssl=true");
    const b = parseConnectionString("sc://h:1/;use_ssl=true;token=t");
    assert.equal(a.useSsl, true);
    assert.equal(b.useSsl, true);
    assert.equal(a.token, "t");
    assert.equal(b.token, "t");
  });

  it("decodes percent-encoded values", () => {
    const r = parseConnectionString("sc://localhost:15002/;token=abc%20def");
    assert.equal(r.token, "abc def");
  });

  it("parses user_id, user_agent, session_id together", () => {
    const sid = "550e8400-e29b-41d4-a716-446655440000";
    const r = parseConnectionString(
      `sc://localhost:15002/;user_id=alice;user_agent=myapp/1.2.3;session_id=${sid}`,
    );
    assert.equal(r.userId, "alice");
    assert.equal(r.userAgent, "myapp/1.2.3");
    assert.equal(r.sessionId, sid);
  });

  it("parses grpc_max_message_size", () => {
    const r = parseConnectionString("sc://localhost:15002/;grpc_max_message_size=67108864");
    assert.equal(r.grpcMaxMessageSize, 67108864);
  });

  it("supports multiple params separated by ;", () => {
    const r = parseConnectionString("sc://localhost:15002/;use_ssl=true;token=t;user_id=u");
    assert.equal(r.useSsl, true);
    assert.equal(r.token, "t");
    assert.equal(r.userId, "u");
  });
});

describe("parseConnectionString: non-reserved params", () => {
  it("collects unknown params as headers", () => {
    const r = parseConnectionString(
      "sc://localhost:15002/;x-databricks-cluster-id=0123-456789-abcdefgh",
    );
    assert.deepStrictEqual(r.headers, {
      "x-databricks-cluster-id": "0123-456789-abcdefgh",
    });
  });

  it("keeps reserved params out of headers", () => {
    const r = parseConnectionString("sc://localhost:15002/;token=abc;custom-header=v");
    assert.deepStrictEqual(r.headers, { "custom-header": "v" });
    assert.equal(r.token, "abc");
  });
});

describe("parseConnectionString: error cases", () => {
  it("rejects empty input", () => {
    assert.throws(() => parseConnectionString(""), InvalidConfigError);
  });

  it("rejects sc:// without host", () => {
    assert.throws(() => parseConnectionString("sc://"), InvalidConfigError);
  });

  it("rejects empty IPv6 host", () => {
    assert.throws(() => parseConnectionString("sc://[]:15002"), InvalidConfigError);
  });

  it("rejects non-numeric port", () => {
    assert.throws(() => parseConnectionString("sc://localhost:notaport"), InvalidConfigError);
  });

  it("rejects port out of range", () => {
    assert.throws(() => parseConnectionString("sc://localhost:99999"), InvalidConfigError);
  });

  it("rejects non-empty path before params", () => {
    assert.throws(
      () => parseConnectionString("sc://localhost:15002/some-path;token=abc"),
      InvalidConfigError,
    );
  });

  it("rejects invalid use_ssl values", () => {
    assert.throws(
      () => parseConnectionString("sc://localhost:15002/;use_ssl=yes"),
      InvalidConfigError,
    );
  });

  it("rejects param without =", () => {
    assert.throws(
      () => parseConnectionString("sc://localhost:15002/;flagonly"),
      InvalidConfigError,
    );
  });

  it("rejects invalid session_id (not a UUID)", () => {
    assert.throws(
      () => parseConnectionString("sc://localhost:15002/;session_id=not-a-uuid"),
      InvalidConfigError,
    );
  });

  it("rejects non-numeric grpc_max_message_size", () => {
    assert.throws(
      () => parseConnectionString("sc://localhost:15002/;grpc_max_message_size=large"),
      InvalidConfigError,
    );
  });

  it("rejects grpc_max_message_size=0", () => {
    assert.throws(
      () => parseConnectionString("sc://localhost:15002/;grpc_max_message_size=0"),
      InvalidConfigError,
    );
  });

  it("rejects token together with explicit use_ssl=false (token first)", () => {
    assert.throws(
      () => parseConnectionString("sc://h:1/;token=t;use_ssl=false"),
      InvalidConfigError,
    );
  });

  it("rejects token together with explicit use_ssl=false (use_ssl first)", () => {
    assert.throws(
      () => parseConnectionString("sc://h:1/;use_ssl=false;token=t"),
      InvalidConfigError,
    );
  });

  it("rejects http:// scheme with a clear message naming the scheme", () => {
    assert.throws(
      () => parseConnectionString("http://localhost:15002"),
      (err: unknown) => {
        if (!(err instanceof InvalidConfigError)) return false;
        assert.match(err.message, /"sc:\/\/"/);
        assert.match(err.message, /"http:\/\/"/);
        return true;
      },
    );
  });

  it("rejects https:// scheme", () => {
    assert.throws(
      () => parseConnectionString("https://example.com:443"),
      (err: unknown) => {
        if (!(err instanceof InvalidConfigError)) return false;
        assert.match(err.message, /"https:\/\/"/);
        return true;
      },
    );
  });

  it("rejects sc://user@host userinfo", () => {
    assert.throws(
      () => parseConnectionString("sc://user@host:15002"),
      (err: unknown) => {
        if (!(err instanceof InvalidConfigError)) return false;
        assert.match(err.message, /userinfo "user@"/);
        assert.match(err.message, /user_id/);
        return true;
      },
    );
  });

  it("rejects sc://user:pass@host userinfo", () => {
    assert.throws(
      () => parseConnectionString("sc://user:pass@host:15002"),
      (err: unknown) => {
        if (!(err instanceof InvalidConfigError)) return false;
        assert.match(err.message, /userinfo "user:pass@"/);
        return true;
      },
    );
  });

  it("rejects bare user@host without sc:// scheme", () => {
    assert.throws(
      () => parseConnectionString("user@host:15002"),
      (err: unknown) => {
        if (!(err instanceof InvalidConfigError)) return false;
        assert.match(err.message, /userinfo "user@"/);
        return true;
      },
    );
  });
});
