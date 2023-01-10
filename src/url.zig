// -------------------------------------------------------------------------- //
// Copyright (c) 2022, Jairus Martin.                                         //
// Distributed under the terms of the MIT  License.                           //
// The full license is in the file LICENSE, distributed with this software.   //
// -------------------------------------------------------------------------- //
const std = @import("std");
const mem = std.mem;
const Scheme = @import("backend.zig").Backend;
const testing = std.testing;


pub const Url = struct {
    scheme: Scheme,
    user: []const u8,
    pass: []const u8,
    host: []const u8,
    port: u16,
    database: []const u8 = "",



    /// Parse a database url in the following format:
    /// scheme://user:pass@host.com:port/dbname
    /// The only optional part is the :port.
    /// If the host is "localhost" it is replaced with 127.0.0.1
    /// This does _not_ check for invalid formats
    pub fn parse(url: []const u8) !Url {
        // From start to ://
        const scheme_end = mem.indexOfPos(u8, url, 0, "://")
            orelse return error.InvalidUrl;
        const s = url[0..scheme_end];
        const scheme =
        if (mem.eql(u8, s, "postgres") or mem.eql(u8, s, "postgresql"))
            Scheme.postgres
        else if (mem.eql(u8, s, "mysql") or mem.eql(u8, s, "mariadb"))
            Scheme.mysql
        else if (mem.eql(u8, s, "sqlite") or mem.eql(u8, s, "file"))
            Scheme.sqlite
        else
            return error.InvalidUrlScheme; // Must be lowercase

        // From :// to :
        const user_start = scheme_end + 3;
        const user_end = mem.indexOfPos(u8, url, user_start, ":")
            orelse return error.InvalidUrl;

        // From : to @
        const pass_start = user_end + 1;
        const pass_end = mem.indexOfPos(u8, url, pass_start, "@")
            orelse return error.InvalidUrl;

        // From @ to /
        const host_start = pass_end + 1;
        const slash_pos = mem.indexOfPos(u8, url, host_start, "/")
            orelse return error.InvalidUrl;
        const db_start = slash_pos + 1;

        // Check if a port is provided, if not use the default
        const host_end =
            if (mem.indexOfPos(u8, url[0..slash_pos], host_start, ":")) |i|
                i
            else
                slash_pos;
        const port =
        if (host_end == slash_pos)
            scheme.defaultPort()
        else
            std.fmt.parseInt(u16, url[host_end+1 .. slash_pos], 10) catch return error.InvalidUrlPort;

        const hostname = url[host_start .. host_end];
        const host = if (mem.eql(u8, hostname, "localhost"))
            "127.0.0.1"
        else
            hostname;

        return Url{
            .scheme=scheme,
            .user=url[user_start .. user_end],
            .pass=url[pass_start ..  pass_end],
            .host=host,
            .port=port,
            .database=url[db_start .. ],
        };
    }
};


test "url-postgres" {
    const db_url = "postgres://user:pass@localhost:5432/name";
    const url = try Url.parse(db_url);
    try testing.expectEqual(Scheme.postgres, url.scheme);
    try testing.expectEqualStrings("user", url.user);
    try testing.expectEqualStrings("pass", url.pass);
    try testing.expectEqualStrings("127.0.0.1", url.host);
    try testing.expectEqual(@as(u16, 5432), url.port);
    try testing.expectEqualStrings("name", url.database);
}

test "url-mysql-default-port" {
    const db_url = "mysql://user:pass@domain.com/name";
    const url = try Url.parse(db_url);
    try testing.expectEqual(Scheme.mysql, url.scheme);
    try testing.expectEqualStrings("user", url.user);
    try testing.expectEqualStrings("pass", url.pass);
    try testing.expectEqualStrings("domain.com", url.host);
    try testing.expectEqual(@as(u16, 3306), url.port);
    try testing.expectEqualStrings("name", url.database);
}
