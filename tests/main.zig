// -------------------------------------------------------------------------- //
// Copyright (c) 2022, Jairus Martin.                                         //
// Distributed under the terms of the MIT  License.                           //
// The full license is in the file LICENSE, distributed with this software.   //
// -------------------------------------------------------------------------- //
const std = @import("std");
const db = @import("zq");
const Engine = db.Engine;
const Url = db.Url;
const Scheme = db.Scheme;

const log = std.log.scoped(.main);

var gpa = std.heap.GeneralPurposeAllocator(.{}){};


pub fn executeRaw(comptime buffer_size: usize, conn: anytype, query: []const u8) !void {
    var query_buffer: [buffer_size]u8 = undefined;
    var cursor = conn.execute(&query_buffer, query, .{}) catch |err| {
        if (conn.protocol.errorMessage()) |msg| {
            log.err("{s}", .{msg});
        }
        return err;
    };

    const stdout = std.io.getStdOut().writer();
    var row_buffer: [buffer_size]u8 = undefined;
    switch (cursor.status) {
        .rows => {
            const headers = try cursor.headers();
            try headers.write(.{}, stdout);

            // Read data
            // NOTE: Each call overwrites data in the buffer
            // if you don't want this pass a new buffer
            while (try cursor.rows(&row_buffer)) |row| {
                try row.write(.{}, stdout);
            }
        },
        .running => {
            try cursor.finish();
        },
        .complete => {
            log.info("Query '{s}' complete!", .{cursor.query});
        },
    }
}



pub fn initTestDatabase(comptime EngineType: type, allocator: std.mem.Allocator, url: Url) !void {
    // Create an "engine" that holds a pool of connections
    var engine = EngineType.init(allocator, .{
        .host=url.host,
        .port=url.port,
        .user=url.user,
        .pass=url.pass,
        .db="",
    });
    defer engine.deinit();

    const conn = try engine.acquire();
    defer conn.close();

    try executeRaw(4096, conn, "DROP DATABASE IF EXISTS test_zq;");
    try executeRaw(4096, conn, "CREATE DATABASE test_zq;");
    try executeRaw(4096, conn, "COMMIT;");

}

pub const User = struct {
    id: ?u32 = null, // id defaults to the pk
    username: [100:0]u8 = undefined,
    hashedpass: [std.crypto.pwhash.bcrypt.hash_length:0]u8 = undefined,
    email: ?[255:0]u8 = null,

    pub fn init(username: []const u8, password: []const u8, email: ?[]const u8) !User {
        var user = User{};
        std.mem.copy(u8, &user.username, username);
        _ = try std.crypto.pwhash.bcrypt.strHash(
            password,
            .{ .params = .{ .rounds_log = 5 }, .encoding = .crypt},
            &user.hashedpass
        );
        if (email) |v| {
            std.mem.copy(u8, &user.email.?, v);
        }
        return user;
    }

    // Table Metadata
    pub const Meta = struct {
        pub const table = "users";
        pub const username_unique = true;
        pub const email_unique = true;
    };

    pub const table = db.Table(@This(), .postgres, .{});
};



pub fn createTableCreateAndDrop(comptime EngineType: type, allocator: std.mem.Allocator, url: Url) !void {
    // Create an "engine" that holds a pool of connections
    var engine = EngineType.init(allocator, .{
        .host=url.host,
        .port=url.port,
        .user=url.user,
        .pass=url.pass,
        .db=url.database,
    });
    defer engine.deinit();

    const conn = try engine.acquire();
    defer conn.close();

    const stdout = std.io.getStdOut().writer();

    var buf: [1024]u8 = undefined;
    var cursor = try conn.executeQuery(&buf, User.table.create());
    try cursor.print(stdout);

    for ([_]User{
        try User.init("testuser", "testpass", null),
        try User.init("foo", "bar", null)
    }) |user| {
        cursor = try conn.executeQuery(&buf, User.table.insert().values(user));
        try cursor.print(stdout);
    }

    //const q = User.table.select().limit(10);//.where(.{.username="testuser");
    const q = User.table.select().limit(10);//.where(.{.username="testuser");
    cursor = try conn.executeQuery(&buf, q);
    try cursor.print(stdout);

    // cursor = try conn.executeQuery(&buf, User.table.drop());
    // try cursor.print(stdout);
}

pub fn main() !void {
    defer std.debug.assert(!gpa.deinit());
    const allocator = gpa.allocator();

    const db_url = std.os.getenv("DATABASE_URL") orelse {
        log.err("DATABASE_URL is not set. Use `export=DATABASE_URL=scheme://user:pass@localhost/dbname`", .{});
        return error.DatabaseUrlMissing;
    };
    const url = try Url.parse(db_url);
    if (std.mem.indexOf(u8, url.database, "test") == null) {
        log.err("database name of url must contain test", .{});
        return error.DatabaseUrlInvalid;
    }

    // TODO: Better way to do this?
    if (url.scheme == .postgres) {
        const EngineType = Engine(.postgres);
        try initTestDatabase(EngineType, allocator, url);
        try createTableCreateAndDrop(EngineType, allocator, url);
    } else {
        log.err("database backend is not yet implemented", .{});
        return error.NotImplemented;
    }

    //try print(conn, "SELECT * FROM codelv.product;");
    //log.info("{s}", .{cursor.result});
}
