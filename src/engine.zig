// -------------------------------------------------------------------------- //
// Copyright (c) 2022, Jairus Martin.                                         //
// Distributed under the terms of the MIT License.                            //
// The full license is in the file LICENSE, distributed with this software.   //
// -------------------------------------------------------------------------- //
const std = @import("std");
const Address = std.net.Address;
const Allocator = std.mem.Allocator;

const Backend = @import("backend.zig").Backend;
const ObjectPool = @import("util.zig").ObjectPool;

pub const Options = struct {
    host: []const u8,
    port: u16,
    user: []const u8,
    pass: []const u8,
    db: []const u8 = "",
};

pub fn Engine(comptime backend: Backend) type {
    return struct {
        const Self = @This();
        pub const Connection = backend.Connection();
        pub const ConnectionPool = ObjectPool(Connection);

        allocator: Allocator,
        connection_pool: ConnectionPool,
        options: Options,

        pub fn init(allocator: Allocator, options: Options) Self {
            return Self{
                .allocator = allocator,
                .connection_pool = ConnectionPool.init(allocator),
                .options = options,
            };
        }

        // Grab a connection from the pool, creating a new one if needed
        pub fn acquire(self: *Self) !*Connection {
            const addr = try Address.parseIp4(self.options.host, self.options.port);

            var conn: ?*Connection = null;
            const lock = self.connection_pool.acquire();
            defer lock.release();

            // If an error occurs while setting up the connection release it
            errdefer if (conn) |c| {
                self.connection_pool.release(c);
            };

            if (self.connection_pool.get()) |c| {
                conn = c;
                if (c.state == .termination) {
                    c.protocol.stream = try std.net.tcpConnectToAddress(addr);
                }
            } else {
                conn = try self.connection_pool.create();
                const stream = try std.net.tcpConnectToAddress(addr);
                errdefer stream.close();
                conn.?.* = try Connection.init(.{
                    .stream = stream,
                    .user = self.options.user,
                    .pass = self.options.pass,
                    .db = self.options.db,
                });
            }
            return conn.?;
        }

        // Return a connection back into the pool
        pub fn release(self: *Self, conn: *Connection) void {
            const lock = self.connection_pool.acquire();
            defer lock.release();
            self.connection_pool.release(conn);
        }

        pub fn deinit(self: *Self) void {
            const lock = self.connection_pool.acquire();
            defer lock.release();
            var n: usize = 0;
            for (self.connection_pool.objects.items) |conn| {
                if (conn.state == .termination) continue;
                conn.close();
                n += 1;
            }
            for (self.connection_pool.free_objects.items) |conn| {
                if (conn.state == .termination) continue;
                conn.close();
                n += 1;
            }
            // log.info("Closed {d} connections.", .{n});
            self.connection_pool.deinit();
        }
    };
}

comptime {
    std.testing.refAllDecls(@This());
}
