// -------------------------------------------------------------------------- //
// Copyright (c) 2022, Jairus Martin.                                         //
// Distributed under the terms of the MIT License.                            //
// The full license is in the file LICENSE, distributed with this software.   //
// -------------------------------------------------------------------------- //
const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;

const HeaderRow = @import("row.zig").HeaderRow;
const Header = @import("row.zig").Header;
const Row = @import("row.zig").Row;
const DataValue = @import("row.zig").DataValue;

const Backend = @import("backend.zig").Backend;

const log = std.log.scoped(.cursor);

pub const HeaderList = std.ArrayList(Header);
pub const ColumnList = std.ArrayList(DataValue);

pub const CursorStatus = enum {
    complete,
    running,
    rows,
};

pub fn Cursor(comptime backend: Backend) type {
    return struct {
        pub const Protocol = backend.Protocol();
        pub const Message = backend.Message();
        const Self = @This();
        protocol: *Protocol,

        // Original query
        query: []const u8,
        status: CursorStatus,

        // Internal storage for the intial row description
        internal_allocator: std.heap.FixedBufferAllocator,
        header_list: ?HeaderList = null,
        column_list: ?ColumnList = null,

        // Message from first read
        initial_msg: Message,

        // Message from last read
        last_msg: Message,

        pub fn numFields(self: Self) usize {
            return if (self.header_list) |h| h.items.len else 0;
        }

        pub fn lastObjectId(self: Self) ?i32 {
            return self.protocol.object_id;
        }

        // Row count from last query, this is not updated until the query is
        // complete (eg all rows returned)
        pub fn lastRowCount(self: Self) usize {
            return self.protocol.rows;
        }

        /// Return slice remaining in the internal buffer
        pub fn internalBuffer(self: Self) []u8 {
            const end = self.internal_allocator.end_index;
            return self.internal_allocator.buffer[end..];
        }

        /// Load the initial row into the internal buffer
        /// *WARNING* this uses space int the internal allocator
        pub fn headers(self: *Self) !HeaderRow {
            // Parse initial row description
            return try self.protocol.handleRowDesc(self, self.initial_msg);
        }

        // Load a row into the buffer provided
        pub fn rows(self: *Self, buf: []u8) !?Row {
            assert(self.header_list != null and self.column_list != null); // You need to call headers first
            const columns = self.column_list.?.items;
            // The backend can send stuff in the middle of queries
            while (try self.process(buf)) |msg| {
                if (self.status == .rows) {
                    return try self.protocol.handleRowData(self, msg, columns);
                }
            }
            return null;
        }

        // Load a row using the given allocator. It will load up to max_size bytes.
        // *WARNING* It's caller's responsibility to deinit the message.
        pub fn rowsAlloc(self: *Self, allocator: Allocator, max_size: usize) !?Row {
            assert(self.header_list != null and self.column_list != null); // You need to call headers first
            const columns = self.column_list.?.items;
            // The backend can send stuff in the middle of queries
            while (try self.processAlloc(allocator, max_size)) |msg| {
                if (self.status == .rows) {
                    return try self.parseRowData(msg, columns);
                }
                msg.deinit();
            }
            return null;
        }

        // Read one message into the provided buffer
        pub fn process(self: *Self, buf: []u8) !?Message {
            self.status = .running;
            self.last_msg = try self.protocol.readMessage(buf);
            return self.protocol.handleMessage(self, self.last_msg);
        }

        pub fn processAlloc(self: *Self, allocator: Allocator, max_size: usize) !?Message {
            self.status = .running;
            self.last_msg = try self.protocol.readMessageAlloc(allocator, max_size);
            return self.protocol.handleMessage(self, self.last_msg);
        }

        pub fn finish(self: *Self) !void {
            // Read until ready
            const buf = self.internalBuffer();
            while (try self.process(buf)) |_| {
                // Do nothing
            }
        }

        pub fn print(self: *Self, out_stream: anytype) !void {
            switch (self.status) {
                .rows => {
                    const h = try self.headers();
                    try h.write(.{}, out_stream);
                    // Note: The call to headers uses the buffer
                    // so buf must be made after
                    const buf = self.internalBuffer();
                    while (try self.rows(buf)) |row| {
                        try row.write(.{}, out_stream);
                    }
                },
                .running => {
                    const buf = self.internalBuffer();
                    while (try self.process(buf)) |msg| {
                        switch (msg.code) {
                            'C' => try out_stream.print("Complete {s}\n", .{msg.data}),
                            else => try out_stream.print("{s}\n", .{msg}),
                        }
                    }
                },
                .complete => {},
            }
        }
    };
}
