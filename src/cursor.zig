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

        // Read headers from the initial response and populate the header and column lists.
        pub fn parseRowDescription(self: *Self, allocator: Allocator, msg: Message) !HeaderRow {
            assert(msg.code == 'T');
            var fbo = std.io.fixedBufferStream(msg.data);
            const reader = fbo.reader();
            const num_fields = try reader.readIntBig(i16);
            // Should this be an error?
            if (num_fields <= 0) {
                return error.PostgresColumnLengthMismatch;
            }

            // Initialize list
            const n = std.math.absCast(num_fields);
            self.header_list = try HeaderList.initCapacity(allocator, n);
            self.column_list = try ColumnList.initCapacity(allocator, n);
            self.header_list.?.expandToCapacity();
            self.column_list.?.expandToCapacity();

            // Parse message
            var i: usize = 0;
            const parsed_headers = self.header_list.?.items;
            while (i < n) : (i += 1) {
                if (std.mem.indexOfScalarPos(u8, msg.data, fbo.pos, 0)) |end| {
                    const name = msg.data[fbo.pos .. end + 1];
                    try fbo.seekTo(end + 1);
                    parsed_headers[i] = Header{
                        .name = name,
                        .table_id = try reader.readIntBig(i32),
                        .attribute_number = try reader.readIntBig(i16),
                        .data_type = try reader.readIntBig(i32),
                        .type_len = try reader.readIntBig(i16),
                        .type_mod = try reader.readIntBig(i32),
                        .format_code = try reader.readIntBig(i16),
                    };
                    // std.log.warn("{s}", .{parsed_headers[i]});
                }
            }
            return HeaderRow{ .headers = parsed_headers };
        }

        // Parse a RowData message into the given columns list
        pub fn parseRowData(self: *Self, msg: Message, columns: []DataValue) !?Row {
            assert(msg.code == 'D');
            var fbo = std.io.fixedBufferStream(msg.data);
            const reader = fbo.reader();
            const num_cols = std.math.absCast(try reader.readIntBig(i16));
            if (num_cols != columns.len) {
                return error.PostgresColumnLengthMismatch;
            }

            // Unpack values
            const parsed_headers = self.header_list.?.items;
            for (parsed_headers) |h, i| {
                const l = try reader.readIntBig(i32);

                // TODO: Support all this switching seems like a waste
                if (l == -1) {
                    columns[i] = switch (h.data_type) {
                        16 => DataValue{ .optional_bool = null },
                        18 => DataValue{ .optional_u8 = null },
                        19 => DataValue{ .optional_str = null },
                        20 => DataValue{ .optional_i64 = null },
                        21 => DataValue{ .optional_i16 = null },
                        23 => DataValue{ .optional_i32 = null },
                        25, 26, 1034 => DataValue{ .optional_str = null },
                        28 => DataValue{ .optional_u64 = null },
                        else => {
                            std.log.err("Unsupported data type: {d}", .{h.data_type});
                            return error.PostgresInvalidDataType;
                        },
                    };
                } else if (l == 0) {
                    columns[i] = DataValue{ .optional_str = "" };
                } else {
                    const end = fbo.pos + std.math.absCast(l);
                    const value = msg.data[fbo.pos..end];
                    //std.log.debug("{s}", .{value});
                    columns[i] = switch (h.data_type) {
                        16 => DataValue{ .bool = value[0] == 't' },
                        18 => DataValue{ .u8 = value[0] },
                        19 => DataValue{ .str = value },
                        20 => DataValue{ .i64 = try std.fmt.parseInt(i64, value, 10) },
                        21 => DataValue{ .i16 = try std.fmt.parseInt(i16, value, 10) },
                        23 => DataValue{ .i32 = try std.fmt.parseInt(i32, value, 10) },
                        25, 26, 1034 => DataValue{ .str = value },
                        28 => DataValue{ .u64 = try std.fmt.parseInt(u64, value, 10) },
                        else => {
                            std.log.err("Unsupported data type: {d}", .{h.data_type});
                            return error.PostgresInvalidDataType;
                        },
                    };

                    try fbo.seekTo(end);
                }
            }
            // NOTE: Even though this is returning from a variable on the stack
            // the copied
            return Row{ .headers = parsed_headers, .data = columns };
        }

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

        // Load the initial row into the given buffer
        pub fn headers(self: *Self) !HeaderRow {
            // Parse initial row description
            assert(self.initial_msg.code == 'T');
            const allocator = self.internal_allocator.allocator();
            return try self.parseRowDescription(allocator, self.initial_msg);
        }

        // Load a row into the buffer iterator
        pub fn rows(self: *Self, buf: []u8) !?Row {
            assert(self.header_list != null and self.column_list != null); // You need to call headers first
            const columns = self.column_list.?.items;
            // The backend can send stuff in the middle of queries
            while (try self.process(buf)) |msg| {
                if (self.status == .rows) {
                    return try self.parseRowData(msg, columns);
                }
            }
            return null;
        }

        // Read one message
        pub fn process(self: *Self, buf: []u8) !?Message {
            self.status = .running;
            self.last_msg = try self.protocol.readMessage(buf);
            switch (self.last_msg.code) {
                'D' => {
                    self.status = .rows;
                }, // Row data
                'C' => try self.protocol.handleCommandComplete(self.last_msg),
                'I' => try self.protocol.handleEmptyQueryResponse(self.last_msg),
                'Z' => {
                    self.status = .complete;
                    return null;
                }, // Complete / ready for next query
                'N' => try self.protocol.handleNoticeResponse(self.last_msg),
                'E' => try self.protocol.handleErrorResponse(self.last_msg), // Error see msg data
                else => return error.DatabaseErrorResponse,
            }
            return self.last_msg;
        }

        pub fn finish(self: *Self) !void {
            // Read until ready
            const end = self.internal_allocator.end_index;
            const remaining = self.internal_allocator.buffer[end..];
            while (try self.process(remaining)) |_| {
                // Do nothing
            }
        }

        pub fn print(self: *Self, out_stream: anytype) !void {
            const end = self.internal_allocator.end_index;
            const remaining = self.internal_allocator.buffer[end..];
            switch (self.status) {
                .rows => {
                    const h = try self.headers();
                    try h.write(.{}, out_stream);
                    while (try self.rows(remaining)) |row| {
                        try row.write(.{}, out_stream);
                    }
                },
                .running => {
                    while (try self.process(remaining)) |msg| {
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
