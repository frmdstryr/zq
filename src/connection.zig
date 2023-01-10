// -------------------------------------------------------------------------- //
// Copyright (c) 2022, Jairus Martin.                                         //
// Distributed under the terms of the MIT License.                            //
// The full license is in the file LICENSE, distributed with this software.   //
// -------------------------------------------------------------------------- //
const std = @import("std");
const Backend = @import("backend.zig").Backend;
const CursorStatus = @import("cursor.zig").CursorStatus;
const log = std.log.scoped(.connection);

pub const Option = struct {
    name: []const u8,
    value: []const u8,
};

pub const Params = struct {
    stream: std.net.Stream,
    user: []const u8,
    pass: []const u8,
    db: []const u8,
    options: ?[]Option = null,
    replication: []const u8 = "false",
};

pub fn Connection(comptime backend: Backend) type {
    return struct {
        const Self = @This();
        pub const Protocol = backend.Protocol();
        pub const Cursor = backend.Cursor();
        pub const State = enum { startup, query, function, copy, termination };
        protocol: Protocol,
        state: State = .query,
        echo: bool = false,

        pub fn init(params: Params) !Self {
            return Self{
                .protocol = try Protocol.init(params),
            };
        }

        // Execute a query and store the initial result in the given buffer
        // The buffer must have enough space to hold the message and a list of
        // column headers
        pub fn execute(self: *Self, buffer: []u8, query: []const u8, params: anytype) !Cursor {
            // TODO: Encode query
            if (self.echo) {
                log.debug("{s}", .{query});
            }
            _ = params;
            // TODO: Need to make sure buffer was flushed
            const n = try self.protocol.sendMessage(buffer, 'Q', query);
            if (n >= buffer.len) return error.NoSpaceLeft;
            const msg = try self.protocol.readMessage(buffer[n..]);
            const k = n + std.math.absCast(msg.len);

            var status: CursorStatus = .running;
            switch (msg.code) {
                'T' => {
                    if (k >= buffer.len) {
                        return error.NoSpaceLeft; // No room to hold rows
                    }
                    status = CursorStatus.rows;
                },
                'C' => try self.protocol.handleCommandComplete(msg),
                'E' => try self.protocol.handleErrorResponse(msg), // Throws error
                'Z' => {
                    status = CursorStatus.complete;
                },
                else => {},
            }
            const storage = buffer[k..];
            return Cursor{
                .query = query,
                .protocol = &self.protocol,
                .internal_allocator = std.heap.FixedBufferAllocator.init(storage),
                .status = status,
                .initial_msg = msg,
                .last_msg = msg,
            };
        }

        // Execute a string that may contain multiple statments delmited by ;
        pub fn executeQuery(self: *Self, buffer: []u8, query: anytype) !Cursor {
            var fbo = std.io.fixedBufferStream(buffer);
            try query.build(fbo.writer());
            const result_buffer = buffer[fbo.pos..];
            const statements = fbo.getWritten();
            var cursor: Cursor = undefined;
            var start_index: usize = 0;
            while (std.mem.indexOfPos(u8, statements, start_index, ";")) |pos| {
                const q = statements[start_index..pos+1];
                cursor = try self.execute(result_buffer, q, .{});
                start_index = pos+1;
            }
            if (start_index == 0) {
                return error.IncompleteStatement;
            }
            return cursor;
        }

        // Execute a single statement
        pub fn executeSingleQuery(self: *Self, buffer: []u8, query: anytype) !Cursor {
            var fbo = std.io.fixedBufferStream(buffer);
            try query.build(fbo.writer());
            const remaining = buffer[fbo.pos..];
            return try self.execute(remaining, fbo.getWritten(), .{});
        }

        pub fn close(self: *Self) void {
            self.protocol.close();
            self.state = .termination;
        }
    };
}
