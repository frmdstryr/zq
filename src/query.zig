// -------------------------------------------------------------------------- //
// Copyright (c) 2022, Jairus Martin.                                         //
// Distributed under the terms of the MIT  License.                           //
// The full license is in the file LICENSE, distributed with this software.   //
// -------------------------------------------------------------------------- //
const std = @import("std");
const assert = std.debug.assert;
const testing = std.testing;

const Backend = @import("backend.zig").Backend;
const Column = @import("table.zig").Column;

pub const Join = struct {
    pub const Type = enum { inner, left, outer, cross };
    table: []const u8,
    join_type: Type = .inner,
    on: []const u8,
};

pub const OrderBy = struct {
    field: []const u8,
    desc: bool = false,
};

pub const Value = struct {
    field: []const u8,
    value: usize,
};

pub const Where = struct {
    pub const Op = enum {
        eq,
        ne,
        lt,
        lte,
        gt,
        gte,
        in,
        and_,
        or_,
        between,
        is_null,
        not,
    }; // TODO handle more
    field: []const u8,
    op: Op = .eq,
    param: []const u8 = "%s",

    pub fn opcode(self: Where) []const u8 {
        return switch (self.op) {
            .eq => " = ",
            .ne => " != ",
            .lt => " < ",
            .lte => " <= ",
            .gt => " > ",
            .gte => " >= ",
            .in => " IN ",
            .and_ => " AND ",
            .or_ => " OR ",
            .between => " BETWEEN ",
            .is_null => " IS NULL",
            .not => " NOT ",
        };
    }
};

pub const QueryType = enum { create, select, insert, update, delete, drop };

pub const ListFormatOptions = struct {
    // Added before and after
    before: []const u8 = "(",
    after: []const u8 = ")",
    // Separator
    sep: []const u8 = ", ",
};

// Write a comma separated list of strings
pub fn writeList(out_stream: anytype, fields: []const []const u8, comptime opts: ListFormatOptions) !void {
    assert(fields.len > 0); // WARNING: You must check bounds first
    const n = fields.len - 1;
    if (comptime opts.before.len > 0) {
        try out_stream.writeAll(opts.before);
    }
    for (fields[0..n]) |f| {
        try out_stream.writeAll(f);
        try out_stream.writeAll(opts.sep);
    }
    try out_stream.writeAll(fields[n]);
    if (comptime opts.after.len > 0) {
        try out_stream.writeAll(opts.after);
    }
}

test "write-list" {
    var buf: [1024]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    try writeList(fbs.writer(), &[_][]const u8{ "1", "2", "3" }, .{});
    try testing.expectEqualStrings("(1, 2, 3)", fbs.getWritten());
}

// Query that writes out a pre-prepared statement
pub const RawQuery = struct {
    const Self = @This();
    statement: []const u8,
    pub fn build(self: Self, out_stream: anytype) !void {
        try out_stream.writeAll(self.statement);
    }
};

const SelectQuery = struct {
    const Self = @This();
    from: []const u8,
    fields: []const []const u8,
    where_clauses: []const Where = &[_]Where{},
    order_by: []const OrderBy = &[_]OrderBy{},
    limit_count: usize = 0,
    start_offset: usize = 0,

    pub fn build(self: Self, out_stream: anytype) !void {
        try out_stream.writeAll("SELECT ");
        if (self.fields.len > 0) {
            try writeList(out_stream, self.fields, .{ .before = "", .after = "" });
        } else {
            try out_stream.writeAll("*");
        }
        try out_stream.print(" FROM {s}", .{self.from});

        if (self.where_clauses.len > 0) {
            // WHERE expr=%param
            const n = self.where_clauses.len - 1;
            try out_stream.writeAll(" WHERE ");
            for (self.where_clauses) |clause, i| {
                try out_stream.writeAll(clause.field);
                try out_stream.writeAll(clause.opcode());
                try out_stream.writeAll(clause.param);
                if (i < n) try out_stream.writeAll(" AND ");
            }
        }

        if (self.order_by.len > 0) {
            const n = self.order_by.len - 1;
            try out_stream.writeAll(" ORDER BY ");
            for (self.order_by) |clause, i| {
                try out_stream.writeAll(clause.field);
                if (clause.desc) try out_stream.writeAll(" DESC");
                if (i < n) try out_stream.writeAll(", ");
            }
        }

        if (self.limit_count > 0) {
            try out_stream.print(" LIMIT {d}", .{self.limit_count});
        }
        if (self.start_offset > 0) {
            try out_stream.print(" OFFSET {d}", .{self.start_offset});
        }
        try out_stream.writeAll(";");
    }

    pub fn limit(self: Self, value: usize) Self {
        var q = self.clone();
        q.limit_count = value;
        return q;
    }

    pub fn offset(self: Self, value: usize) Self {
        var q = self.clone();
        q.start_offset = value;
        return q;
    }

    pub fn where(self: Self, parameters: anytype) Self {
        var q = self.clone();
        q.where_clauses = parameters;
        return q;
    }

    pub fn clone(self: Self) Self {
        return Self{
            .from = self.from,
            .fields = self.fields,
            .where_clauses = self.where_clauses,
            .order_by = self.order_by,
            .limit_count = self.limit_count,
            .start_offset = self.start_offset,
        };
    }
};

pub const UpdateQuery = struct {
    const Self = @This();
    from: []const u8,
    fields: ?[]const []const u8 = null,
    pub fn build(self: Self, out_stream: anytype) !void {
        try out_stream.print("UPDATE {s} SET", .{self.from});
    }
};

pub fn InsertQuery(comptime T: type, comptime columns: []const Column) type {
    return struct {
        const Self = @This();
        from: []const u8,
        fields: []const []const u8 = &[_][]const u8{},
        item: ?T = null,

        pub fn build(self: Self, out_stream: anytype) !void {
            if (self.item == null) {
                return error.UnboundQuery; // Use values() to bind it
            }
            if (self.fields.len > 0) {
                try out_stream.print("INSERT INTO {s} ", .{self.from});
                try writeList(out_stream, self.fields, .{});
                try out_stream.writeAll(" VALUES ");
            } else {
                try out_stream.print("INSERT INTO {s} VALUES ", .{self.from});
            }

            try out_stream.writeAll("(");
            inline for (columns) |col, i| {
                if (!col.pk) {
                    const val = self.item.?;
                    const field_type = @typeInfo(@TypeOf(@field(val, col.field)));

                    const type_info = switch (field_type) {
                        .Optional => |info| @typeInfo(info.child),
                        else => field_type,
                    };
                    comptime var template: []const u8 = switch (type_info) {
                        .Int => "{d},",
                        else => "'{s}',",
                    };
                    // TODO: NEED TO ESCAPE THIS
                    const v = @field(val, col.field);
                    if (i + 1 == columns.len) {
                        template = template[0 .. template.len - 1]; // Remove comma
                    }
                    try out_stream.print(template, .{v});
                }
            }
            try out_stream.writeAll(");");
        }

        pub fn values(self: Self, item: T) Self {
            return Self{
                .from = self.from,
                .fields = self.fields,
                .item = item,
            };
        }
    };
}

pub const DeleteQuery = struct {
    const Self = @This();
    from: []const u8,
    where: ?[]const u8,
    pub fn build(self: Self, out_stream: anytype) !void {
        if (self.where) |condition| {
            try out_stream.print("DELETE FROM {s} WHERE {s};", .{ self.from, condition });
        } else {
            try out_stream.print("DELETE FROM {s};", .{self.from});
        }
    }
};

pub fn Query(
    comptime T: type,
    comptime backend: Backend,
    comptime query_type: QueryType,
    comptime columns: []const Column,
) type {
    _ = backend;
    return switch (query_type) {
        .create, .drop => RawQuery,
        .select => SelectQuery,
        .update => UpdateQuery,
        .delete => DeleteQuery,
        .insert => InsertQuery(T, columns),
    };
}

test "query-select-1" {
    const User = struct {
        id: u32,
        email: []const u8,
    };

    const columns = &[_]Column{
        .{ .field = "id", .pk = true },
        .{ .field = "email" },
    };

    const q = Query(User, .postgres, .select, columns){
        .fields = &[_][]const u8{ "id", "email" },
        .from = "users",
    };
    var buf: [4096]u8 = undefined;
    var fbo = std.io.fixedBufferStream(&buf);
    try q.build(fbo.writer());
    try testing.expectEqualStrings("SELECT id, email FROM users;", fbo.getWritten());
}

test "query-select-2" {
    const Message = struct {
        id: u32,
        text: []const u8,
    };

    const columns = &[_]Column{
        .{ .field = "id", .pk = true },
        .{ .field = "text" },
    };
    const q = Query(Message, .postgres, .select, columns){
        .fields = &[_][]const u8{ "id", "text" },
        .from = "messages",
        .limit_count = 100,
        .start_offset = 200,
    };
    var buf: [4096]u8 = undefined;
    var fbo = std.io.fixedBufferStream(&buf);
    try q.build(fbo.writer());
    try testing.expectEqualStrings("SELECT id, text FROM messages LIMIT 100 OFFSET 200;", fbo.getWritten());
}

test "query-select-3" {
    const Event = struct {
        id: u32,
        created: usize,
        data: []const u8,
    };
    const columns = &[_]Column{
        .{ .field = "id", .pk = true },
        .{ .field = "created" },
        .{ .field = "data" },
    };
    const q = Query(Event, .postgres, .select, columns){
        .fields = &[_][]const u8{ "id", "created", "data" },
        .order_by = &[_]OrderBy{.{ .field = "created", .desc = true }},
        .from = "events",
    };
    var buf: [4096]u8 = undefined;
    var fbo = std.io.fixedBufferStream(&buf);
    try q.build(fbo.writer());
    try testing.expectEqualStrings("SELECT id, created, data FROM events ORDER BY created DESC;", fbo.getWritten());
}

test "query-insert-1" {
    const Message = struct {
        id: ?u32 = null,
        text: []const u8,
    };
    const columns = &[_]Column{
        .{ .field = "id", .pk = true },
        .{ .field = "text" },
    };
    const unbound = Query(Message, .postgres, .insert, columns){
        .fields = &[_][]const u8{"text"},
        .from = "messages",
    };

    var buf: [4096]u8 = undefined;
    var fbo = std.io.fixedBufferStream(&buf);
    const q = unbound.values(.{ .text = "Hello" });
    try q.build(fbo.writer());
    try testing.expectEqualStrings(
        "INSERT INTO messages (text) VALUES ('Hello');",
        fbo.getWritten(),
    );
}
