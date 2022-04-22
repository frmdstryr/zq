// -------------------------------------------------------------------------- //
// Copyright (c) 2022, Jairus Martin.                                         //
// Distributed under the terms of the MIT  License.                           //
// The full license is in the file LICENSE, distributed with this software.   //
// -------------------------------------------------------------------------- //
const std = @import("std");

pub const DataType = enum {
    // oid 16
    bool,
    optional_bool,

    // oid 18
    i8,
    optional_i8,

    // oid 23
    i16,
    optional_i16,

    // oid 23
    i32,
    optional_i32,

    // oid 20
    i64,
    optional_i64,

    // oid 17
    u8,
    optional_u8,

    // oid 23
    u16,
    optional_u16,

    // oid 23
    u32,
    optional_u32,

    // oid 20
    u64,
    optional_u64,

    // oid 701
    f32,
    optional_f32,

    // oid 25
    str,
    optional_str,

    json,
    optional_json,
};

pub const DataValue = union(DataType) {
    bool: bool,
    optional_bool: ?bool,

    i8: i8,
    optional_i8: ?i8,

    i16: i16,
    optional_i16: ?i16,

    i32: i32,
    optional_i32: ?i32,

    i64: i64,
    optional_i64: ?i64,

    u8: u8,
    optional_u8: ?u8,

    u16: u16,
    optional_u16: ?u16,

    u32: u32,
    optional_u32: ?u32,

    u64: u64,
    optional_u64: ?u64,

    f32: f32,
    optional_f32: ?f32,

    str: []const u8,
    optional_str: ?[]const u8,

    json: std.json.ValueTree,
    optional_json: ?std.json.ValueTree,
};

pub const RowFormatOptions = struct {
    before: []const u8 = "",
    after: []const u8 = "\n",
    format: []const u8 = "{s:<30}", // TODO Support multiple columns here...
    separator: []const u8 = "| ",
};

pub const Header = struct {
    table_id: i32,
    type_mod: i32,
    data_type: i32,
    attribute_number: i16,
    // Note that negative values denote variable-width types.
    type_len: i16,
    format_code: i16,
    name: []const u8,
    pub fn format(
        self: Header,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        out_stream: anytype,
    ) !void {
        _ = fmt;
        _ = options;
        try std.fmt.format(out_stream, "Header{{ .table_id={d}, .type_mod={d}, .data_type={d}, .attribute_number={d}, .type_len={d}, .format_code={d}, .name=\"{s}\" }}", .{ self.table_id, self.type_mod, self.data_type, self.attribute_number, self.type_len, self.format_code, self.name });
    }
};

inline fn validateStream(out_stream: anytype) void {
    if (!@hasDecl(@TypeOf(out_stream), "print")) {
        @compileError("out_stream must be a stream");
    }
}

pub const HeaderRow = struct {
    headers: []Header,

    pub fn write(self: HeaderRow, comptime opts: RowFormatOptions, out_stream: anytype) !void {
        const last = if (self.headers.len > 0) self.headers.len - 1 else 0;
        if (comptime opts.before.len > 0) {
            try out_stream.writeAll(opts.before);
        }
        for (self.headers) |h, i| {
            const sep = if (i == last) "" else opts.separator;
            try out_stream.print(opts.format ++ "{s}", .{ h.name, sep });
        }
        if (comptime opts.after.len > 0) {
            try out_stream.writeAll(opts.after);
        }
    }
};

pub const Row = struct {
    headers: []Header,
    data: []DataValue,

    pub fn write(self: Row, comptime opts: RowFormatOptions, out_stream: anytype) !void {
        if (comptime opts.before.len > 0) {
            try out_stream.writeAll(opts.before);
        }
        const last = if (self.data.len > 0) self.data.len - 1 else 0;
        for (self.data) |v, i| {
            const sep = if (i == last) "" else opts.separator;
            switch (v) {
                DataType.str => |value| {
                    try out_stream.print(opts.format ++ "{s}", .{ value, sep }); // str
                },
                DataType.optional_str => |value| {
                    try out_stream.print(opts.format ++ "{s}", .{ value, sep }); // opt str
                },
                DataType.bool => |value| {
                    try out_stream.print(opts.format ++ "{s}", .{ value, sep }); // bool
                },
                DataType.optional_bool => |value| {
                    try out_stream.print(opts.format ++ "{s}", .{ value, sep }); // opt bool
                },
                DataType.i8 => |value| {
                    try out_stream.print("{d}{s}", .{ value, sep }); // i8
                },
                DataType.i16 => |value| {
                    try out_stream.print("{d}{s}", .{ value, sep }); // i16
                },
                DataType.i32 => |value| {
                    try out_stream.print("{d}{s}", .{ value, sep }); // i32
                },
                DataType.i64 => |value| {
                    try out_stream.print("{d}{s}", .{ value, sep }); // i64
                },
                DataType.u8 => |value| {
                    try out_stream.print("{d}{s}", .{ value, sep }); // u8
                },
                DataType.u16 => |value| {
                    try out_stream.print("{d}{s}", .{ value, sep }); // u16
                },
                DataType.u32 => |value| {
                    try out_stream.print("{d}{s}", .{ value, sep }); // u32
                },
                DataType.u64 => |value| {
                    try out_stream.print("{d}{s}", .{ value, sep }); // u64
                },
                else => |value| {
                    try out_stream.print("{s}{s}", .{ value, sep }); // Default
                },
            }
        }
        if (comptime opts.after.len > 0) {
            try out_stream.writeAll(opts.after);
        }
    }
};
