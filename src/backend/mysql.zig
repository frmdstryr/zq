// -------------------------------------------------------------------------- //
// Copyright (c) 2022, Jairus Martin.                                         //
// Distributed under the terms of the MIT  License.                           //
// The full license is in the file LICENSE, distributed with this software.   //
// -------------------------------------------------------------------------- //
const std = @import("std");
const comptimePrint = std.fmt.comptimePrint;
const Column = @import("../table.zig").Column;
const TableOptions = @import("../table.zig").TableOptions;

pub fn createColumn(
    comptime T: type,
    comptime table_name: []const u8,
    comptime column: Column,
    comptime field_type: type,
) []const u8 {
    _ = table_name;
    comptime var sql = comptimePrint("{s} {s}", .{
        column.name,
        switch (field_type) {
            bool => "bool",
            u8 => "char",
            i16 => "int2",
            i32 => "int4",
            i64 => "int8",
            i8 => "integer",
            f32 => "float4",
            f64 => "float8",
            u16 => "int4",
            u32 => "int8",
            []const u8 => if (column.len > 0)
                comptimePrint("varchar({d})", .{column.len})
            else
                "text",
            // TODO: The rest
            else => |v| @compileError(comptimePrint("Cannot generate table column for field `{s}: {s}` of '{s}` (type {s})", .{
                column.field, @typeName(field_type), @typeName(T), v,
            })),
        },
    });

    if (column.pk) {
        sql = sql ++ " PRIMARY KEY";
        if (column.autoincrement) {
            sql = sql ++ "AUTO_INCREMENT";
        }
    } else {
        if (!column.optional) {
            sql = sql ++ " NOT NULL";
        }
        if (column.unique) {
            sql = sql ++ " UNIQUE";
        }
        if (column.default) |v| {
            sql = sql ++ comptimePrint(" DEFAULT {s}", .{v});
        }
    }
    return sql;
}

pub fn createTable(
    comptime table_name: []const u8,
    comptime columns: []const Column,
    comptime options: TableOptions,
) []const u8 {
    comptime var sql = comptimePrint("CREATE TABLE {s} (\n", .{table_name});

    // Insert column definitions
    inline for (columns) |col, i| {
        const sep = if (i == columns.len - 1) "" else ",";
        sql = sql ++ comptimePrint("  {s}{s}\n", .{ col.sql, sep });
    }

    // Insert constraints
    if (options.constraints) |constraints| {
        inline for (constraints) |constraint| {
            sql = sql ++ comptimePrint("  CONSTRAINT {s}\n", .{constraint});
        }
    }
    // Insert checs
    if (options.checks) |checks| {
        inline for (checks) |check| {
            sql = sql ++ comptimePrint("  CHECK({s})\n", .{check});
        }
    }

    return sql; // Strip last newline
}
