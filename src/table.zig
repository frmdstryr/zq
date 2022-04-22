// -------------------------------------------------------------------------- //
// Copyright (c) 2022, Jairus Martin.                                         //
// Distributed under the terms of the MIT  License.                           //
// The full license is in the file LICENSE, distributed with this software.   //
// -------------------------------------------------------------------------- //
const std = @import("std");
const builtin = @import("builtin");
const testing = std.testing;
const comptimePrint = std.fmt.comptimePrint;

const util = @import("util.zig");

const Query = @import("query.zig").Query;
const RawQuery = @import("query.zig").RawQuery;
const Backend = @import("backend.zig").Backend;

const pg = @import("backend/postgres.zig");
const mysql = @import("backend/mysql.zig");
const sqlite = @import("backend/sqlite.zig");

pub const TableOptions = struct {
    // Name of pk field
    pk: []const u8 = "id",

    // Name of table
    name: []const u8 = "",

    // Database name
    database: []const u8 = "default",

    // Column definitions, you can define your own here
    columns: []const Column = &[_]Column{},

    // Constraints
    constraints: ?[]const []const u8 = null,

    // Checks
    checks: ?[]const []const u8 = null,
};

pub const ForeignKeyAction = enum {
    cascade,
    protect, // restrict
    setnull,
};

pub const Column = struct {
    // Name of field in struct / state
    field: []const u8,

    // Set to false if you want it to not create a column in the database.
    // eg for in memory computed values.
    store: bool = true,

    // Primary key
    pk: bool = false,

    // Auto increment (if primary key)
    autoincrement: bool = false,

    // Foreign key
    fk: []const u8 = "",

    // Action when the fk is deleted
    on_delete: ForeignKeyAction = .cascade,

    // Action when the fk is update
    on_update: ForeignKeyAction = .cascade,

    // Column name in database, this can differ from the field name to
    // help with migrations and using existing tables
    name: []const u8 = "",

    // Whether to apply a uniqe constraint
    unique: bool = false,

    // Length value
    len: usize = 0,

    // Default value
    default: ?[]const u8 = null,

    // Nullable, this auto detected
    optional: bool = true,

    // Column SQL, if provided (non-zero len) this will be used in the
    // create table command for the given field.
    sql: []const u8 = "",
};

pub fn createColumn(
    comptime backend: Backend,
    comptime T: type,
    comptime name: []const u8,
    comptime pk_name: []const u8,
    comptime table_name: []const u8,
    comptime options: TableOptions,
) Column {
    const f = util.fieldByName(T, name);
    comptime var column = Column{
        .field = f.name,
        .name = f.name,
        .optional = @typeInfo(f.field_type) == .Optional,
        .pk = std.mem.eql(u8, pk_name, f.name),
        .default = null,
    };
    column.autoincrement = column.pk;

    if (f.default_value) |value| {
        // On zig 0.10.0
        if (builtin.zig_version.minor > 9) {
            const v = @ptrCast(*const f.field_type, value);
            column.default = comptimePrint("{s}", .{v.*});
        } else {
            column.default = comptimePrint("{s}", .{value});
        }
    }

    // Attempt to lookup options defined using a prefix decl
    if (@hasDecl(T, "Meta")) {
        const Meta = T.Meta;
        inline for (std.meta.fieldNames(Column)) |param| {
            const attr = name ++ "_" ++ param;
            if (!std.mem.eql(u8, param, "field") and @hasDecl(Meta, attr)) {
                @field(column, param) = @field(Meta, attr);
            }
        }
    }

    // Attempt to lookup the column provided in options (if any) to grab
    // the length and and any other options
    inline for (options.columns) |meta| {
        if (std.mem.eql(u8, meta.field, f.name)) {
            if (meta.name.len > 0) {
                column.name = meta.name; // Renamed column
            }
            column.unique = meta.unique;
            column.len = meta.len;
            if (meta.default) |v| {
                column.default = v;
            }
        }
    }

    // Generate sql using backend
    if (column.sql.len == 0) {
        const data_type = switch (@typeInfo(f.field_type)) {
            .Optional => |info| info.child,
            else => f.field_type,
        };
        column.sql = switch (backend) {
            .postgres => pg.createColumn(T, table_name, column, data_type),
            .mysql => mysql.createColumn(T, table_name, column, data_type),
            .sqlite => sqlite.createColumn(T, table_name, column, data_type),
        };
    }
    return column;
}

pub fn fieldNamesExclude(comptime fields: []const []const u8, comptime exclude: []const u8) [fields.len - 1][]const u8 {
    comptime {
        var names: [fields.len - 1][]const u8 = undefined;
        var i: usize = 0;
        for (fields) |field| {
            if (!std.mem.eql(u8, field, exclude)) {
                names[i] = field;
                i += 1;
            }
        }
        return names;
    }
}

pub fn Table(comptime T: type, comptime backend: Backend, comptime options: TableOptions) type {
    comptime var pk_name: []const u8 = options.pk;
    comptime var table_name: []const u8 = options.name;
    comptime var db_name: []const u8 = options.database;
    if (@hasDecl(T, "Meta")) {
        const Meta = T.Meta;
        if (@hasDecl(Meta, "pk")) {
            pk_name = Meta.pk;
        }

        if (@hasDecl(Meta, "table")) {
            table_name = Meta.table;
        }

        if (@hasDecl(Meta, "database")) {
            db_name = Meta.database;
        }
    }

    if (table_name.len == 0) {
        @compileError("No table name specified for '" ++ @typeName(T) ++
            "'. Please set it using 'options.name' or define 'pub const table = \"name\"' (Meta be pub)");
    }

    const fields = std.meta.fieldNames(T);
    const fields_except_pk = &fieldNamesExclude(fields, pk_name);
    comptime var table_columns: [fields.len]Column = undefined;
    inline for (std.meta.fieldNames(T)) |n, i| {
        table_columns[i] = createColumn(backend, T, n, pk_name, table_name, options);
    }

    return struct {
        const Self = @This();
        pub const columns = &table_columns;
        pub const SelectQuery = Query(T, backend, .select, columns);
        pub const InsertQuery = Query(T, backend, .insert, columns);
        pub const UpdateQuery = Query(T, backend, .update, columns);
        pub const DeleteQuery = Query(T, backend, .delete, columns);

        pub fn create() RawQuery {
            const create_sql = switch (backend) {
                .postgres => pg.createTable(table_name, columns, options),
                .mysql => mysql.createTable(table_name, columns, options),
                .sqlite => sqlite.createTable(table_name, columns, options),
            };
            return RawQuery{ .statement = create_sql };
        }

        pub fn drop() RawQuery {
            const drop_sql = comptimePrint("DROP TABLE {s};", .{table_name});
            return RawQuery{ .statement = drop_sql };
        }

        pub fn select() SelectQuery {
            return SelectQuery{ .from = table_name, .fields = fields };
        }

        pub fn update() UpdateQuery {
            return UpdateQuery{ .from = table_name, .fields = fields };
        }

        pub fn insert() InsertQuery {
            return InsertQuery{ .from = table_name, .fields = fields_except_pk };
        }

        pub fn delete() DeleteQuery {
            return DeleteQuery{ .from = table_name };
        }
    };
}

test "create-basic" {
    const User = struct {
        id: ?u32,
        name: ?[]const u8 = null,
        email: []const u8,
        enabled: bool = true,
    };

    const UserTable = Table(User, .postgres, .{
        .name = "users",
        .columns = &[_]Column{ .{ .field = "name", .len = 100 }, .{ .field = "email", .len = 255 } },
    });
    const q = UserTable.create();
    var buf: [4096]u8 = undefined;
    var fbo = std.io.fixedBufferStream(&buf);
    try q.build(fbo.writer());
    try testing.expectEqualStrings(
        \\CREATE SEQUENCE users_id_seq;
        \\CREATE TABLE users (
        \\  id int8 PRIMARY KEY DEFAULT nextval('users_id_seq'),
        \\  name varchar(100) DEFAULT null,
        \\  email varchar(255) NOT NULL,
        \\  enabled bool NOT NULL DEFAULT true
        \\);
        \\ALTER SEQUENCE users_id_seq OWNED BY users.id;
    , fbo.getWritten());
}

test "create-meta" {
    const User = struct {
        uuid: ?u32,
        name: ?[]const u8 = null,
        email: []const u8,
        enabled: bool = true,

        pub const Meta = struct {
            pub const table = "users";
            pub const pk = "uuid";
            pub const uuid_autoincrement = false;
            pub const name_len = 100;
            pub const email_len = 255;
            pub const email_unique = true;
        };

        pub const table = Table(@This(), .postgres, .{});
    };

    const q = User.table.create();
    var buf: [4096]u8 = undefined;
    var fbo = std.io.fixedBufferStream(&buf);
    try q.build(fbo.writer());
    try testing.expectEqualStrings(
        \\CREATE TABLE users (
        \\  uuid int8 PRIMARY KEY,
        \\  name varchar(100) DEFAULT null,
        \\  email varchar(255) NOT NULL UNIQUE,
        \\  enabled bool NOT NULL DEFAULT true
        \\);
    , fbo.getWritten());
}

test "drop-table" {
    const User = struct {
        id: ?u32,
    };

    const UserTable = Table(User, .postgres, .{ .name = "users" });
    const q = UserTable.drop();
    var buf: [4096]u8 = undefined;
    var fbo = std.io.fixedBufferStream(&buf);
    try q.build(fbo.writer());
    try testing.expectEqualStrings("DROP TABLE users;", fbo.getWritten());
}
