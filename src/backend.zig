// -------------------------------------------------------------------------- //
// Copyright (c) 2022, Jairus Martin.                                         //
// Distributed under the terms of the MIT License.                            //
// The full license is in the file LICENSE, distributed with this software.   //
// -------------------------------------------------------------------------- //
const Engine = @import("engine.zig").Engine;

pub const Backend = enum {
    postgres,
    mysql,
    sqlite,

    pub fn Connection(comptime self: Backend) type {
        return @import("connection.zig").Connection(self);
    }

    pub fn Cursor(comptime self: Backend) type {
        return @import("cursor.zig").Cursor(self);
    }

    pub fn Protocol(comptime self: Backend) type {
        return switch (self) {
            .postgres => @import("backend/postgres.zig").Protocol,
            .mysql => @import("backend/mysql.zig").Protocol,
            .sqlite => @import("backend/sqlite.zig").Protocol,
        };
    }

    pub fn Message(comptime self: Backend) type {
        return switch (self) {
            .postgres => @import("backend/postgres.zig").Message,
            .mysql => @import("backend/mysql.zig").Message,
            .sqlite => @import("backend/sqlite.zig").Message,
        };
    }

    pub fn defaultPort(self: Backend) u16 {
        return switch (self) {
            .postgres => 5432,
            .mysql => 3306,
            .sqlite => 0,
        };
    }

};
