// -------------------------------------------------------------------------- //
// Copyright (c) 2022, Jairus Martin.                                         //
// Distributed under the terms of the MIT License.                            //
// The full license is in the file LICENSE, distributed with this software.   //
// -------------------------------------------------------------------------- //

const std = @import("std");
const util = @import("util.zig");

const TableOptions = @import("table.zig").TableOptions;
const Table = @import("table.zig").Table;

pub fn ModelManager(comptime T: type, comptime options: TableOptions) type {
    const Manager = struct {
        const Self = @This();
        pub const table = Table(T, options);

        pub fn create(self: Self, values: anytype) !T {
            var q = Manager.table.insert();
            q.values = values;
            conn.executeQuery(q);
        }

        pub fn get(self: Self, parameters: anytype) !?T {}

        pub fn getOrCreate(self: Self, parameters: anytype) !T {}

        pub fn filter(self: Self, parameters: anytype) !Self {}

        pub fn orderBy(self: *Self, fields: anytype) Self {}

        pub fn limit(self: *Self, count: anytype) Self {}

        pub fn offset(self: *Self, count: anytype) Self {}

        pub fn distinct(self: *Self, fields: anytype) Self {}

        pub fn selectRelated(self: *Self, fields: anytype) Self {}

        pub fn prefetchRelated(self: *Self, buf: []u8, fields: anytype) Self {}

        pub fn all(self: Self) ![]T {}

        pub fn delete(self: Self) !usize {}

        pub fn update(self: Self, parameters: anytype) !usize {}
    };
    return Manager;
}

// Generates a model for the given type. This defines methods
// save, delete, and restore if the user has not defined them.
// It also assigns a "manager" as the objects field of the model.
pub fn Model(comptime T: type, comptime options: TableOptions) type {
    // TODO: If pk is not optional throw compile error
    if (comptime util.isOptionalField(T, options.pk)) {
        @compileError("The pk field '" ++ name ++ "'on '" ++ @typeName(T) ++ "' must be optional");
    }

    const M = struct {
        const Self = @Self();
        pub const objects = ModelManager(T, options);
        state: T,

        pub fn save(self: *Self) !void {
            // TODO: Assumes an integer, support different field types
            if (@field(self.state, options.pk)) |pk| {
                // Update
                const cursor = try self.objects.table.update().where(.{ .pk = pk }).values(self.state);
                if (cursor.lastRowCount() != 1) {
                    return error.DbSaveError;
                }
            } else {
                // Insert new
                const cursor = try self.objects.table.insert().values(self.state);
                @field(self.state, options.pk) = cursor.lastRowId();
            }
        }

        pub fn delete(self: *Self) !void {
            if (@field(self.state, options.pk)) |pk| {
                const cursor = try self.objects.table.delete().where(.{ .pk = pk }).values(self.state);
                if (cursor.lastRowCount() != 1) {
                    return error.DbDeleteError;
                }
            }
            // TODO: Assumes integer
            @field(self.state, options.pk) = null;
        }

        //pub fn restore(state: anytype) !M {
        //}

    };
    return M;
}
