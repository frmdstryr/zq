// -------------------------------------------------------------------------- //
// Copyright (c) 2022, Jairus Martin.                                         //
// Distributed under the terms of the MIT  License.                           //
// The full license is in the file LICENSE, distributed with this software.   //
// -------------------------------------------------------------------------- //
const std = @import("std");
const testing = std.testing;
const StructField = std.builtin.TypeInfo.StructField;
const Allocator = std.mem.Allocator;

pub fn fieldByName(comptime T: type, comptime name: []const u8) StructField {
    inline for (std.meta.fields(T)) |f| {
        if (std.mem.eql(u8, f.name, name)) {
            return f;
        }
    }
    @compileError("Struct '" ++ @typeName(T) ++ " has no field '" ++ name ++ "'");
}

pub fn isOptionalField(comptime T: type, comptime name: []const u8) bool {
    const info = fieldByName(T, name);
    return @typeInfo(info.field_type) == .Optional;
}

test "is-optional-field" {
    const Foo = struct {
        a: i32,
        b: ?i32,
    };
    try testing.expectEqual(isOptionalField(Foo, "a"), false);
    try testing.expectEqual(isOptionalField(Foo, "b"), true);
}

const DummyHeldLock = struct {
    mutex: *std.Thread.Mutex,
    pub fn release(self: DummyHeldLock) void {
        self.mutex.unlock();
    }
};

// The event based lock doesn't work without evented io
pub const Lock = if (std.io.is_async) std.event.Lock else std.Thread.Mutex;
pub const HeldLock = if (std.io.is_async) std.event.Lock.Held else DummyHeldLock;

pub fn ObjectPool(comptime T: type) type {
    return struct {
        const Self = @This();
        pub const ObjectList = std.ArrayList(*T);

        allocator: Allocator,
        // Stores all created objects
        objects: ObjectList,

        // Stores objects that have been released
        free_objects: ObjectList,

        // Lock to use if using threads
        mutex: Lock = Lock{},

        pub fn init(allocator: Allocator) Self {
            return Self{
                .allocator = allocator,
                .objects = ObjectList.init(allocator),
                .free_objects = ObjectList.init(allocator),
            };
        }

        // Get an object released back into the pool
        pub fn get(self: *Self) ?*T {
            if (self.free_objects.items.len == 0) return null;
            return self.free_objects.swapRemove(0); // Pull the oldest
        }

        // Create an object and allocate space for it in the pool
        pub fn create(self: *Self) !*T {
            const obj = try self.allocator.create(T);
            try self.objects.append(obj);
            try self.free_objects.ensureTotalCapacity(self.objects.items.len);
            return obj;
        }

        // Return a object back to the pool, this assumes it was created
        // using create (which ensures capacity to return this quickly).
        pub fn release(self: *Self, object: *T) void {
            return self.free_objects.appendAssumeCapacity(object);
        }

        pub fn deinit(self: *Self) void {
            while (self.objects.popOrNull()) |obj| {
                self.allocator.destroy(obj);
            }
            self.objects.deinit();
            self.free_objects.deinit();
        }

        pub fn acquire(self: *Self) HeldLock {
            if (std.io.is_async) {
                return self.mutex.acquire();
            } else {
                self.mutex.lock();
                return DummyHeldLock{ .mutex = &self.mutex };
            }
        }
    };
}
