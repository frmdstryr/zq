# ZQ

A pure Zig object relation mapper (currently using Postgres).

> Note: This is a very incomplete work in progress. It can only do raw queries
on postgres at the moment.


# Intro


### Connecting to a database

To connect to a database create an "engine" for your database type and then
acquire a connection from the pool.

```zig

// Create an "engine"
var engine = Engine(.postgres).init(allocator, .{
    .host="127.0.0.1",
    .port=5432,
    .user="postgres",
    .pass="postgres",
    .db="test_zq",
});
defer engine.deinit();

// Grab a connection from the pool
const conn = try engine.acquire();
defer conn.close();

```

The allocator is only used to create new connections in the connection pool.

> Note: There's provisions for multiple backends but only postgres is implemented


### Executing queries

The connection has `execute` and `executeQuery` functions. Both return a cursor
that can be iterated on the rows. Each (currently) requires you to pass a buffer
that has enough room for the initial query and response.

Subsequent rows can use their own buffer's allocator as needed.


```zig
pub fn executeRaw(comptime buffer_size: usize, conn: anytype, query: []const u8) !void {
    var query_buffer: [buffer_size]u8 = undefined;
    var cursor = conn.execute(&query_buffer, query, .{}) catch |err| {
        if (conn.protocol.errorMessage()) |msg| {
            std.log.err("{s}", .{msg});
        }
        return err;
    };

    const stdout = std.io.getStdOut().writer();
    var row_buffer: [buffer_size]u8 = undefined;
    switch (cursor.status) {
        .rows => {
            const headers = try cursor.headers();
            try headers.write(.{}, stdout);

            // Read data
            // NOTE: Each call overwrites data in the buffer
            // if you don't want this pass a new buffer
            while (try cursor.rows(&row_buffer)) |row| {
                try row.write(.{}, stdout);
            }
        },
        .running => {
            while (try cursor.process(&row_buffer)) |msg| {
                try stdout.print("{s}", .{msg});
            }
        },
        .complete => {
            std.log.info("Query '{s}' complete!", .{cursor.query});
        },
    }
}
```

Then to use it:

```zig
try executeRaw(4096, conn, "DROP DATABASE IF EXISTS test_zq;");
try executeRaw(4096, conn, "CREATE DATABASE test_zq;");
try executeRaw(4096, conn, "COMMIT;");
```


### Creating a model

A model is a just a struct. Meta for database columns can be added by nesting a
`Meta` struct as shown below.  The primary key field currentl ymust be optional.

```zig
const db = @import("zq");

pub const User = struct {
    id: ?u32 = null, // id defaults to the pk
    username: []const u8,
    hashedpass: []const u8,
    email: ?[]const u8,

    // Table Metadata
    pub const Meta = struct {
        pub const table = "users";
        pub const hashedpass_len = 64;
        pub const username_len = 100;
        pub const username_unique = true;
        pub const email_unique = true;
        pub const email_len = 255;
    };

    pub const table = db.Table(@This(), .postgres, .{});
};
```

Alternatively you can pass meta to the last parameter of the `Table`.
See `TableOptions` and `Column` definitons in the `table.zig`.

### Creating / dropping a table for the model

Once a table is created you can create/drop it using the functions on the table.

Create

```zig
// Create
var buf: [1024]u8 = undefined;
var cursor = try conn.executeQuery(&buf, User.table.create());
try cursor.print(stdout);
```

and drop

```zig
// Drop
var buf: [1024]u8 = undefined;
var cursor = try conn.executeQuery(&buf, User.table.drop());
try cursor.print(stdout);
```

### Model queries

This is a work in progress.. Eventually it should have some easier way to
do this using a "manager" like django.

```
var user = User{
    .username="testuser",
    .hashedpass="thisisnothashed",
    .email=null,
};
cursor = try conn.executeQuery(&buf, User.table.insert().values(user));
try cursor.print(stdout);

```
