// -------------------------------------------------------------------------- //
// Copyright (c) 2022, Jairus Martin.                                         //
// Distributed under the terms of the MIT  License.                           //
// The full license is in the file LICENSE, distributed with this software.   //
// -------------------------------------------------------------------------- /
const Builder = @import("std").build.Builder;

pub fn build(b: *Builder) void {
    const mode = b.standardReleaseOptions();
    const exe = b.addExecutable("zq-test", "tests/main.zig");

    exe.setBuildMode(mode);
    exe.addPackagePath("zq", "src/zq.zig");
    exe.valgrind_support = true;
    exe.strip = false;
    const run_cmd = exe.run();

    const run_step = b.step("run", "Run the example");
    run_step.dependOn(&run_cmd.step);

    b.default_step.dependOn(&exe.step);
    b.installArtifact(exe);

    var main_tests = b.addTest("src/zq.zig");
    main_tests.setBuildMode(mode);

    const test_step = b.step("test", "Run library tests");
    test_step.dependOn(&main_tests.step);

}
