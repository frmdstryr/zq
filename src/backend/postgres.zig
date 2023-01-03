// -------------------------------------------------------------------------- //
// Copyright (c) 2022, Jairus Martin.                                         //
// Distributed under the terms of the MIT  License.                           //
// The full license is in the file LICENSE, distributed with this software.   //
// -------------------------------------------------------------------------- //
const std = @import("std");
const mem = std.mem;
const ascii = std.ascii;
const assert = std.debug.assert;
const Md5 = std.crypto.hash.Md5;
const comptimePrint = std.fmt.comptimePrint;

const util = @import("../util.zig");
const Params = @import("../connection.zig").Params;
const TableOptions = @import("../table.zig").TableOptions;
const Column = @import("../table.zig").Column;

const COPY = @bitCast(u32, [4]u8{ 'C', 'O', 'P', 'Y' });
const MOVE = @bitCast(u32, [4]u8{ 'M', 'O', 'V', 'E' });
const SELECT = @bitCast(u32, [4]u8{ 'S', 'E', 'L', 'E' });
const FETCH = @bitCast(u32, [4]u8{ 'F', 'E', 'T', 'C' });
const DELETE = @bitCast(u32, [4]u8{ 'D', 'E', 'L', 'E' });
const UPDATE = @bitCast(u32, [4]u8{ 'U', 'P', 'D', 'A' });
const INSERT = @bitCast(u32, [4]u8{ 'I', 'N', 'S', 'E' });

pub const Command = enum(u8) {
    Bind = 'B',
    Parse = 'P',
    Query = 'Q',
    Execute = 'E',
    Flush = 'F',
    Sync = 'S',
    Password = 'p',
    Describe = 'D',
    Terminate = 'X',
    Close = 'C',
};

// Generateed using a script for pg 14
pub const ErrorCode = enum(u64) {
    unknown = 0,
    successful_completion = 0x3030303030, // 00000
    warning = 0x3030303130, // 01000
    dynamic_result_sets_returned = 0x4330303130, // 0100C
    implicit_zero_bit_padding = 0x3830303130, // 01008
    null_value_eliminated_in_set_function = 0x3330303130, // 01003
    privilege_not_granted = 0x3730303130, // 01007
    privilege_not_revoked = 0x3630303130, // 01006
    string_data_right_truncation = 0x3430303130, // 01004
    deprecated_feature = 0x3130503130, // 01P01
    no_data = 0x3030303230, // 02000
    no_additional_dynamic_result_sets_returned = 0x3130303230, // 02001
    sql_statement_not_yet_complete = 0x3030303330, // 03000
    connection_exception = 0x3030303830, // 08000
    connection_does_not_exist = 0x3330303830, // 08003
    connection_failure = 0x3630303830, // 08006
    sqlclient_unable_to_establish_sqlconnection = 0x3130303830, // 08001
    sqlserver_rejected_establishment_of_sqlconnection = 0x3430303830, // 08004
    transaction_resolution_unknown = 0x3730303830, // 08007
    protocol_violation = 0x3130503830, // 08P01
    triggered_action_exception = 0x3030303930, // 09000
    feature_not_supported = 0x3030304130, // 0A000
    invalid_transaction_initiation = 0x3030304230, // 0B000
    locator_exception = 0x3030304630, // 0F000
    invalid_locator_specification = 0x3130304630, // 0F001
    invalid_grantor = 0x3030304c30, // 0L000
    invalid_grant_operation = 0x3130504c30, // 0LP01
    invalid_role_specification = 0x3030305030, // 0P000
    diagnostics_exception = 0x3030305a30, // 0Z000
    stacked_diagnostics_accessed_without_active_handler = 0x3230305a30, // 0Z002
    case_not_found = 0x3030303032, // 20000
    cardinality_violation = 0x3030303132, // 21000
    data_exception = 0x3030303232, // 22000
    array_subscript_error = 0x4532303232, // 2202E
    character_not_in_repertoire = 0x3132303232, // 22021
    datetime_field_overflow = 0x3830303232, // 22008
    division_by_zero = 0x3231303232, // 22012
    error_in_assignment = 0x3530303232, // 22005
    escape_character_conflict = 0x4230303232, // 2200B
    indicator_overflow = 0x3232303232, // 22022
    interval_field_overflow = 0x3531303232, // 22015
    invalid_argument_for_logarithm = 0x4531303232, // 2201E
    invalid_argument_for_ntile_function = 0x3431303232, // 22014
    invalid_argument_for_nth_value_function = 0x3631303232, // 22016
    invalid_argument_for_power_function = 0x4631303232, // 2201F
    invalid_argument_for_width_bucket_function = 0x4731303232, // 2201G
    invalid_character_value_for_cast = 0x3831303232, // 22018
    invalid_datetime_format = 0x3730303232, // 22007
    invalid_escape_character = 0x3931303232, // 22019
    invalid_escape_octet = 0x4430303232, // 2200D
    invalid_escape_sequence = 0x3532303232, // 22025
    nonstandard_use_of_escape_character = 0x3630503232, // 22P06
    invalid_indicator_parameter_value = 0x3031303232, // 22010
    invalid_parameter_value = 0x3332303232, // 22023
    invalid_preceding_or_following_size = 0x3331303232, // 22013
    invalid_regular_expression = 0x4231303232, // 2201B
    invalid_row_count_in_limit_clause = 0x5731303232, // 2201W
    invalid_row_count_in_result_offset_clause = 0x5831303232, // 2201X
    invalid_tablesample_argument = 0x4832303232, // 2202H
    invalid_tablesample_repeat = 0x4732303232, // 2202G
    invalid_time_zone_displacement_value = 0x3930303232, // 22009
    invalid_use_of_escape_character = 0x4330303232, // 2200C
    most_specific_type_mismatch = 0x4730303232, // 2200G
    null_value_not_allowed = 0x3430303232, // 22004
    null_value_no_indicator_parameter = 0x3230303232, // 22002
    numeric_value_out_of_range = 0x3330303232, // 22003
    sequence_generator_limit_exceeded = 0x4830303232, // 2200H
    string_data_length_mismatch = 0x3632303232, // 22026
    //    string_data_right_truncation = 0x3130303232, // 22001
    substring_error = 0x3131303232, // 22011
    trim_error = 0x3732303232, // 22027
    unterminated_c_string = 0x3432303232, // 22024
    zero_length_character_string = 0x4630303232, // 2200F
    floating_point_exception = 0x3130503232, // 22P01
    invalid_text_representation = 0x3230503232, // 22P02
    invalid_binary_representation = 0x3330503232, // 22P03
    bad_copy_file_format = 0x3430503232, // 22P04
    untranslatable_character = 0x3530503232, // 22P05
    not_an_xml_document = 0x4c30303232, // 2200L
    invalid_xml_document = 0x4d30303232, // 2200M
    invalid_xml_content = 0x4e30303232, // 2200N
    invalid_xml_comment = 0x5330303232, // 2200S
    invalid_xml_processing_instruction = 0x5430303232, // 2200T
    duplicate_json_object_key_value = 0x3033303232, // 22030
    invalid_argument_for_sql_json_datetime_function = 0x3133303232, // 22031
    invalid_json_text = 0x3233303232, // 22032
    invalid_sql_json_subscript = 0x3333303232, // 22033
    more_than_one_sql_json_item = 0x3433303232, // 22034
    no_sql_json_item = 0x3533303232, // 22035
    non_numeric_sql_json_item = 0x3633303232, // 22036
    non_unique_keys_in_a_json_object = 0x3733303232, // 22037
    singleton_sql_json_item_required = 0x3833303232, // 22038
    sql_json_array_not_found = 0x3933303232, // 22039
    sql_json_member_not_found = 0x4133303232, // 2203A
    sql_json_number_not_found = 0x4233303232, // 2203B
    sql_json_object_not_found = 0x4333303232, // 2203C
    too_many_json_array_elements = 0x4433303232, // 2203D
    too_many_json_object_members = 0x4533303232, // 2203E
    sql_json_scalar_required = 0x4633303232, // 2203F
    integrity_constraint_violation = 0x3030303332, // 23000
    restrict_violation = 0x3130303332, // 23001
    not_null_violation = 0x3230353332, // 23502
    foreign_key_violation = 0x3330353332, // 23503
    unique_violation = 0x3530353332, // 23505
    check_violation = 0x3431353332, // 23514
    exclusion_violation = 0x3130503332, // 23P01
    invalid_cursor_state = 0x3030303432, // 24000
    invalid_transaction_state = 0x3030303532, // 25000
    active_sql_transaction = 0x3130303532, // 25001
    branch_transaction_already_active = 0x3230303532, // 25002
    held_cursor_requires_same_isolation_level = 0x3830303532, // 25008
    inappropriate_access_mode_for_branch_transaction = 0x3330303532, // 25003
    inappropriate_isolation_level_for_branch_transaction = 0x3430303532, // 25004
    no_active_sql_transaction_for_branch_transaction = 0x3530303532, // 25005
    read_only_sql_transaction = 0x3630303532, // 25006
    schema_and_data_statement_mixing_not_supported = 0x3730303532, // 25007
    no_active_sql_transaction = 0x3130503532, // 25P01
    in_failed_sql_transaction = 0x3230503532, // 25P02
    idle_in_transaction_session_timeout = 0x3330503532, // 25P03
    invalid_sql_statement_name = 0x3030303632, // 26000
    triggered_data_change_violation = 0x3030303732, // 27000
    invalid_authorization_specification = 0x3030303832, // 28000
    invalid_password = 0x3130503832, // 28P01
    dependent_privilege_descriptors_still_exist = 0x3030304232, // 2B000
    dependent_objects_still_exist = 0x3130504232, // 2BP01
    invalid_transaction_termination = 0x3030304432, // 2D000
    sql_routine_exception = 0x3030304632, // 2F000
    function_executed_no_return_statement = 0x3530304632, // 2F005
    modifying_sql_data_not_permitted = 0x3230304632, // 2F002
    prohibited_sql_statement_attempted = 0x3330304632, // 2F003
    reading_sql_data_not_permitted = 0x3430304632, // 2F004
    invalid_cursor_name = 0x3030303433, // 34000
    external_routine_exception = 0x3030303833, // 38000
    containing_sql_not_permitted = 0x3130303833, // 38001
    //    modifying_sql_data_not_permitted = 0x3230303833, // 38002
    //    prohibited_sql_statement_attempted = 0x3330303833, // 38003
    //    reading_sql_data_not_permitted = 0x3430303833, // 38004
    external_routine_invocation_exception = 0x3030303933, // 39000
    invalid_sqlstate_returned = 0x3130303933, // 39001
    //    null_value_not_allowed = 0x3430303933, // 39004
    trigger_protocol_violated = 0x3130503933, // 39P01
    srf_protocol_violated = 0x3230503933, // 39P02
    event_trigger_protocol_violated = 0x3330503933, // 39P03
    savepoint_exception = 0x3030304233, // 3B000
    invalid_savepoint_specification = 0x3130304233, // 3B001
    invalid_catalog_name = 0x3030304433, // 3D000
    invalid_schema_name = 0x3030304633, // 3F000
    transaction_rollback = 0x3030303034, // 40000
    transaction_integrity_constraint_violation = 0x3230303034, // 40002
    serialization_failure = 0x3130303034, // 40001
    statement_completion_unknown = 0x3330303034, // 40003
    deadlock_detected = 0x3130503034, // 40P01
    syntax_error_or_access_rule_violation = 0x3030303234, // 42000
    syntax_error = 0x3130363234, // 42601
    insufficient_privilege = 0x3130353234, // 42501
    cannot_coerce = 0x3634383234, // 42846
    grouping_error = 0x3330383234, // 42803
    windowing_error = 0x3032503234, // 42P20
    invalid_recursion = 0x3931503234, // 42P19
    invalid_foreign_key = 0x3033383234, // 42830
    invalid_name = 0x3230363234, // 42602
    name_too_long = 0x3232363234, // 42622
    reserved_name = 0x3933393234, // 42939
    datatype_mismatch = 0x3430383234, // 42804
    indeterminate_datatype = 0x3831503234, // 42P18
    collation_mismatch = 0x3132503234, // 42P21
    indeterminate_collation = 0x3232503234, // 42P22
    wrong_object_type = 0x3930383234, // 42809
    generated_always = 0x3943383234, // 428C9
    undefined_column = 0x3330373234, // 42703
    undefined_function = 0x3338383234, // 42883
    undefined_table = 0x3130503234, // 42P01
    undefined_parameter = 0x3230503234, // 42P02
    undefined_object = 0x3430373234, // 42704
    duplicate_column = 0x3130373234, // 42701
    duplicate_cursor = 0x3330503234, // 42P03
    duplicate_database = 0x3430503234, // 42P04
    duplicate_function = 0x3332373234, // 42723
    duplicate_prepared_statement = 0x3530503234, // 42P05
    duplicate_schema = 0x3630503234, // 42P06
    duplicate_table = 0x3730503234, // 42P07
    duplicate_alias = 0x3231373234, // 42712
    duplicate_object = 0x3031373234, // 42710
    ambiguous_column = 0x3230373234, // 42702
    ambiguous_function = 0x3532373234, // 42725
    ambiguous_parameter = 0x3830503234, // 42P08
    ambiguous_alias = 0x3930503234, // 42P09
    invalid_column_reference = 0x3031503234, // 42P10
    invalid_column_definition = 0x3131363234, // 42611
    invalid_cursor_definition = 0x3131503234, // 42P11
    invalid_database_definition = 0x3231503234, // 42P12
    invalid_function_definition = 0x3331503234, // 42P13
    invalid_prepared_statement_definition = 0x3431503234, // 42P14
    invalid_schema_definition = 0x3531503234, // 42P15
    invalid_table_definition = 0x3631503234, // 42P16
    invalid_object_definition = 0x3731503234, // 42P17
    with_check_option_violation = 0x3030303434, // 44000
    insufficient_resources = 0x3030303335, // 53000
    disk_full = 0x3030313335, // 53100
    out_of_memory = 0x3030323335, // 53200
    too_many_connections = 0x3030333335, // 53300
    configuration_limit_exceeded = 0x3030343335, // 53400
    program_limit_exceeded = 0x3030303435, // 54000
    statement_too_complex = 0x3130303435, // 54001
    too_many_columns = 0x3131303435, // 54011
    too_many_arguments = 0x3332303435, // 54023
    object_not_in_prerequisite_state = 0x3030303535, // 55000
    object_in_use = 0x3630303535, // 55006
    cant_change_runtime_param = 0x3230503535, // 55P02
    lock_not_available = 0x3330503535, // 55P03
    unsafe_new_enum_value_usage = 0x3430503535, // 55P04
    operator_intervention = 0x3030303735, // 57000
    query_canceled = 0x3431303735, // 57014
    admin_shutdown = 0x3130503735, // 57P01
    crash_shutdown = 0x3230503735, // 57P02
    cannot_connect_now = 0x3330503735, // 57P03
    database_dropped = 0x3430503735, // 57P04
    idle_session_timeout = 0x3530503735, // 57P05
    system_error = 0x3030303835, // 58000
    io_error = 0x3033303835, // 58030
    undefined_file = 0x3130503835, // 58P01
    duplicate_file = 0x3230503835, // 58P02
    snapshot_too_old = 0x3030303237, // 72000
    config_file_error = 0x3030303046, // F0000
    lock_file_exists = 0x3130303046, // F0001
    fdw_error = 0x3030305648, // HV000
    fdw_column_name_not_found = 0x3530305648, // HV005
    fdw_dynamic_parameter_value_needed = 0x3230305648, // HV002
    fdw_function_sequence_error = 0x3031305648, // HV010
    fdw_inconsistent_descriptor_information = 0x3132305648, // HV021
    fdw_invalid_attribute_value = 0x3432305648, // HV024
    fdw_invalid_column_name = 0x3730305648, // HV007
    fdw_invalid_column_number = 0x3830305648, // HV008
    fdw_invalid_data_type = 0x3430305648, // HV004
    fdw_invalid_data_type_descriptors = 0x3630305648, // HV006
    fdw_invalid_descriptor_field_identifier = 0x3139305648, // HV091
    fdw_invalid_handle = 0x4230305648, // HV00B
    fdw_invalid_option_index = 0x4330305648, // HV00C
    fdw_invalid_option_name = 0x4430305648, // HV00D
    fdw_invalid_string_length_or_buffer_length = 0x3039305648, // HV090
    fdw_invalid_string_format = 0x4130305648, // HV00A
    fdw_invalid_use_of_null_pointer = 0x3930305648, // HV009
    fdw_too_many_handles = 0x3431305648, // HV014
    fdw_out_of_memory = 0x3130305648, // HV001
    fdw_no_schemas = 0x5030305648, // HV00P
    fdw_option_name_not_found = 0x4a30305648, // HV00J
    fdw_reply_handle = 0x4b30305648, // HV00K
    fdw_schema_not_found = 0x5130305648, // HV00Q
    fdw_table_not_found = 0x5230305648, // HV00R
    fdw_unable_to_create_execution = 0x4c30305648, // HV00L
    fdw_unable_to_create_reply = 0x4d30305648, // HV00M
    fdw_unable_to_establish_connection = 0x4e30305648, // HV00N
    plpgsql_error = 0x3030303050, // P0000
    raise_exception = 0x3130303050, // P0001
    no_data_found = 0x3230303050, // P0002
    too_many_rows = 0x3330303050, // P0003
    assert_failure = 0x3430303050, // P0004
    internal_error = 0x3030305858, // XX000
    data_corrupted = 0x3130305858, // XX001
    index_corrupted = 0x3230305858, // XX002
    _,
};

pub const ErrorInfo = struct {
    code: ErrorCode,
    message: ?[]const u8 = null,

    pub fn format(
        self: ErrorInfo,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        out_stream: anytype,
    ) !void {
        _ = fmt;
        _ = options;
        try std.fmt.format(out_stream, "ErrorInfo{{ .code='{s}' .message='{s}' }}", .{ self.code, self.message });
    }
};

pub const Event = enum(u8) {
    // ------------------------------------------------------------------------
    // Startup phase
    // ------------------------------------------------------------------------
    // This message provides secret-key data that the frontend must save if it
    // wants to be able to issue cancel requests later. The frontend should not
    // respond to this message, but should continue listening for a
    // ReadyForQuery message.
    BackendKeyData = 'K',

    // This message informs the frontend about the current (initial) setting of
    // backend parameters, such as client_encoding or DateStyle. The frontend
    // can ignore this message, or record the settings for its future use;
    // see Section 53.2.6 for more details. The frontend should not respond to
    // this message, but should continue listening for a ReadyForQuery message.
    ParameterStatus = 'S',

    // Start-up is completed. The frontend can now issue commands.
    ReadyForQuery = 'Z',

    // Start-up failed. The connection is closed after sending this message.
    ErrorResponse = 'E',

    // A warning message has been issued. The frontend should display the
    // message but continue listening for ReadyForQuery or ErrorResponse.
    NoticeResponse = 'N',

    // ------------------------------------------------------------------------
    // Simple query
    // ------------------------------------------------------------------------
    // An SQL command completed normally.
    CommandComplete = 'C',

    // The backend is ready to copy data from the frontend to a table;
    // see Section 53.2.5.
    CopyInResponse = 'G',

    // The backend is ready to copy data from a table to the frontend;
    // see Section 53.2.5.
    CopyOutResponse = 'H',

    // Indicates that rows are about to be returned in response to a SELECT,
    // FETCH, etc query. The contents of this message describe the column
    // layout of the rows. This will be followed by a DataRow message for each
    // row being returned to the frontend.
    RowDescription = 'T',

    // One of the set of rows returned by a SELECT, FETCH, etc query.
    DataRow = 'D',

    // An empty query string was recognized.
    EmptyQueryResponse = 'I',
};

pub const Message = struct {
    data: []const u8 = "",
    len: i32 = 0,
    code: u8 = 0,

    pub fn format(
        self: Message,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        out_stream: anytype,
    ) !void {
        _ = fmt;
        _ = options;
        try std.fmt.format(out_stream, "Message{{ .code='{c}' .len={d}, .data='{s}' }}", .{ self.code, self.len, self.data });
    }
};

// See
// https://www.postgresql.org/docs/current/protocol-overview.html
// https://www.postgresql.org/docs/current/protocol-message-formats.html
pub const BackendParameters = struct {
    // Fixed storage space for each string
    storage: [10][64]u8 = undefined,

    application_name: []const u8 = "",
    client_encoding: []const u8 = "UTF8",
    server_encoding: []const u8 = "UTF8",
    server_version: []const u8 = "",
    datestyle: []const u8 = "ISO, MDY",
    intervalstyle: []const u8 = "postgres",
    session_authorization: []const u8 = "",
    timezone: []const u8 = "America/New_York",
    is_superuser: bool = false,
    integer_datetimes: bool = true,
    standard_conforming_strings: bool = true,
    process_id: i32 = 0,
    secret_key: i32 = 0,
};

pub const Protocol = struct {
    const Self = @This();
    stream: std.net.Stream,
    in_transaction: bool = false,
    // Buffer for parameters
    backend: BackendParameters = BackendParameters{},
    object_id: ?u32 = null, // Last object ID
    rows: usize = 0, // Rows from last complete
    error_buffer: [256]u8 = undefined, // Storage for error info
    error_info: ?ErrorInfo = null,

    pub fn init(params: Params) !Self {
        const stream = params.stream;
        var protocol = Self{
            .stream = stream,
        };
        try protocol.sendStartup(params);
        protocol.handleStartup(params) catch |err| switch (err) {
            error.EndOfStream => return error.PostgresConnectionError,
            else => return err,
        };
        return protocol;
    }

    // Send startup message
    pub fn sendStartup(self: *Self, params: Params) !void {
        var buffer: [1024]u8 = undefined;
        var buf = std.io.fixedBufferStream(&buffer);
        var msg = buf.writer();
        try msg.writeIntBig(i32, 0);
        try msg.writeIntBig(i32, 196608); // version
        try msg.writeAll("user");
        try msg.writeByte(0);
        try msg.writeAll(params.user);
        try msg.writeByte(0);
        if (params.db.len > 0) {
            try msg.writeAll("database");
            try msg.writeByte(0);
            try msg.writeAll(params.db);
            try msg.writeByte(0);
        }
        if (params.options) |options| {
            for (options) |option| {
                try msg.writeAll(option.name);
                try msg.writeByte(0);
                try msg.writeAll(option.value);
                try msg.writeByte(0);
            }
        }
        try msg.writeAll("replication");
        try msg.writeByte(0);
        try msg.writeAll(params.replication);
        try msg.writeByte(0);
        try msg.writeByte(0); // End
        const n = buf.pos;
        buf.pos = 0; // Update length
        try msg.writeIntBig(i32, @intCast(i32, n));
        //std.log.debug("Startup message: {c}\n", .{buffer[0..n]});
        try self.stream.writer().writeAll(buffer[0..n]);
    }

    pub fn handleStartup(self: *Self, params: Params) !void {
        var input_buffer: [1024]u8 = undefined;
        var output_buffer: [1024]u8 = undefined;

        while (true) {
            const resp = try self.readMessage(&input_buffer);
            // std.log.debug("Read: {s}\n", .{resp});
            try self.processStartupMessage(resp, &output_buffer, params);
            if (resp.code == 'Z') break; // Ready
        }
        //std.log.info("Connected to database!", .{});
    }

    pub fn readMessage(self: Self, buffer: []u8) !Message {
        const stream = self.stream.reader();
        const code = try stream.readIntBig(u8);
        const len = try stream.readIntBig(i32);
        if (len < 4) {
            return error.DatabaseMessageInvalid; // Message length bad
        }
        const size = @intCast(usize, len - 4);
        if (size > buffer.len) {
            return error.DatabaseMessageInvalid; // Not enough room in buffer
        }
        const data = buffer[0..size];
        try stream.readNoEof(data);
        return Message{ .code = code, .len = len, .data = data };
    }

    // Send a message already preparted
    pub fn sendCompleteMessage(self: Self, data: []const u8) !usize {
        const stream = self.stream.writer();
        // std.log.debug("Send: {c}\n", .{data});
        try stream.writeAll(data);
        return data.len;
    }

    // Send a message, copying first into a buffer
    pub fn sendMessage(self: Self, output_buffer: []u8, code: u8, data: []const u8) !usize {
        var fbo = std.io.fixedBufferStream(output_buffer);
        const buf = fbo.writer();
        try buf.writeIntBig(u8, code);
        try buf.writeIntBig(i32, @intCast(i32, data.len + 5));
        try buf.writeAll(data);
        try buf.writeByte(0);
        return self.sendCompleteMessage(fbo.getWritten());
    }

    pub fn sendCommand(self: Self, output_buffer: []u8, code: u8) !usize {
        var fbo = std.io.fixedBufferStream(output_buffer);
        const buf = fbo.writer();
        try buf.writeIntBig(u8, code);
        try buf.writeIntBig(i32, 4);
        return self.sendCompleteMessage(fbo.getWritten());
    }

    // Process a message
    pub fn processStartupMessage(self: *Self, msg: Message, storage: []u8, params: Params) !void {
        // The possible messages from the backend in this phase are
        return switch (msg.code) {
            'S' => self.handleParameterStatus(msg),
            'R' => self.handleAuthRequest(msg, storage, params),
            'K' => self.handleBackendKeyData(msg),
            'Z' => self.handleReadyForQuery(msg),
            'N' => self.handleNoticeResponse(msg),
            'E' => self.handleErrorResponse(msg),
            else => error.UnhandledPostgresCode,
        };
    }

    pub fn handleQueryMessages(self: *Self, output_buffer: []u8) !void {
        while (true) {
            const resp = try self.readMessage(output_buffer);
            //std.log.debug("Read: {s}", .{resp});
            try self.processMessage(resp, output_buffer);
            if (resp.code == 'Z') break; // Ready for next query
        }
    }

    // Process a message
    //     pub fn processSimpleQueryMessage(self: *Self, msg: Message) !void {
    //         return switch (msg.code) {
    //             'D' => self.handleRowData(cursor),
    //             'T' => self.handleRowDesc(cursor),
    //             'n' => self.handleNoData(cursor),
    //             'C' => self.handleCommandComplete(cursor),
    //             '1' => self.handleParseComplete(cursor),
    //             '2' => self.handleBindComplete(cursor),
    //             '3' => self.handleCloseComplete(cursor),
    //             'c' => self.handleCopyDone(cursor),
    //             'd' => self.handleCopyData(cursor),
    //             'Z' => self.handleReadyForQuery(msg),
    //             'N' => self.handleNoticeResponse(msg),
    //             'I' => self.handleEmptyQueryResponse(msg),
    //             'A' => self.handleNotificationResponse(msg),
    //             'E' => error.PostgresError, // See buffer for message
    //             else => error.UnhandledPostgresCode,
    //         };
    //     }

    pub fn handleReadyForQuery(self: *Self, msg: Message) !void {
        // Z
        if (msg.data.len > 0) {
            self.in_transaction = msg.data[0] != 'I';
        }
        //std.log.info("ReadyForQuery: {s}", .{msg});
    }

    pub fn handleNoticeResponse(self: *Self, msg: Message) !void {
        // N
        _ = self;
        std.log.warn("NOTICE: {s}", .{msg.data});
    }

    pub fn handleNotificationResponse(self: *Self, msg: Message) !void {
        // N
        _ = self;
        std.log.info("NotificationResponse: {s}", .{msg});
    }

    pub fn handleAuthRequest(self: *Self, msg: Message, storage: []u8, params: Params) !void {
        // R
        const reader = std.io.fixedBufferStream(msg.data).reader();
        const auth_type = try reader.readIntBig(u32);
        switch (auth_type) {
            0 => {
                // Auth success!
                //std.log.debug("Logged in!", .{});
            },
            3 => {
                // Cleartext
                if (params.pass.len == 0) {
                    std.log.warn("Server requested cleartext auth but no password was given", .{});
                    return error.PostgresAuthFailed;
                }
                _ = try self.sendMessage(storage, 'p', params.pass);
            },
            5 => {
                // MD5
                // Specifies that an MD5-encrypted password is required.
                if (params.pass.len == 0) {
                    std.log.warn("Server requested MD5 auth but no password was given", .{});
                    return error.PostgresAuthFailed;
                }
                var fbo = std.io.fixedBufferStream(storage);
                const buf = fbo.writer();
                try buf.writeIntBig(u8, 'p'); // Code
                try buf.writeIntBig(i32, 32 + 3 + 5); // Length
                try buf.writeAll("md5"); // Prefix

                // Write hash directly to output buf
                const charset = "0123456789abcdef";
                const salt = try reader.readBytesNoEof(4);
                var hash: [16]u8 = undefined;
                var h = Md5.init(.{});
                h.update(params.pass);
                h.update(params.user);
                h.final(&hash);
                h = Md5.init(.{});
                for (hash) |c| {
                    const tmp = [2]u8{ charset[c >> 4], charset[c & 15] };
                    h.update(&tmp);
                }
                h.update(&salt);
                h.final(&hash);
                for (hash) |c| {
                    const tmp = [2]u8{ charset[c >> 4], charset[c & 15] };
                    try buf.writeAll(&tmp);
                }
                try buf.writeByte(0);
                _ = try self.sendCompleteMessage(fbo.getWritten());
            },
            // TODO: Others
            else => return error.PostgresAuthTypeUnsupported,
        }
    }

    // S - Identifies the message as a run-time parameter status report.
    pub fn handleParameterStatus(self: *Self, msg: Message) !void {
        //std.log.info("ParameterStatus: {s}", .{msg});
        if (mem.indexOfScalar(u8, msg.data, 0)) |i| {
            const parameter = msg.data[0..i];
            const j = i + 1;
            const k = msg.data.len - 1;
            const value = msg.data[j..k];
            // NOTE: All strings fields must come first so the index matches the
            // index in the storage array
            const parameters = [_][]const u8{
                "application_name",
                "client_encoding",
                "server_encoding",
                "server_version",
                "datestyle",
                "intervalstyle",
                "session_authorization",
                "timezone",
                // Bools
                "is_superuser",
                "integer_datetimes",
                "standard_conforming_strings",
            };
            inline for (parameters) |name, index| {
                if (ascii.eqlIgnoreCase(parameter, name)) {
                    //const f = std.meta.fieldInfo(BackendParameters, name);
                    const T = @typeInfo(@TypeOf(@field(self.backend, name)));
                    switch (T) {
                        .Pointer => |ptr_info| {
                            if (ptr_info.size != .Slice) {
                                @compileError("Unsupported backedn field " ++ name);
                            }
                            // Copy value into buffer
                            const buf = self.backend.storage[index][0..];
                            mem.copy(u8, buf, value);
                            @field(self.backend, name) = buf[0..value.len];
                        },
                        .Bool => {
                            @field(self.backend, name) = mem.eql(u8, value, "on");
                        },
                        else => @compileError("Unhandled field " ++ name),
                    }
                    //std.log.info("Updated parameter {s}: {s}", .{parameter, value});
                    return;
                }
            }
            std.log.warn("Unhandled parameter {s}: {s}", .{ parameter, value });
        }
    }

    pub fn handleParameterDescription(self: *Self, msg: Message) !void {
        // S
        _ = self;
        std.log.info("ParameterDesc: {s}", .{msg});
    }

    // K - Identifies the message as cancellation key data. The frontend must
    // save these values if it wishes to be able to issue CancelRequest
    // messages later.
    pub fn handleBackendKeyData(self: *Self, msg: Message) !void {
        const reader = std.io.fixedBufferStream(msg.data).reader();
        self.backend.process_id = try reader.readIntBig(i32);
        self.backend.secret_key = try reader.readIntBig(i32);
        // std.log.info("BackendKeyData: {s}", .{msg});
    }

    pub fn handleRowData(self: *Self, msg: Message) !void {
        // D
        _ = self;
        std.log.info("RowData: {s}", .{msg});
    }

    pub fn handleRowDesc(self: Self, msg: Message) !void {
        // T
        _ = self;
        //const reader = std.io.fixedBufferStream(msg.data).reader();
        //const num_fields = try reader.readIntBig(i16);

        std.log.info("RowDesc: {s}", .{msg});
    }

    pub fn handleNoData(self: *Self, msg: Message) !void {
        // n
        _ = self;
        std.log.info("NoData: {s}", .{msg});
    }

    pub fn handleCommandComplete(self: *Self, msg: Message) !void {
        // C
        //std.log.info("CommandComplete: {s}", .{msg});
        if (msg.data.len < 6) return; // Bounds check

        // Extract row count and object id
        const statement = @bitCast(u32, msg.data[0..4].*);
        switch (statement) {
            INSERT, DELETE, UPDATE, SELECT, MOVE, FETCH, COPY => {
                if (mem.lastIndexOfScalar(u8, msg.data, ' ')) |i| {
                    const end = msg.data.len - 1; // Remove null at end
                    const n = i + 1;
                    if (n >= end) return; // Empty
                    const buf = msg.data[n..end];
                    self.rows = try std.fmt.parseInt(usize, buf, 10);

                    if (statement == INSERT) {
                        const remaining = msg.data[0..i];
                        if (mem.lastIndexOfScalar(u8, remaining, ' ')) |j| {
                            const k = j + 1;
                            if (k >= n) return; // Empty
                            self.object_id = try std.fmt.parseInt(u32, remaining[k..], 10);
                        }
                    }
                }
            },
            else => {},
        }
    }

    pub fn handleParseComplete(self: *Self, msg: Message) !void {
        // 1
        _ = self;
        std.log.info("ParseComplete: {s}", .{msg});
    }

    pub fn handleBindComplete(self: *Self, msg: Message) !void {
        // 2
        _ = self;
        std.log.info("BindComplete: {s}", .{msg});
    }

    pub fn handleCloseComplete(self: *Self, msg: Message) !void {
        // 3
        _ = self;
        std.log.info("CloseComplete: {s}", .{msg});
    }

    pub fn handleSuspended(self: *Self, msg: Message) !void {
        // s
        _ = self;
        std.log.info("Suspended: {s}", .{msg});
    }

    pub fn handleCopyDone(self: *Self, msg: Message) !void {
        // s
        _ = self;
        std.log.info("CopyDone: {s}", .{msg});
    }

    pub fn handleCopyData(self: *Self, msg: Message) !void {
        // s
        _ = self;
        std.log.info("CopyData: {s}", .{msg});
    }

    pub fn handleEmptyQueryResponse(self: *Self, msg: Message) !void {
        // I
        _ = self;
        _ = msg; // Do nothing
    }

    pub fn handleErrorResponse(self: *Self, msg: Message) !void {
        // E
        const reader = std.io.fixedBufferStream(msg.data).reader();
        const e = try reader.readIntBig(u8);
        var error_message: ?[]const u8 = null;

        // Copy error message into protocol's error buffer
        if (e != 0) {
            const n = try reader.read(&self.error_buffer);
            error_message = self.error_buffer[0..n];
        }

        var code: ErrorCode = .unknown;
        if (error_message) |data| {
            if (mem.indexOf(u8, data, &[_]u8{ 0, 'C' })) |c| {
                const end = c + 8;
                if (end < data.len) { // Check length
                    const buf = data[c..end];
                    // This captures { , C, 3, D, 0, 0, 0,  }
                    // so shift 16 remove the null byte and 'C' then &
                    // to remove the top null (not really necessary but just in case)
                    code = @intToEnum(ErrorCode, @bitCast(u64, buf[0..8].*) >> 16);
                }
            }
            if (mem.indexOf(u8, data, &[_]u8{ 0, 'M' })) |c| {
                const start = c + 2;
                if (mem.indexOfScalarPos(u8, data, start, 0)) |end| {
                    error_message = data[start..end];
                }
            }
        }

        std.log.err("{} - {s}", .{ code, error_message });
        // Save error info
        self.error_info = ErrorInfo{
            .code = code,
            .message = error_message,
        };
        return switch (code) {
            .invalid_catalog_name => error.DatabaseDoesNotExist,
            .undefined_table => error.TableDoesNotExist,
            .undefined_column => return error.ColumnDoesNotExist,
            else => error.DatabaseErrorResponse, // Unknown error, see buffer for message
        };
    }

    pub fn errorCode(self: *Self) ?ErrorCode {
        return if (self.error_info) |info| info.code else null;
    }

    pub fn errorMessage(self: *Self) ?[]const u8 {
        return if (self.error_info) |info| info.message else null;
    }

    pub fn terminate(self: *Self) !void {
        try self.sendCommand('X');
    }

    pub fn sync(self: *Self) !void {
        try self.sendCommand('S');
    }

    pub fn close(self: *Self) void {
        // May be closed due to an error
        if (self.stream.handle > 0) {
            self.stream.close();
        }
    }
};

pub fn createColumn(
    comptime T: type,
    comptime table_name: []const u8,
    comptime column: Column,
    comptime field_type: type,
) []const u8 {
    comptime {
        var sql: []const u8 = comptimePrint("{s} {s}", .{
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
                []u8, []const u8 => if (column.len > 0)
                    comptimePrint("varchar({d})", .{column.len})
                else
                    "text",
                // TODO: The rest
                else => |v| blk: {
                    switch (@typeInfo(v)) {
                        .Array => |info| switch(info.child) {
                            u8 => break :blk comptimePrint("varchar({d})", .{info.len}),
                            else => {},
                        },
                        else => {},
                    }

                    @compileError(
                        comptimePrint("Cannot generate table column for field `{s}: {s}` of '{s}` (type {s})", .{
                            column.field, @typeName(field_type), @typeName(T), v
                        })
                    );
                }
            },
        });

        if (column.pk) {
            sql = sql ++ " PRIMARY KEY";
            if (column.autoincrement) {
                sql = sql ++ comptimePrint(" DEFAULT nextval('{s}_{s}_seq')", .{
                    table_name,
                    column.name,
                });
            }
        } else {
            if (!column.optional) {
                sql = sql ++ " NOT NULL";
            }
            if (column.unique) {
                sql = sql ++ " UNIQUE";
            }
            if (column.default) |v| {
                //@compileLog(v);
                sql = sql ++ comptimePrint(" DEFAULT {s}", .{v});
            }
        }
        return sql;
    }
}

pub fn createTable(
    comptime table_name: []const u8,
    comptime columns: []const Column,
    comptime options: TableOptions,
) []const u8 {
    comptime var sql: []const u8 = "";

    // Create sequence for any auto increment fields
    inline for (columns) |col| {
        if (col.autoincrement) {
            sql = sql ++ comptimePrint("CREATE SEQUENCE {s}_{s}_seq;\n", .{
                table_name, col.name,
            });
        }
    }

    // Same for all
    sql = sql ++ comptimePrint("CREATE TABLE {s} (\n", .{table_name});

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

    sql = sql ++ ");\n";
    inline for (columns) |col| {
        if (col.autoincrement) {
            sql = sql ++ comptimePrint("ALTER SEQUENCE {s}_{s}_seq OWNED BY {s}.{s};\n", .{
                table_name, col.name,
                table_name, col.name,
            });
        }
    }
    return sql[0 .. sql.len - 1]; // Strip last newline

}
