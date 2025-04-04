
mg_endpoint_server :: struct {
    mgr: mg_mgr;
    endpoints: [mg_method_count] [..] mg_endpoint;
    headers: [] [2] *u8;
    poll_thread: *Thread;
    poll_thread_alive: bool;
    alive: bool;
    ctx: *#Context;
    data: *void;
    listen_data: *void;
}

mg_endpoint :: struct {
    path: string;
    cb: mg_endpoint_callback_data;
}

mg_request :: struct {
    hm: *mg_http_message;
}

mg_response :: struct {
    rem: mg_addr;
    headers: [] [2] *u8;
}

mg_result :: struct {
    status: s32;
    header: *u8;
    body: *u8;
}

mg_method :: enum {
    Connect;
    Delete;
    Get;
    Head;
    Options;
    Patch;
    Post;
    Put;
    Trace;
    Fallback;
}

mg_endpoint_callback :: #type (request: *mg_request, response: *mg_response) -> mg_result;
mg_endpoint_callback_data :: #type (request: *mg_request, response: *mg_response, data: *void) -> mg_result;
mg_message_callback :: #type (cb: mg_endpoint_callback_data, conn: *mg_connection, request: *mg_request, response: *mg_response, data: *void, listen_data: *void);
mg_poll_callback :: #type (conn: *mg_connection, listen_data: *void);
mg_close_callback :: #type (conn: *mg_connection, listen_data: *void);
mg_write_callback :: #type (conn: *mg_connection, listen_data: *void);
mg_error_callback :: #type (conn: *mg_connection, listen_data: *void);

mg_endpoint_server_create :: (data: *void = null) -> *mg_endpoint_server {
    server := New(mg_endpoint_server);
    server.alive = true;
    server.ctx = *context;
    server.data = data;

    return server;
}

mg_endpoint_server_listen :: (server: *mg_endpoint_server, port: u16) {
    process_message :: (cb: mg_endpoint_callback_data, conn: *mg_connection, request: *mg_request, response: *mg_response, data: *void, listen_data: *void) {
        reply := cb(request, response, data);
        mg_http_reply_CFormat(conn, reply.status, reply.header, "%s", reply.body);
        mg_free(reply);

        set_connection_state(conn, .is_draining, true);
    }

    mg_endpoint_server_listen(server, port, process_message, null, null, null, null, null);
}

mg_endpoint_server_listen :: (server: *mg_endpoint_server, port: u16, $process_message: mg_message_callback, $process_poll: mg_poll_callback, $process_write: mg_write_callback, $process_close: mg_close_callback, $process_error: mg_error_callback, listen_data: *void) {
    mg_callback :: (conn: *mg_connection, ev: s32, ev_data: *void) #c_call {
        // guessing at fn_data since the update
        server := cast(*mg_endpoint_server) conn.fn_data;
        push_context server.ctx.* {
            // MG_EV_OPEN
            // MG_EV_ACCEPT
            // MG_EV_READ
            // MG_EV_HTTP_MSG
            // MG_EV_WRITE

            if cast(MG_EV) ev == .HTTP_MSG {
                hm := cast(*mg_http_message) ev_data;
                method := mg_get_method(hm);

                endpoints := server.endpoints[method];
                for entry: endpoints {
                    if mg_match(hm.uri, to_mg_str(entry.path), null) {
                        request := mg_request.{hm};
                        response := mg_response.{conn.rem, server.headers};

                        #if process_message then process_message(entry.cb, conn, *request, *response, server.data, server.listen_data);

                        reset_temporary_storage();

                        return;
                    }
                }

                fallback_endpoints := server.endpoints[mg_method.Fallback];
                for entry: fallback_endpoints {
                    if mg_match(hm.uri, to_mg_str(entry.path), null) {
                        request := mg_request.{hm};
                        response := mg_response.{conn.rem, server.headers};

                        #if process_message then process_message(entry.cb, conn, *request, *response, server.data, server.listen_data);

                        reset_temporary_storage();

                        return;
                    }
                }

                for entry: fallback_endpoints {
                    if entry.path == "*" {
                        request := mg_request.{hm};
                        response := mg_response.{conn.rem, server.headers};

                        #if process_message then process_message(entry.cb, conn, *request, *response, server.data, server.listen_data);

                        reset_temporary_storage();

                        return;
                    }
                }

                mg_http_reply(conn, 404, null, "");
            } else if cast(MG_EV) ev == .POLL {
                #if process_poll then process_poll(conn, server.listen_data);

                reset_temporary_storage();
            } else if cast(MG_EV) ev == .WRITE {
                #if process_write then process_write(conn, server.listen_data);

                reset_temporary_storage();
            } else if cast(MG_EV) ev == .CLOSE {
                #if process_close then process_close(conn, server.listen_data);

                reset_temporary_storage();
            } else if cast(MG_EV) ev == .READ {
            } else if cast(MG_EV) ev == .ERROR {
                #if process_error then process_error(conn, server.listen_data);

                reset_temporary_storage();
            }
        }
    }

    url := sprint("http://0.0.0.0:%", port);
    defer free(url);
    url_c := to_c_string(url);
    defer free(url_c);

    server.listen_data = listen_data;

    mg_mgr_init(*server.mgr);
    mg_http_listen(*server.mgr, url_c, mg_callback, server);
}

mg_endpoint_server_close :: (server: *mg_endpoint_server) {
    server.alive = false;
    while server.poll_thread_alive {
        sleep_milliseconds(1);
    }
    // definitely not right yet
}

mg_endpoint_server_free :: (server: mg_endpoint_server) {
    if server.poll_thread {
        thread_deinit(server.poll_thread);
        free(server.poll_thread);
    }

    mg_mgr_free(*server.mgr);
    for endpoints: server.endpoints {
        array_free(endpoints);
    }
}

mg_endpoint_server_free :: (server: *mg_endpoint_server) {
    mg_endpoint_server_free(server.*);
    free(server);
}

mg_add_endpoint :: (server: *mg_endpoint_server, method: mg_method, $path: string, cb: mg_endpoint_callback_data) {
    entry: mg_endpoint = ---;
    entry.path = path;
    entry.cb = cb;

    array_add(*server.endpoints[method], entry);
}

mg_add_endpoint :: (server: *mg_endpoint_server, method: mg_method, $path: string, $cb: mg_endpoint_callback) {
    baked_cb :: (request: *mg_request, response: *mg_response, data: *void) -> mg_result {
        return inline cb(request, response);
    }

    mg_add_endpoint(server, method, path, baked_cb);
}

mg_reply :: (response: *mg_response, status: s32) -> mg_result {
    return mg_reply(response, status, .[], "");
}

mg_reply :: (response: *mg_response, status: s32, headers: [] [2] *u8, body: string) -> mg_result {
    header_size := 0;
    for header: response.headers {
        header_size += c_style_strlen(header[0]) + 2 + c_style_strlen(header[1]) + 2;
    }
    for header: headers {
        header_size += c_style_strlen(header[0]) + 2 + c_style_strlen(header[1]) + 2;
    }

    header_string_written := 0;
    header_string := alloc_string(header_size + 1);

    for header: response.headers {
        length := c_style_strlen(header[0]);
        memcpy(header_string.data + header_string_written, header[0], length);
        header_string_written += length;

        header_string[header_string_written] = #char ":";
        header_string[header_string_written + 1] = #char " ";
        header_string_written += 2;

        length = c_style_strlen(header[1]);
        memcpy(header_string.data + header_string_written, header[1], length);
        header_string_written += length;

        header_string[header_string_written] = #char "\r";
        header_string[header_string_written + 1] = #char "\n";
        header_string_written += 2;
    }
    for header: headers {
        length := c_style_strlen(header[0]);
        memcpy(header_string.data + header_string_written, header[0], length);
        header_string_written += length;

        header_string[header_string_written] = #char ":";
        header_string[header_string_written + 1] = #char " ";
        header_string_written += 2;

        length = c_style_strlen(header[1]);
        memcpy(header_string.data + header_string_written, header[1], length);
        header_string_written += length;

        header_string[header_string_written] = #char "\r";
        header_string[header_string_written + 1] = #char "\n";
        header_string_written += 2;
    }

    header_string[header_string_written] = #char "\0";

    body_size := body.count;
    if body.count > 0 && body[body.count - 1] != #char "\0" {
        body_size += 1;
    }
    body_allocated := alloc_string(body_size);
    memcpy(body_allocated.data, body.data, body.count);
    if body_allocated.count > 0 {
        body_allocated[body_allocated.count - 1] = #char "\0";
    }

    return .{
        status,
        header_string.data,
        body_allocated.data,
    };
}

mg_free :: (result: mg_result) {
    free(result.header);
    free(result.body);
}

mg_endpoint_server_poll :: (server: *mg_endpoint_server, interval: s32 = 50) {
    mg_mgr_poll(*server.mgr, interval);
}

mg_endpoint_server_poll_async :: (server: *mg_endpoint_server, $interval: s32 = 50) {
    server.poll_thread = New(Thread);
    server.poll_thread.data = server;
    server.poll_thread_alive = true;
    thread_init(server.poll_thread, (thread: *Thread) -> int {
        server := cast(*mg_endpoint_server) thread.data;
        server.ctx = *context;
        while server.alive {
            mg_endpoint_server_poll(server, interval);
        }
        server.poll_thread_alive = false;
        return 0;
    });
    thread_start(server.poll_thread);
}

mg_http_static_start :: (path: *u8, port: u16, interval: s32 = 50) {
    mg_static_callback :: (conn: *mg_connection, ev: s32, ev_data: *void, fn_data: *void) #c_call {
        push_context mg_context {
            if ev == .HTTP_MSG {
                opts: mg_http_serve_opts = .{root_dir = "."};
                hm := cast(*mg_http_message) ev_data;

                mg_http_serve_dir(conn, hm, *opts);
            }
        }
    }

    url := sprint("http://0.0.0.0:%", port);
    defer free(url);
    url_c := to_c_string(url);
    defer free(url_c);

    mgr: mg_mgr;
    mg_mgr_init(*mgr);
    mg_http_listen(*mgr, url_c, mg_static_callback, null);
    while true {
        mg_mgr_poll(*mgr, interval);
    }
    mg_mgr_free(*mgr);
}

mg_get_wildcard :: (request: *mg_request, index: int = 0) -> string, bool {
    match: mg_str;
    if index == 0 {
        match = mg_http_var(request.hm.uri, to_mg_str("*1"));
    } else {
        match = mg_http_var(request.hm.uri, to_mg_str(tprint("*%", index + 1)));
    }

    if match.buf {
        return to_string(match), true;
    }

    return "", false;
}

// doesn't require freeing the result
mg_form_value :: (request: *mg_request, key: *u8) -> string, bool {
    value, success := mg_form_value(request.hm, key);
    return value, success;
}

mg_form_value :: (hm: *mg_http_message, key: *u8) -> string, bool {
    mg_key_str := mg_str_s(key);

    part: mg_http_part;
    ofs: u64 = mg_http_next_multipart(hm.body, 0, *part);
    while ofs > 0 {
        if mg_strcasecmp(part.name, mg_key_str) == 0 {
            result: string;
            result.data = part.body.buf;
            result.count = xx part.body.len;
            return result, true;
        }

        ofs = mg_http_next_multipart(hm.body, ofs, *part);
    }

    return "", false;
}

mg_get_method :: (hm: *mg_http_message) -> mg_method {
    method_string: string;
    method_string.data = hm.method.buf;
    method_string.count = cast(s64) hm.method.len;

    if method_string == {
        case "CONNECT";
            return .Connect;
        case "DELETE";
            return .Delete;
        case "GET";
            return .Get;
        case "HEAD";
            return .Head;
        case "OPTIONS";
            return .Options;
        case "PATCH";
            return .Patch;
        case "POST";
            return .Post;
        case "PUT";
            return .Put;
        case "TRACE";
            return .Trace;
        case;
            return .Fallback;
    }
}

mg_get_connection_ip :: (response: *mg_response) -> string {
    return mg_get_connection_ip(response.rem);
}

mg_get_connection_ip :: (addr: mg_addr) -> string {
    builder: String_Builder;
    if addr.is_ip6 {
        hex_table :: u8.[#char "0", #char "1", #char "2", #char "3", #char "4", #char "5", #char "6", #char "7", #char "8", #char "9", #char "A", #char "B", #char "C", #char "D", #char "E", #char "F"];

        ip16: [] u16;
        ip16.data = cast(*u16) addr.ip.data;
        ip16.count = 8;

        for part: ip16 {
            part1 := (part >> 12) & 0xf;
            part2 := (part >> 8) & 0xf;
            part3 := (part >> 4) & 0xf;
            part4 := part & 0xf;
            append(*builder, hex_table[part1]);
            append(*builder, hex_table[part2]);
            append(*builder, hex_table[part3]);
            append(*builder, hex_table[part4]);

            if it_index < ip16.count - 1 {
                append(*builder, #char ":");
            }
        }
    } else {
        for i: 0..3 {
            append(*builder, tprint("%", addr.ip[i]));

            if i < 3 {
                append(*builder, ".");
            }
        }
    }

    return builder_to_string(*builder);
}

mg_ws_server :: struct {
    mgr: mg_mgr;
    // this is the actual bytes received from all clients
    data_buffer: [..] u8;
    data_buffer_back: [..] u8;
    connections: Table(u32, *mg_connection);
    // return data
    events: [..] mg_ws_server_event;
}

mg_ws_server_event :: struct {
    id: u32;
    type: enum u8 {
        Connected;
        Disconnected;
        Packet;
    };
    union {
        connected: struct {};
        disconnected: struct {};
        packet: struct {
            data: *u8;
            count: int;
        };
    }
}

mg_ws_server_create :: (port: u16) -> *mg_ws_server {
    mg_ws_callback :: (conn: *mg_connection, ev: s32, ev_data: *void) #c_call {
        push_context mg_context {
            // guessing at this since the update
            fn_data := conn.fn_data;
            server := cast(*mg_ws_server) fn_data;

            if cast(MG_EV) ev == {
                case .CLOSE;
                    table_remove(*server.connections, cast(u32) conn.id);

                    event: mg_ws_server_event;
                    event.id = cast(u32) conn.id;
                    event.type = .Disconnected;

                    array_add(*server.events, event);

                case .HTTP_MSG;
                    hm := cast(*mg_http_message) ev_data;
                    mg_ws_upgrade_CFormat(conn, hm, null);

                    table_set(*server.connections, cast(u32) conn.id, conn);

                    event: mg_ws_server_event;
                    event.id = cast(u32) conn.id;
                    event.type = .Connected;

                    array_add(*server.events, event);

                case .WS_MSG;
                    wm := cast(*mg_ws_message) ev_data;
                    wm_bytes: [] u8 = ---;
                    wm_bytes.data = xx wm.data.buf;
                    wm_bytes.count = xx wm.data.len;

                    event: mg_ws_server_event;
                    event.id = cast(u32) conn.id;
                    event.type = .Packet;
                    event.packet.data = xx server.data_buffer.count;
                    event.packet.count = xx wm_bytes.count;

                    array_add(*server.events, event);

                    array_add_bytes(*server.data_buffer, wm_bytes);
            }
        }
    }

    server := New(mg_ws_server);
    mg_mgr_init(*server.mgr);
    array_reserve(*server.data_buffer, 1024 * 1024);
    array_reserve(*server.data_buffer_back, 1024 * 1024);
    init(*server.connections);

    url := sprint("http://0.0.0.0:%", port);
    defer free(url);
    url_c := to_c_string(url);
    defer free(url_c);

    mg_http_listen(*server.mgr, url_c, mg_ws_callback, server);

    return server;
}

mg_ws_server_get_events :: (server: *mg_ws_server) -> [] mg_ws_server_event {
    server.data_buffer.data, server.data_buffer_back.data = server.data_buffer_back.data, server.data_buffer.data;
    server.data_buffer_back.count = server.data_buffer.count;
    server.data_buffer.count = 0;

    buffer_pointer := cast(u64) server.data_buffer_back.data;

    for *event: server.events {
        if event.type == .Packet {
            (cast(*u64) *event.packet.data).* += buffer_pointer;
        }
    }

    server_events: [] mg_ws_server_event;
    server_events = server.events;

    server.events.count = 0;

    return server_events;
}

mg_ws_server_get_connections :: (server: *mg_ws_server) -> [] u32 {
    ids := NewArray(server.connections.entries, u32,, temp);
    for entry: server.connections.entries {
        array_add(*ids, entry.key);
    }

    return server.ids;
}

mg_ws_server_send :: (server: *mg_ws_server, id: u32, bytes: [] u8) {
    conn, found := table_find(*server.connections, id);
    if !found {
        return;
    }

    mg_ws_send(conn, bytes.data, xx bytes.count, WEBSOCKET_OP_BINARY);
}

mg_ws_server_flush_and_poll :: (server: *mg_ws_server) {
    mg_mgr_poll(*server.mgr, 0);
}

mg_ws_server_free :: (server: *mg_ws_server) {
    array_free(server.events);
    deinit(*server.connections);
    array_free(server.data_buffer_back);
    array_free(server.data_buffer);
    mg_mgr_free(*server.mgr);
    free(server);
}

mg_ws_client :: struct {
    mgr: mg_mgr;
    connection: *mg_connection;
    connected: bool;
    data_buffer: [..] u8;
    packets_data_entry: [..] mg_ws_client_data_entry;
    data_buffer_back: [..] u8;
    packets_data_entry_back: [..] mg_ws_client_data_entry;
    // return data
    packets: [..] mg_ws_client_packet;
}

mg_ws_client_data_entry :: struct {
    id: u64;
    offset: s32;
    length: s32;
}

mg_ws_client_packet :: struct {
    id: u64;
    data: *u8;
    length: int;
}

mg_ws_client_create :: () -> *mg_ws_client {
    client := New(mg_ws_client);
    mg_mgr_init(*client.mgr);
    array_reserve(*client.data_buffer, 1024 * 1024);
    array_reserve(*client.packets_data_entry, 1024);
    array_reserve(*client.data_buffer_back, 1024 * 1024);
    array_reserve(*client.packets_data_entry_back, 1024);
    array_reserve(*client.packets, 1024);

    return client;
}

mg_ws_client_connect :: (client: *mg_ws_client, address: string) {
    mg_ws_callback :: (conn: *mg_connection, ev: s32, ev_data: *void, fn_data: *void) #c_call {
        push_context mg_context {
            client := cast(*mg_ws_client) fn_data;

            if cast(MG_EV) ev == {
                case MG_EV_ERROR; #through;
                case MG_EV_CLOSE;
                    mg_ws_client_close(client);

                case MG_EV_WS_OPEN;
                    client.connected = true;

                case MG_EV_WS_MSG;
                    wm := cast(*mg_ws_message) ev_data;
                    wm_bytes: [] u8 = ---;
                    wm_bytes.data = xx wm.data.buf;
                    wm_bytes.count = xx wm.data.len;

                    entry: mg_ws_client_data_entry = ---;
                    entry.id = cast(u32) conn.id;
                    entry.offset = xx client.data_buffer.count;
                    entry.length = xx wm_bytes.count;

                    array_add_bytes(*client.data_buffer, wm_bytes);
                    array_add(*client.packets_data_entry, entry);
            }
        }
    }

    url_c := to_c_string(address);
    defer free(url_c);

    mg_ws_client_close(client);
    client.connection = mg_ws_connect(*client.mgr, url_c, mg_ws_callback, client, null);
}

mg_ws_client_close :: (client: *mg_ws_client) {
    if !client.connection {
        return;
    }

    set_connection_listeninig(client.connection, .is_closing, true);

    client.connection = null;
    client.connected = false;
}

mg_ws_client_get_packets :: (client: *mg_ws_client) -> [] mg_ws_client_packet {
    client.data_buffer.data, client.data_buffer_back.data = swap(client.data_buffer.data, client.data_buffer_back.data);
    client.packets_data_entry.data, client.packets_data_entry_back.data = swap(client.packets_data_entry.data, client.packets_data_entry_back.data);

    client.data_buffer_back.count = client.data_buffer.count;
    client.packets_data_entry_back.count = client.packets_data_entry.count;

    client.data_buffer.count = 0;
    client.packets_data_entry.count = 0;

    pointer := client.data_buffer_back.data;
    client.packets.count = 0;

    for entry: client.packets_data_entry_back {
        packet: mg_ws_client_packet = ---;
        packet.id = entry.id;
        packet.data = pointer + entry.offset;
        packet.length = entry.length;

        array_add(*client.packets, packet);
    }

    return client.packets;
}

mg_ws_client_send :: (client: *mg_ws_client, bytes: [] u8) {
    if !client.connected {
        return;
    }

    mg_ws_send(client.connection, bytes.data, xx bytes.count, WEBSOCKET_OP_BINARY);
}

mg_ws_client_flush_and_poll :: (client: *mg_ws_client) {
    mg_mgr_poll(*client.mgr, 0);
}

mg_ws_client_free :: (client: *mg_ws_client) {
    mg_ws_client_close(client);
    array_free(client.ids);
    array_free(client.packets);
    array_free(client.packets_data_entry_back);
    array_free(client.data_buffer_back);
    array_free(client.packets_data_entry);
    array_free(client.data_buffer);
    mg_mgr_free(*client.mgr);
    free(client);
}

mg_client :: struct {
    mgr: mg_mgr;
}

mg_client_request :: struct {
    address: *u8;
    content_type: string;
    body: string;
    method: mg_method;
    response: *mg_client_response;
    timeout: int;
    client_context: #Context;
    thread_id: u64;
}

free_keep_response :: (request: *mg_client_request) {
    if request.address free(request.address);
    // content type is always constant
    if request.body free(request.body);
    request.response = null;

    free(cast(*void) request);
}

mg_client_response :: struct {
    loaded: bool;
    status: int;
    body: string;
}

free :: (response: *mg_client_response) {
    if response.body free(response.body);

    free(cast(*void) response);
}

mg_client_create :: () -> *mg_client {
    client := New(mg_client);
    mg_mgr_init(*client.mgr);

    return client;
}

mg_client_free :: (client: *mg_client) {
    mg_mgr_free(*client.mgr);
    free(client);
}

mg_client_send_request :: (client: *mg_client, address: string, method: mg_method) -> *mg_client_response {
    return inline mg_client_send_request(client, address, method, .[]);
}

// the mg_client_response must be freed when its marked as done
mg_client_send_request :: (client: *mg_client, address: string, method: mg_method, form_data: [] [2] string) -> *mg_client_response {
    REQUEST_TIMEOUT :: 2000;

    build_form_data :: (form_data: [] [2] string) -> string {
        builder: String_Builder;

        url_encode :: (value: string) -> string {
            buffer := NewArray(value.count + 4, u8);
            length := 0;
            while !length {
                length = cast(int) mg_url_encode(value.data, cast(u64) value.count, buffer.data, cast(u64) buffer.count);

                if !length {
                    next_count := buffer.count * 2;
                    array_free(buffer);
                    buffer = NewArray(next_count, u8);
                }
            }

            output: string;
            output.data = buffer.data;
            output.count = length;

            return output;
        }

        for form_entry, form_index: form_data {
            name := url_encode(form_entry[0]);
            defer free(name);
            value := url_encode(form_entry[1]);
            defer free(value);

            append(*builder, name);
            append(*builder, "=");
            append(*builder, value);

            if form_index < form_data.count - 1 {
                append(*builder, "&");
            }
        }

        return builder_to_string(*builder);
    }

    get_status_code :: (response: string) -> int {
        index := 0;

        while index < response.count && response[index] == #char " " {
            index += 1;
        }

        while index < response.count && response[index] != #char " " {
            index += 1;
        }

        while index < response.count && response[index] == #char " " {
            index += 1;
        }

        starting_index := index;

        while index < response.count && response[index] != #char " " {
            index += 1;
        }

        ending_index := index;

        status_code_string: string;
        status_code_string.data = response.data + starting_index;
        status_code_string.count = ending_index - starting_index;

        status_code, success := parse_int(*status_code_string);
        if !success {
            return 418;
        }

        return status_code;
    }

    get_response_body :: (response: string) -> string {
        index := 0;
        while index < response.count {
            rn_ending := index < response.count - 3;
            rn_ending = rn_ending && response[index] == #char "\r";
            rn_ending = rn_ending && response[index + 1] == #char "\n";
            rn_ending = rn_ending && response[index + 2] == #char "\r";
            rn_ending = rn_ending && response[index + 3] == #char "\n";
            if rn_ending {
                body: string;
                body.data = response.data + index + 4;
                body.count = response.count - index - 4;
                return copy_string(body);
            }

            n_ending := index < response.count - 1;
            n_ending = n_ending && response[index] == #char "\n";
            n_ending = n_ending && response[index + 1] == #char "\n";
            if n_ending {
                body: string;
                body.data = response.data + index + 2;
                body.count = response.count - index - 2;
                return copy_string(body);
            }

            index += 1;
        }

        return "";
    }

    mg_client_callback :: (conn: *mg_connection, ev: s32, ev_data: *void, fn_data: *void) #c_call {
        request := cast(*mg_client_request) fn_data;

        // if this is being used in multiple threads its really not safe
        // because each thread will be calling the callback independently and the same event can be fired multiple times

        push_context request.client_context {
            if cast(MG_EV) ev == {
                case .OPEN;
                    time := cast(int) (get_time() * 1000);
                    request.timeout = cast(int) (time + REQUEST_TIMEOUT);

                case .ERROR; #through;
                case .CLOSE;
                    request.response.loaded = true;
                    free_keep_response(request);

                case .CONNECT;
                    // Connected to server. Extract host name from URL
                    host := mg_url_host(request.address);

                    // If s_url is https://, tell client connection to use TLS
                    if mg_url_is_ssl(request.address) {
                        opts := mg_tls_opts.{ca = "ca.pem", srvname = host};
                        mg_tls_init(conn, *opts);
                    }

                    REQUEST_HTTP :: "% % HTTP/1.0\r\nHost: %\r\nContent-Length: %\r\nContent-Type: %\r\n\r\n";

                    method: string;
                    if request.method == {
                        case .Get;
                            method = "GET";
                        case .Post;
                            method = "POST";
                        case;
                            print("Unsupported request method. %\n", method);
                    }

                    host_string: string;
                    host_string.data = host.buf;
                    host_string.count = cast(int) host.len;

                    request_string := tprint(REQUEST_HTTP, method, to_string(mg_url_uri(request.address)), host_string, request.body.count, request.content_type);

                    mg_printf(conn, request_string);
                    mg_send(conn, request.body.data, cast(u64) request.body.count);

                case .HTTP_MSG;
                    hm := cast(*mg_http_message) ev_data;

                    response: string;
                    response.data = hm.message.buf;
                    response.count = cast(int) hm.message.len;

                    request.response.loaded = true;
                    request.response.status = get_status_code(response);
                    request.response.body = get_response_body(response);

                    mg_set_closing(conn, true);

                case .POLL;
                    time := cast(int) (get_time() * 1000);
                    if time >= request.timeout {
                        request.response.status = 408;

                        mg_set_closing(conn, true);
                    }
            }
        }
    }

    response := New(mg_client_response);
    request := New(mg_client_request);
    request.address = to_c_string(address);
    request.response = response;
    // content_type will never be freed
    if form_data {
        request.content_type = "application/x-www-form-urlencoded";
        request.body = build_form_data(form_data);
    } else {
        request.content_type = "text/html; charset=utf-8";
        request.body = "";
    }
    request.method = method;
    request.client_context = context;
    request.thread_id = get_thread_id();

    mg_http_connect(*client.mgr, request.address, mg_client_callback, request);

    return response;
}

has :: (pointer: *void) -> bool {
    for p: unique_pointers {
        if p == pointer {
            return true;
        }
    }

    return false;
}

add :: (pointer: *void) {
    array_add(*unique_pointers, pointer);
}

mg_client_poll :: (client: *mg_client, timeout: int = 50) {
    mg_mgr_poll(*client.mgr, cast(s32) timeout);
}

mg_set_closing :: (conn: *mg_connection, closing: bool) {
    if closing {
        // bitfields yay
        conn.is_listening |= cast(u32) 0b1000000000000;
    } else {
        conn.is_listening &= ~(cast(u32) 0b1000000000000);
    }
}

to_string :: (str: mg_str) -> string {
    r: string;
    r.data = str.buf;
    r.count = cast(s64) str.len;
    return r;
}

to_mg_str :: (str: string) -> mg_str {
    r: mg_str;
    r.buf = str.data;
    r.len = cast(u64) str.count;
    return r;
}

#scope_file

random_mutex: Mutex;

core_count :: #run get_number_of_processors();
core_thread_count :: core_count * 2;
mg_method_count :: cast(u16) mg_method.Fallback + 1;

mg_context: #Context;

array_add_bytes :: (array: *[..] u8, to_append: [] u8) {
    // returns the next power of 2 >= the current number
    next_power_of_two :: inline (v: int) -> int {
        // count leading zeros
        #if WASM {
            p := 1;
            while p < v {
                p += p;
            }
            return p;
        } else {
            result: s64 = ---;
            v -= 1;
            #asm {
                lzcnt result, v;
            }
            return 1 << (64 - result);
        }
    }

    old_count := array.count;
    append_count := to_append.count;

    array_reserve(array, next_power_of_two(old_count + append_count));
    memcpy(array.data + old_count, to_append.data, append_count);
    array.count += append_count;
}

get_thread_id :: () -> u64 {
    #if OS == .WINDOWS {
        #import "Windows";

        return cast(u64) GetCurrentThreadId();
    } else #if OS == .LINUX {
        #import "POSIX";

        return pthread_self();
    } else #if OS == .MACOS {
        #import "POSIX";

        thread_id: u64;
        pthread_threadid_np(null, *thread_id);
        return thread_id;
    } else {
        assert(false, "Unknown OS.");
        return 0;
    }
}

#import "mongooseJai";

#import "Math";
#import "Basic";
#import "Thread";
#import "Hash_Table";
#import "System";
#import "String";
#import "rpmalloc";